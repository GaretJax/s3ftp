import tempfile
import os

from zope.interface import implements

from twisted.cred import portal, error as cred_error
from twisted.internet import defer
from twisted.protocols import ftp
from twisted.internet.protocol import Protocol
from twisted.web.client import FileBodyProducer

from txs3.client import ns
from txs3 import utils


class S3Reader(Protocol):
    implements(ftp.IReadFile)

    def __init__(self, response):
        self._response = response
        self._send = False

    def dataReceived(self, bytes):
        self._consumer.write(bytes)

    def resumeProducing(self):
        self.transport.resumeProducing()

    def pauseProducing(self):
        self.transport.pauseProducing()

    def stopProducing(self):
        self.transport.stopProducing()

    def connectionLost(self, reason):
        self._consumer.unregisterProducer()
        self._done.callback(None)

    def send(self, consumer):
        assert not self._send, 'Can only call S3Reader.send once per instance'
        self._send = True
        self._consumer = consumer
        self._done = defer.Deferred()
        self._consumer.registerProducer(self, True)
        self._response.deliverBody(self)
        return self._done


class S3FTPShell(object):
    delimiter = '/'

    def __init__(self, bucket, path=''):
        self._bucket = bucket
        self._pathPrefix = path

    def _publicPath(self, chunks):
        return self.delimiter.join(chunks)

    def _path(self, chunks, isDir=False):
        chunks = list(chunks)
        if self._pathPrefix:
            chunks = [self._pathPrefix] + chunks
        if isDir and chunks:
            chunks = chunks + ['']
        path = self.delimiter.join(chunks)
        return path

    def _mkdir(self, path):
        body = utils.StringProducer('')
        return self._bucket.object(path.rstrip('/') + '/').upload(body)

    def makeDirectory(self, path):
        key = self._path(path)
        return self._mkdir(key)

    @defer.inlineCallbacks
    def removeDirectory(self, path):
        prefix = self._path(path, True)
        results = self._bucket.objects(prefix=prefix, delimiter=self.delimiter)
        results = iter(results)

        try:
            obj = yield next(results)
        except StopIteration:
            return

        if obj.tag == ns('CommonPrefixes'):
            raise Exception('Directory is not empty.')

        try:
            yield next(results)
        except StopIteration:
            yield self._bucket.object(obj.find(ns('Key')).text).delete()
        else:
            raise Exception('Directory is not empty.')

    def removeFile(self, path):
        key = self._path(path)
        return self._bucket.object(key).delete()

    @defer.inlineCallbacks
    def rename(self, fromPath, toPath):
        prefix = self._path(fromPath, True)
        results = self._bucket.objects(prefix=prefix, delimiter=self.delimiter)
        results = iter(results)

        try:
            yield next(results)
        except StopIteration:
            yield self.renameFile(fromPath, toPath)
        else:
            try:
                yield next(results)
            except StopIteration:
                yield self.renameEmptyDirectory(fromPath, toPath)
            else:
                raise ftp.CmdNotImplementedError(
                    'Cannot rename non-empty directories')

    @defer.inlineCallbacks
    def renameFile(self, fromPath, toPath):
        fromObj = self._bucket.object(self._path(fromPath))
        toObj = self._bucket.object(self._path(toPath))
        yield fromObj.copyTo(toObj)
        yield fromObj.delete()


    def renameEmptyDirectory(self, fromPath, toPath):
        d1 = self._bucket.object(self._path(fromPath, True)).delete()
        d2 = self._mkdir(self._path(toPath))
        return defer.DeferredList([d1, d2])

    @defer.inlineCallbacks
    def access(self, path):
        prefix = self._path(path, True)
        results = self._bucket.objects(prefix=prefix, delimiter=self.delimiter)
        results = yield results.asList()

        if not results:
            raise ftp.FileNotFoundError(self.delimiter.join(path))

    @defer.inlineCallbacks
    def stat(self, path, keys=()):
        prefix = self._path(path)
        results = yield self._bucket.objects(prefix=prefix).asList()

        if not results:
            raise ftp.FileNotFoundError(self.delimiter.join(path))

        fileName, ent = yield self._stat(keys, results[0])
        defer.returnValue(ent)

    def _stat(self, keys, xml, path=None):
        if path:
            prefix = self._path(path, True)
        else:
            prefix = None

        isDir = xml.tag == ns('CommonPrefixes')

        stat = {
            'size': 0,
            'directory': isDir,
            'hardlinks': 0,
            'modified': 0,
            'owner': 'nobody',
            'group': 'nobody'
        }

        if isDir:
            key = xml.find(ns('Prefix')).text
            filename = key[:-1].rsplit(self.delimiter, 1)[-1]
            stat['permissions'] = 0755
        else:
            key = xml.find(ns('Key')).text
            if not isDir and key == prefix:
                return False, []
            filename = key.rsplit(self.delimiter, 1)[-1]

            stat['size'] = int(xml.find(ns('Size')).text)
            stat['permissions'] = 0644
            stat['owner'] = xml.find(ns('Owner', 'DisplayName')).text

        return filename, [stat[k] for k in keys]

    @defer.inlineCallbacks
    def list(self, path, keys=()):
        prefix = self._path(path, True)
        objects = yield self._bucket.objects(prefix=prefix,
                                             delimiter=self.delimiter)
        results = []
        for d in objects:
            obj = yield d
            fileName, ent = yield self._stat(keys, obj, path)
            if not fileName:
                continue
            results.append((fileName, ent))
        defer.returnValue(results)

    @defer.inlineCallbacks
    def openForReading(self, path):
        key = self._path(path)
        obj = self._bucket.object(key)
        response = yield obj.get()
        defer.returnValue(S3Reader(response))

    def openForWriting(self, path):
        key = self._path(path)
        object = self._bucket.object(key)

        try:
            fd, path = tempfile.mkstemp()
            os.close(fd)
            fh = open(path, 'w')
        except (IOError, OSError), e:
            return ftp.errnoToFailure(e.errno, path)
        except:
            return defer.fail()
        return defer.succeed(_UploadingFileWriter(fh, object))


class _UploadingFileWriter(ftp._FileWriter, object):

    def __init__(self, fObj, object):
        super(_UploadingFileWriter, self).__init__(fObj)
        self._object = object

    def close(self):

        class T:
            def stop(self):
                pass

        def cbClose(ignored):
            fh.close()
            return ignored

        def ebBuckets(p):
            p.printTraceback()
            for r in getattr(p.value, 'reasons', []):
                r.printTraceback()

        fh = open(self.fObj.name)

        fileReader = FileBodyProducer(fh, readSize=64 * 1024)
        fileReader._task = T()

        d = self._object.upload(fileReader)
        d.addBoth(cbClose)
        d.addErrback(ebBuckets)

        return d


class S3Realm(object):
    implements(portal.IRealm)

    def __init__(self, shells):
        self._shells = shells

    def getShell(self, avatarId):
        try:
            return self._shells[avatarId]
        except KeyError:
            raise cred_error.UnauthorizedLogin()

    def requestAvatar(self, avatarId, mind, *interfaces):
        for iface in interfaces:
            if iface is ftp.IFTPShell:
                avatar = self.getShell(avatarId)
                return (ftp.IFTPShell, avatar,
                        getattr(avatar, 'logout', lambda: None))
        raise NotImplementedError(
            'Only IFTPShell interface is supported by this realm')
