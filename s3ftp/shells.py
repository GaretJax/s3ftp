from twisted.protocols import ftp
from twisted.python import log

from .protocol import S3FTPShell


class Dropbox(S3FTPShell):
    uploadFolders = set()

    rmask = 0400
    wmask = 0200
    dmask = 0100

    def isDir(self, path):
        return len(path) == 1

    def isWritable(self, path):
        return path and path[0] in self.uploadFolders

    def assertWritable(self, path, reason):
        if not self.isWritable(path):
            log.msg('Is not writable {}: {}'.format(path, reason))
            raise ftp.PermissionDeniedError(self._publicPath(path))

    def getPermissions(self, path):
        perm = self.rmask
        perm |= self.wmask if self.isWritable(path) else 0
        perm |= self.dmask if self.isDir(path) else 0
        return perm

    def makeDirectory(self, path):
        log.msg('Directory creation disabled {}'.format(self._path(path)))
        raise ftp.PermissionDeniedError(self._publicPath(path))

    def removeDirectory(self, path):
        log.msg('Directory removal disabled {}'.format(self._path(path)))
        raise ftp.PermissionDeniedError(self._publicPath(path))

    def removeFile(self, path):
        self.assertWritable(path, 'cannot remove')
        return super(Dropbox, self).removeFile(path)

    def rename(self, fromPath, toPath):
        if self.isDir(fromPath):
            log.msg('Cannot rename directory {}'.format(self._path(fromPath)))
            raise ftp.PermissionDeniedError(self._publicPath(fromPath))
        if self.isDir(fromPath):
            log.msg('Cannot rename directory {}'.format(self._path(fromPath)))
            raise ftp.PermissionDeniedError(self._publicPath(toPath))
        self.assertWritable(fromPath, 'cannot remove')
        self.assertWritable(toPath, 'cannot remove')
        return super(Dropbox, self).rename(fromPath, toPath)

    def _stat(self, keys, xml, path=None):
        filename, ent = super(Dropbox, self)._stat(keys, xml, path)

        if filename:
            try:
                i = keys.index('permissions')
            except ValueError:
                pass
            else:
                path = list(path or []) + [filename]
                ent[i] = self.getPermissions(path)

        return filename, ent

    def openForWriting(self, path):
        self.assertWritable(path, 'cannot write')
        return super(Dropbox, self).openForWriting(path)
