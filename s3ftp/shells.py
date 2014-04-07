from twisted.protocols import ftp

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

    def assertWritable(self, path):
        if not self.isWritable(path):
            raise ftp.PermissionDeniedError(self._publicPath(path))

    def getPermissions(self, path):
        perm = self.rmask
        perm |= self.wmask if self.isWritable(path) else 0
        perm |= self.dmask if self.isDir(path) else 0
        return perm

    def makeDirectory(self, path):
        raise ftp.PermissionDeniedError(self._publicPath(path))

    def removeDirectory(self, path):
        raise ftp.PermissionDeniedError(self._publicPath(path))

    def removeFile(self, path):
        self.assertWritable(path)
        return super(Dropbox, self).removeFile(path)

    def rename(self, fromPath, toPath):
        raise ftp.CmdNotImplementedError('RNFR/RNTO')

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
        self.assertWritable(path)
        return super(Dropbox, self).openForWriting(path)
