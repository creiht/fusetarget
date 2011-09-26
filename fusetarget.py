#!/usr/bin/env python

"""uses fuse python bindings"""
import os
import math
import sys
import time
import errno

import fuse

from directio import DirectFile


INODE_DIR = 1
INODE_VOLUME = 2
BLOCK_SIZE = 1024*4

class TargetFuse(fuse.Fuse):
    def __init__(self, path, *args, **kw):
        fuse.Fuse.__init__(self, *args, **kw)
        self.path = path
        self.vol = DirectFile(path)

    def _get_vol_attrs(self):
        stat = os.lstat(self.path)
        attrs = fuse.Stat()
        attrs.st_ino = INODE_VOLUME
        attrs.generation = 0
        attrs.entry_timeout = 300
        attrs.attr_timeout = 300
        attrs.st_mode = stat.st_mode
        attrs.st_nlink = stat.st_nlink
        attrs.st_uid = stat.st_uid
        attrs.st_gid = stat.st_gid
        attrs.st_rdev = stat.st_dev
        attrs.st_size = stat.st_size
        attrs.st_blksize = BLOCK_SIZE
        attrs.st_blocks = math.ceil(stat.st_size/float(BLOCK_SIZE))
        attrs.st_atime = stat.st_atime
        attrs.st_mtime = stat.st_mtime
        attrs.st_ctime = stat.st_ctime
        return attrs

    def _get_dir_attrs(self):
        stat = os.lstat('/'.join(self.path.split('/')[:-1]))
        attrs = fuse.Stat()
        attrs.st_ino = INODE_DIR
        attrs.generation = 0
        attrs.entry_timeout = 300
        attrs.attr_timeout = 300
        attrs.st_mode = stat.st_mode
        attrs.st_nlink = stat.st_nlink
        attrs.st_uid = stat.st_uid
        attrs.st_gid = stat.st_gid
        attrs.st_rdev = stat.st_dev
        attrs.st_size = stat.st_size
        attrs.st_blksize = BLOCK_SIZE
        attrs.st_blocks = math.ceil(stat.st_size/float(BLOCK_SIZE))
        attrs.st_atime = stat.st_atime
        attrs.st_mtime = stat.st_mtime
        attrs.st_ctime = stat.st_ctime
        return attrs

    def getattr(self, path):
        print "GETATTR path=%s" % path
        if path == '/':
            return self._get_dir_attrs()
        elif path == '/volume':
            return self._get_vol_attrs()
        else:
            return errno.ENOENT

    def access(self, path, mode):
        print '*** access', path, mode

    def statfs(self):
        print '*** statfs'
        fstat = os.lstat(self.path)
        stat = fuse.StatVfs()
        stat.f_bsize = BLOCK_SIZE
        stat.f_frsize = BLOCK_SIZE
        stat.f_blocks = math.ceil(fstat.st_size/float(BLOCK_SIZE))
        stat.f_bfree = 0
        stat.f_files = 1
        stat.f_ffree = 0
        return stat

    def readdir(self, path, offset):
        print '*** readdir', path, offset
        dirents = ['.', '..', 'volume']
        for r in dirents:
            yield fuse.Direntry(r)

    def chmod(self, path, mode):
        print '*** chmod', path, oct(mode)
        return -errno.ENOSYS

    def chown(self, path, uid, gid):
        print '*** chown', path, uid, gid
        return -errno.ENOSYS

    def fsync(self, path, isFsyncFile):
        print '*** fsync', path, isFsyncFile
        return 0

    def link(self, targetPath, linkPath):
        print '*** link', targetPath, linkPath
        return -errno.ENOSYS

    def mkdir(self, path, mode):
        print '*** mkdir', path, oct(mode)
        return -errno.ENOSYS

    def mknod(self, path, mode, dev):
        print '*** mknod', path, oct(mode), dev
        return -errno.ENOSYS

    def open( self, path, flags):
        print '*** open', path, flags
        return 0

    def read(self, path, length, offset):
        print '*** read', path, length, offset
        if path == '/volume':
            return self.vol.pread(offset, length)
        return errno.ENOENT

    def lock(self, path, cmd, owner, **kw):
        print '*** lock', path, cmd, owner, kw

    def readlink(self, path):
        print '*** readlink', path
        return -errno.ENOSYS

    def release(self, path, flags):
        print '*** release', path, flags
        return 0

    def rename(self, oldPath, newPath):
        print '*** rename', oldPath, newPath
        return -errno.ENOSYS

    def rmdir(self, path):
        print '*** rmdir', path
        return -errno.ENOSYS

    def symlink(self, targetPath, linkPath):
        print '*** symlink', targetPath, linkPath
        return -errno.ENOSYS

    def truncate(self, path, size):
        print '*** truncate', path, size
        return 0

    def unlink(self, path):
        print '*** unlink', path
        return -errno.ENOSYS

    def utime(self, path, times):
        print '*** utime', path, times
        return -errno.ENOSYS

    def write(self, path, buf, offset):
        #print '*** write', path, offset, len(buf)/1024
        if path == '/volume':
            return self.vol.pwrite(offset, buf)
        return errno.ENOENT

if __name__ == '__main__':
    fuse.fuse_python_api = (0, 2)
    path = '/srv/vol1'
    fs = TargetFuse(path, version="% prog ")
    fs.multithread = 0
    fs.parse(errex=1)
    fs.main()
