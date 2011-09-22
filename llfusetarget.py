import errno
import os
import math
import sys
import time

import llfuse
import xattr

from directio import DirectFile


INODE_DIR = 1
INODE_VOLUME = 2
BLOCK_SIZE = 1024*4

class Operations(llfuse.Operations):
    def __init__(self, path):
        super(Operations, self).__init__()
        self.path = path
        self.vol = DirectFile(path)

    def _get_vol_attrs(self):
        stat = os.lstat(self.path)
        attrs = llfuse.EntryAttributes()
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
        attrs = llfuse.EntryAttributes()
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

    def access(self, inode, mode, ctx):
        """Check access"""
        print "ACCESS inode=%s mode=%s" % (inode, mode)
        return True

    def create(self, inode_p, name, mode, ctx):
        print "!CREATE! inode_p=%s name=%s mode=%s" % (inode_p, name, mode)
        return super(Operations, self).create(inode_p, name, mode, ctx)

    def destroy(self):
        """Clean up"""
        print "DESTROY"
        self.vol = None

    def flush(self, fh):
        print "!FLUSH! fh=%s" % fh
        return super(Operations, self).flush(fh)

    def forget(self, inode, nlookup):
        print "!FORGET! inode=%s nlookup=%s" % (inode, nlookup)
        return super(Operations, self).forget(inode, nlookup)

    def fsync(self, fh, datasync):
        print "!FSYNC! fh=%s datasync=%s" % (fs, datasync)
        return super(Operations, self).fsync(inode, nlookup)

    def fsyncdir(self, fh, datasync):
        print "!FSYNC! fh=%s datasync=%s" % (fs, datasync)
        return super(Operations, self).fsyncdir(inode, nlookup)

    def getattr(self, inode):
        print "GETATTR inode=%s" % inode
        if inode == INODE_VOLUME:
            return self._get_vol_attrs()
        if inode == INODE_DIR:
            return self._get_dir_attrs()
        raise llfuse.FUSEError(errno.ENOENT)

    def getxattr(self, inode, name):
        print "GETXATTR inode=%s name=%s" % (inode, name)
        if inode != INODE_VOLUME:
            raise llfuse.FUSEError(errno.ENOENT)
        raise llfuse.FUSEError(llfuse.ENOATTR)

    def listxattr(self, inode):
        print "!LISTXATTR! inode=%s" % inode
        return super(Operations, self).fsyncdir(inode, nlookup)

    def lookup(self, inode_p, name):
        print "LOOKUP inode_p=%s name=%s" % (inode_p, name)
        if name == '.' or name == '..':
            return inode_p
        if name != 'volume':
            raise llfuse.FUSEError(errno.ENOENT)
        return self._get_vol_attrs()

    def open(self, inode, flags):
        print "OPEN inode=%s flags=%s" % (inode, flags)
        if inode != INODE_VOLUME:
            raise llfuse.FUSEError(errno.ENOENT)
        return INODE_VOLUME

    def opendir(self, inode):
        print "OPENDIR inode=%s" % inode
        return INODE_DIR

    def read(self, fh, off, size):
        print "READ fh=%s, off=%s, size=%sK" % (fh, off, size/1024)
        if fh != INODE_VOLUME:
            raise llfuse.FUSEError(errno.ENOENT)
        return self.vol.pread(off, size)

    def readdir(self, fh, off):
        print "READDIR: fh=%s off=%s" % (fh, off)
        if fh != INODE_DIR:
            raise llfuse.FUSEError(errno.ENOENT)
        if off == 0:
            yield ('volume', self._get_vol_attrs(), 1)

    def release(self, fh):
        print "RELEASE: fh=%s" % fh
        pass

    def releasedir(self, fh):
        print "RELEASE DIR: fh=%s" % fh
        pass

    def removexattr(self, inode, name):
        print "!REMOVEXATTR! inode=%s attr=%s" % (inode, name)
        return super(Operations, self).removexattr(inode, name)

    def setattr(self, inode, attr):
        print "!SETATTR! inode=%s name=%s" % (inode, attr)
        return super(Operations, self).setattr(indode, attr)

    def setxattr(self, inode, name, value):
        print "!SETXATTR! inode=%s name=%s value=%s" % (inode, name, value)
        return super(Operations, self).setxattr(inode, name, value)

    def statfs(self):
        print "STATFS"
        stat = os.lstat(self.path)
        attrs = llfuse.StatvfsData()
        attrs.f_bsize = BLOCK_SIZE
        attrs.f_frsize = stat.st_size
        attrs.f_blocks = math.ceil(stat.st_size/float(BLOCK_SIZE))
        attrs.f_bfree = 0
        attrs.f_bavail = 0
        attrs.f_files = 1
        attrs.f_ffree = 0
        attrs.f_favail = 0
        return attrs

    def write(self, fh, off, buf):
        print "WRITE fh=%s off=%s len=%sK" % (fh, off, len(buf)/1024)
        if fh != INODE_VOLUME:
            raise llfuse.FUSEError(errno.ENOENT)
        amt = self.vol.pwrite(off, buf)
        return amt

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print ('Usage: %s PATH MOUNTPOINT' % sys.argv[0])
        sys.exit(1)
    path = sys.argv[1]
    mountpoint = sys.argv[2]
    operations = Operations(path)
    llfuse.init(operations, mountpoint,
            [ b'fsname=lunr'])

    try:
        llfuse.main(single=True)
    except Exception as e:
        print e
    finally:
        llfuse.close(unmount=False)
        time.sleep(10)
    llfuse.close()
