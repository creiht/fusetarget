"""
I was using this to test the performance of NBD virtual block devices by
creating a loopback to a physical disk.

It's difficult to impossible to use direct i/o from Python without bindings.
"""

import os
import errno

cdef extern from 'stdlib.h':
    int posix_memalign(void **memptr, size_t alignment, size_t size) nogil
    void free(char *) nogil
    int ERRNO "errno"

cdef extern from 'unistd.h':
    unsigned long c_pread "pread" (int, char *, unsigned long,
                                   unsigned long) nogil
    unsigned long c_pwrite "pwrite" (int, char *, unsigned long,
                                     unsigned long) nogil

cdef extern from 'string.h':
    void *memcpy(char *, char *, int) nogil


PAGE_SIZE = 4096


def pread(fd, offset, length, align=PAGE_SIZE):
    """
    Read length bytes from fd at offset.

    :param fd: read opened fd
    :param offset: byte to start read from, must be memaligned
    :param length: number of bytes to read, must be multiple of BLOCK_SIZE
    :param align: page align byte size
    """
    # validate input to protect against segmentation fault
    fd = <int>fd
    offset = <unsigned long>offset
    length = <unsigned long>length
    align = <size_t>align
    # local c vars
    cdef char *buf
    cdef int err
    cdef int read_amt
    with nogil:
        err = posix_memalign(<void **>&buf, align, length)
    if err:
        raise OSError(err, os.strerror(err))
    with nogil:
        read_amt = c_pread(fd, buf, length, offset)
    if read_amt < 0:
        raise OSError(ERRNO, os.strerror(ERRNO))
    resp = buf[0:read_amt]
    with nogil:
        free(buf)
    return resp


def pwrite(fd, offset, buf, align=PAGE_SIZE):
    """
    Write buf to fd at offset.
    
    :param fd: write opened fd
    :param offset: byte to start write at, must be memaligned
    :param buf: bytes to be written, len must be multiple of BLOCK_SIZE
    :param align: page align byte size
    """
    # validate input to protect against segfault
    fd = <int>fd
    buf = str(buf)
    offset = <unsigned long>offset
    align = <size_t>align
    # local c vars
    cdef char *buf2
    cdef int err
    cdef int write_amt
    cdef unsigned long length = len(buf)
    with nogil:
        err = posix_memalign(<void **>&buf2, align, length)
    if err:
        raise OSError(err, os.strerror(err))
    with nogil:
        memcpy(buf2, buf, length)
        write_amt = c_pwrite(fd, buf2, length, offset)
    if write_amt < 0:
        raise OSError(ERRNO, os.strerror(ERRNO))
    free(buf2)
    return write_amt


cdef class DirectFile:
    """
    Manipulate file with directio using pread and pwrite.

    :param path: path to an existing file on disk
    :param align: page align byte size
    """
    cdef int fd
    cdef int align

    def __init__(self, path, align=PAGE_SIZE):
        try:
            self.fd = os.open(path, os.O_DIRECT | os.O_RDWR)
        except OSError:
            self.fd = 0
            raise
        self.align = align

    def __dealloc__(self):
        if self.fd:
            os.close(self.fd)

    def fileno(self):
        """
        :returns : integer, file descriptor
        """
        return self.fd

    def pread(self, offset, length, auto_align=True):
        """
        Read length bytes from file at offset.
        
        :param offset: byte to start read from
        :param length: number of bytes to read
        :param auto_align: enabled page reads to auto align arguments

        Unless auto_align is disabled, if offset and length are not aligned
        with block size, the operation will be automatically retried and the
        result truncated to match your request.

        :returns : a string of bytes, read from disk at offset
        
        The length of the returned string will not be larger than specified by
        length, but may be smaller than the number of bytes read from disk if
        auto_align is True.
        """
        try:
            return pread(self.fd, offset, length, align=self.align)
        except OSError, e:
            if auto_align and e.errno == errno.EINVAL:
                # possibly misalgned offset or length
                off_cnt, offset = divmod(offset, self.align)
                len_cnt, length = divmod(offset + length, self.align)
                if offset or length:
                    cache = pread(self.fd, off_cnt * self.align, (len_cnt + 1) *
                                 self.align, align=self.align)
                    return cache[offset:(len_cnt * self.align) + length]
            raise

    def pwrite(self, offset, buf, auto_align=False):
        """
        Write buf to file at offset.

        :param offset: byte to start write at
        :param buf: bytes to be written
        :param auto_align: enabled page reads to auto align arguments

        When auto_align is enabled, if offset and length of buffer are not
        aligned with block size, some number of pages greater than the size of
        buf will be read into memory, updated with the new data, and the
        operation will be automatically retried.
        
        :returns : the amount of data written to disk
        
        The value may be larger than the length of buf if auto_align is True.
        """
        try:
            return pwrite(self.fd, offset, buf, align=self.align)
        except OSError, e:
            if auto_align and e.errno == errno.EINVAL:
                # possibly misalgned offset or length
                off_cnt, offset = divmod(offset, self.align)
                len_cnt, length = divmod(offset + len(buf), self.align)
                if offset or length:
                    cache = pread(self.fd, off_cnt * self.align,
                                   (len_cnt + 1) * self.align,
                                   align=self.align)
                    cache = bytearray(cache)
                    # update cache with new data from buf
                    cache[offset:(len_cnt * self.align) + length] = buf
                    return pwrite(self.fd, off_cnt * self.align, str(cache),
                                  align=self.align)
            raise

    def __len__(self):
        return os.lseek(self.fd, 0, os.SEEK_END)
