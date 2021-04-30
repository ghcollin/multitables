# Copyright (C) 2019 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

import os
import threading
from contextlib import contextmanager
import numpy as np

from . import numpy_utils

if os.name == "nt":
    _USE_POSIX = False
else:
    _USE_POSIX = True

# Python 3.8 will eventually have an inbuilt library to handle all of this, but it is currently 
# not usable due to the resource tracker interfering with memory management.
# try:
#     # Use the provided shared memory facilities if available.
#     import multiprocessing.shared_memory
#     _USE_INTERNAL = True
# except ImportError:

_USE_INTERNAL = False
# Otherwise, revert to basic shared memory implementation.
# The older shared memory API provided by multiprocessing is not appropriate as it can only use anonymous memory.
import mmap

def _make_map_id():
    """ Create a random string identifier for a shared memory block. """
    import base64, random, struct
    return b'/' + base64.urlsafe_b64encode(struct.pack('<Q', random.getrandbits(64)))

if _USE_POSIX:
    import ctypes
    import errno

    def _handle_errno(result, func, args):
        """ Handle basic return signals created by the POSIX API. """
        if result == -1:
            for erno in errno.errorcode.keys():
                if result == erno:
                    raise OSError(result, os.strerror(result))
            raise RuntimeError("Unknown error type: {} when handling execution of {} with args {}".format(erno, func, args))
        else:
            return result

    # The POSIX shared memory functions are located in different libraries in Linux and macOS.
    _posixshmlib = ctypes.cdll.LoadLibrary(None)
    try:
        _posixshmlib.shm_open.argtypes = []
    except AttributeError:
        import ctypes.util
        _posixshmlib = ctypes.cdll.LoadLibrary(ctypes.util.find_library("rt"))

    # Setup the types for the POSIX API.
    _posixshmlib.shm_open.argtypes = [ ctypes.c_char_p, ctypes.c_int, ctypes.c_ushort ]
    _posixshmlib.shm_open.errcheck = _handle_errno
    _posixshmlib.shm_unlink.argtypes = [ ctypes.c_char_p ]
    _posixshmlib.shm_unlink.errcheck = _handle_errno
    def _shm_open(name, access, mode):
        return _posixshmlib.shm_open(name, access, mode)
    def _shm_unlink(name):
        return _posixshmlib.shm_unlink(name)
else:
    # Shared memory in Windows can be handled entirely by mmap.
    # _winapi is just required for handling errors.
    import _winapi

import sys
_PYTHON3 = sys.version_info > (3, 0)

class SharedMemoryError(Exception):
    pass


class SharedBuffer:
    """ A class for creating a segment of named shared memory. """

    def __init__(self, map_id, size_nbytes):
        """ 
        Create a segment of shared memory with an automatically generated name.
        Or open an already created segment with the provided name.

        :param map_id: The name of the shared memory. Should be None if a new segment is to be created.
        :param size_nbytes: The size, in bytes, of the segment.
        """
        # This flag is used to signal if the shared memory has been unlinked by the owning process.
        self._flag = None
        self._closed = False
        try:
            # We actually need one extra byte for the signal flag.
            alloc_nbytes = size_nbytes + 1
            # The master process is the process that created the memory.
            # The master handles the lifetime of the memory, and unlinks it when it is no longer needed.
            master = (map_id is None)
            if _USE_INTERNAL:
                if master:
                    self._raw_mem = multiprocessing.shared_memory.SharedMemory(create=True, size=alloc_nbytes)
                    map_id = self._raw_mem.name
                else:
                    self._raw_mem = multiprocessing.shared_memory.SharedMemory(name=map_id, size=alloc_nbytes)
                
                def unlink():
                    self._raw_mem.unlink()
                
                def close():
                    self._raw_mem.close()
                
                self._buf = self._raw_mem.buf
                def release_buf():
                    pass # Releasing the buffer is handled by multiprocessing.
            else:
                if _USE_POSIX:
                    if master:
                        #master = True
                        while True:
                            # Generate a new ID.
                            map_id = _make_map_id()
                            # Attempt to create the buffer with this ID, this command will fail if a buffer already exists with that name.
                            try:
                                self._fd = _shm_open(map_id, os.O_CREAT | os.O_EXCL | os.O_RDWR, mode=0o600)
                            except OSError as e:
                                # If a buffer with that name already exists, try again.
                                if e.errno == errno.EEXIST:
                                    continue
                                else:
                                    raise
                            break
                    else:
                        self._fd = _shm_open(map_id, os.O_RDWR, mode=0o600)

                    def unlink():
                        if self._fd is not None:
                            _shm_unlink(map_id)
                    
                    try:
                        if master:
                            # If the segment has just been created, resize it to the appropriate size.
                            os.ftruncate(self._fd, alloc_nbytes)
                        # Now map it into memory.
                        self._mmap = mmap.mmap(self._fd, alloc_nbytes)
                    except OSError:
                        unlink()
                        raise

                    def close():
                        if self._mmap is not None:
                            self._mmap.close()
                            self._mmap = None
                        if self._fd is not None:
                            os.close(self._fd)
                            self._fd = None
                
                else: ## WINDOWS
                    if master:
                        while True:
                            map_id = _make_map_id()
                            # It might be better here to open a mmap with length 0, catch the exception, then reopen it with the correct size
                            self._mmap = mmap.mmap(-1, alloc_nbytes, tagname=map_id.decode("utf-8"))
                            if _winapi.GetLastError() == 0:
                                # If mapping the segment suceeded, then stop now, otherwise try again.
                                break
                            self._mmap.close()
                    else:
                        self._mmap = mmap.mmap(-1, alloc_nbytes, tagname=map_id.decode("utf-8"))

                    def unlink():
                        pass

                    def close():
                        self._mmap.close()
                
                if _PYTHON3:
                    self._buf = memoryview(self._mmap)
                else:
                    # Python 2.7 requires passing the mapped memory through ctypes first.
                    self._buf = memoryview( (ctypes.c_byte * alloc_nbytes).from_buffer(self._mmap) )
                
                def release_buf():
                    if self._buf is not None:
                        if _PYTHON3:
                            self._buf.release()
                        self._buf = None

            # The flag is the first byte of the memory
            self._flag = self._buf[:1]
            # The actual exposed buffer is the rest of the memory.
            self._ary = self._buf[1:]

            def release():
                # Release these pointers when the buffer is closed.
                if self._flag is not None:
                    if _PYTHON3:
                        self._flag.release()
                    self._flag = None
                if self._ary is not None:
                    if _PYTHON3:
                        self._ary.release()
                    self._ary = None
                release_buf()
            
            self._flag[:] = b'\x00' # Flag set to initially available.
            self.name = map_id
            self.size_nbytes = size_nbytes
            self._lock = threading.RLock()
        except:
            # If something went wrong, make sure these functions are defined.
            if 'release' not in locals():
                def release():
                    pass
            if 'close' not in locals():
                def close():
                    pass
            if 'unlink' not in locals():
                def unlink():
                    pass
            raise
        finally:
            def cleanup():
                self._cleanup = lambda: None
                if master and self._flag is not None:
                    self._flag[:] = b'\x01' # Set the flag to signal this buffer has been unlinked.
                release()
                close()
                if master:
                    # Unlinking has to happen last, but the flag needs to be set before the pointers are released.
                    unlink()
            self._cleanup = cleanup

    def _is_unlinked(self):
        """
        Returns the linked status of this buffer.
        :return: True if this buffer has been unlinked.
        """
        if self._flag is None:
            return True
        else:
            return self._flag[:] == b'\x01'

    @contextmanager
    def get_direct(self):
        """
        Context manager to get a direct reference to the shared memory, with access controlled by a thread lock.
        :return: A memoryview into the buffer.
        """
        with self._lock:
            yield self._ary

    def size(self):
        """
        Get the size of the buffer.
        :return: The size in bytes.
        """
        return len(self._ary)

    @contextmanager
    def asarray_direct(self, dtype, shape):
        """
        Context manager to get a direct reference to the shared memory in the form of a numpy array, with 
        access controlled by a lock. The size of the requested numpy array may be smaller than the size of
        this buffer. If the requested size is larger, a SharedMemoryError exception is thrown.

        :param dtype: The numpy datatype of the exposed ndarray interface.
        :param shape: The shape of the exposed ndarray interface.
        :return: The ndarray interface to this buffer.
        """
        nbytes = numpy_utils._calc_nbytes(dtype, shape)
        if self.size() < nbytes:
            raise SharedMemoryError("Stage is smaller than requested array: {} < {}".format(self.size(), nbytes))
        with self._lock:
            yield np.array(self._ary[:nbytes], copy=False).view(dtype).reshape(shape)

    def set_to(self, value):
        """
        Set the buffer to the given numpy array. The array may be smaller than the size of the buffer.
        :param value: A numpy ndarray with the desired values.
        """
        with self.asarray_direct(value.dtype, value.shape) as ary:
            ary[...] = value

    def is_closed(self):
        """
        Check if this buffer has been closed.
        :return: True if the buffer has been closed.
        """
        return self._closed

    def close(self):
        """
        Close the buffer. When this is called, there should be no dangling references to the underlying buffer, or
        else an exception will be thrown.
        """
        self._closed = True
        self._cleanup()
    
    def __del__(self):
        self.close()