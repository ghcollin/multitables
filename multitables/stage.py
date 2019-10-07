# Copyright (C) 2019 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

import threading
from contextlib import contextmanager
import collections

import sys
_PYTHON3 = sys.version_info > (3, 0)

if _PYTHON3:
    import queue
else:
    import Queue as queue

from . import shared_mem

class Stage:
    """ Resource manager for an underlying shared buffer. """

    def __init__(self, size_nbytes):
        """
        Create a stage that can host a numpy array of at most size_nbytes bytes.
        :param size_nbytes: The size, in bytes, of the underlying shared buffer.
        """
        self.size_nbytes = size_nbytes
        self._shm_buf = shared_mem.SharedBuffer(map_id=None, size_nbytes=self.size_nbytes)
        self._lock = threading.Lock()

    def _acquire(self):
        """
        Acquire the stage, locking it so that it cannot be acquired again until it is released.
        :return: A tuple, the first element is the stage that hosts the shared buffer, the second element
            is the underlying shared buffer.
        """
        if self._shm_buf.is_closed():
            raise RuntimeError("Stage has already been closed.")
        success = self._lock.acquire(False)
        if not success:
            raise RuntimeError("Stage already fielding another request.")
        return self, self._shm_buf

    @contextmanager
    def _release(self):
        """
        Release the stage, yielding a context manager that casts the underlying shared buffer into a numpy array.
        :return: A context manager that yields a context manager that in turn yields a numpy view into the buffer.
        """
        try:
            #yield self.__asarray_direct
            yield self._shm_buf.asarray_direct
        finally:
            self._lock.release()

    def close(self):
        """
        Close the stage, releasing the underlying shared memory resource.
        """
        self._shm_buf.close()

    def __del__(self):
        self.close()

class StagePool:
    """ Manages a pool from which stages can be acquired and returned. """

    class StagePoolWrapper:
        """ Wraps a stage instance, so that it will be returned to its parent pool when it is released. """

        def __init__(self, stage, pool):
            self._stage = stage
            self._pool = pool

        def _acquire(self):
            return self, self._stage._acquire()[1]
        
        @contextmanager
        def _release(self):
            try:
                with self._stage._release() as buf:
                    yield buf
            finally:
                self._pool._return(self)

    def __init__(self, dataset, stage_size, N_stages, timeout=None):
        """
        Create a stage pool based on a given dataset.
        :param dataset: Parent dataset that is used to calculate the size of the member stage elements.
        :stage_size: Size of each stage in the pool, this is passed to the constructor for the stage.
        :N_stages: The number of stages to be allocated in the pool.
        :timeout: Optional time out when attempting to acquire a stage from the pool.
        """
        self._stage_pool = collections.deque()
        if timeout is None:
            self._cvar = threading.Condition()
        else:
            from . import shared_queue
            self._cvar = shared_queue.HeartbeatCondition(timeout)
        self._timeout = timeout

        for _ in range(N_stages):
            self._stage_pool.append(StagePool.StagePoolWrapper(dataset.create_stage(stage_size), self))

    def _acquire(self):
        """
        Acquire a stage, and its underlying shared memory, from the pool. This method blocks until either a
        stage is available, or the optional timeout (provided in the pool constructor) has expired. If the
        timeout expires, a queue.Empty exception is raised.
        :return: A tuple, the first element is the acquired stage, the second element is its shared memory.
        """
        if self._timeout is not None:
            import time
            start = time.time()
        with self._cvar:
            while len(self._stage_pool) == 0:
                if self._timeout is not None and time.time() - start >= self._timeout:
                    raise queue.Empty()
                self._cvar.wait()
            return self._stage_pool.popleft()._acquire()

    def _return(self, stage):
        """
        Return a stage to the pool.
        """
        with self._cvar:
            self._stage_pool.append(stage)
            self._cvar.notify()
