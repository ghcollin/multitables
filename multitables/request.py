# Copyright (C) 2019 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

import threading
from contextlib import contextmanager
import collections

from . import msgpack_ext
from .msgpack_ext import msgpack_registry
from . import signals
from . import resource_wrapper


class RequestDetails(msgpack_ext.MsgpackExtType):
    """ A simple class to contain the details of a request. """

    def __init__(self, req_id, key, shm_buf):
        self.req_id = req_id 
        self.key = key
        self.map_id = shm_buf.name
        self.size_nbytes = shm_buf.size_nbytes

    _pack_map = msgpack_ext.MsgpackExtType._pack_map + ['req_id', 'key', 'size_nbytes', 'map_id']
msgpack_registry.register_class(RequestDetails)


class Request:
    """ Public interface for managing requests. """

    def __init__(self, details, stage):
        """
        Creates a request with the provided details and associated stage.
        :param details: A RequestDetails instance that decribes the request.
        :param stage: A Stage instance from which the request result can be accessed.
        """
        self._details = details
        self._stage = stage
        self._ready = threading.Event()

    def _notify(self, dtype, shape, e=None):
        """
        Notify this request that the associated stage now contains the requested data.
        :param dtype: The resulting dtype of the request.
        :param shape: The resulting shape of the request.
        :param e: If not None, the exception raised when attempting to field this request.
        """
        self._dtype, self._out_shape = dtype, shape
        self._exception = e
        self._ready.set()
        
    @contextmanager
    def get_unsafe(self):
        """
        A context manager for accessing the result of the request directly. This manager waits until the
        request is fulfilled. Once ready, it yields a direct reference to the underlying shared memory.
        If an exception was raised when fielding this request, the exception is re-raised here.
        Use of this context manager can be unsafe, as it yields a direct reference to the shared memory.
        If this reference is not properly managed, it can lead to a dangling pointer that causes an
        exception when the associated stage is closed. The contents of this dangling pointer will also change
        when the associated stage is re-used for another request. It is recommended to use a safer access method,
        or immediately delete or set to None the local variable bound to the yielded reference after use.
        """
        self._ready.wait()
        with self._stage._release() as get_ary:
            if self._exception is not None:
                raise signals.CreateSubprocessException(self._exception)
            with get_ary(self._dtype, self._out_shape) as ary:
                yield ary

    def get_direct(self, action):
        """
        A safer method for directly accessing the shared memory. This method blocks until the request is
        fulfilled. Once ready, it called the provided action function with a direct reference to the shared
        memory as an argument. Care should be taken that this direct reference does not leave the scope
        of the function, or else the problems enumerated in the get_unsafe context manager may result.

        :param action: A function that takes one argument, which will be supplied as a direct reference
            to the shared memory.
        """
        with self.get_unsafe() as data:
            action(data)

    @contextmanager
    def get_proxy(self):
        """
        A safe context manager for indirectly accessing the shared memory. This manager waits until the
        request is fulfilled. Once ready, it yields a proxy to the underlying shared memory. Once the 
        context manager expires, the proxy will be released, and access to the shared memory is no longer
        possible. Any attempt to access the shared memory past this point raises an exception.
        """
        with self.get_unsafe() as data:
            try:
                proxy = resource_wrapper.ResourceWrapper(data)
                yield proxy
            finally:
                proxy.release()

    def get(self):
        """
        A safe method for accessing the result of the request. This method makes a copy of the result
        and returns it. This copy can be used in any fashion, as it no longer has resource contraints.
        :return: A copy of the result of the request.
        """
        with self.get_unsafe() as data:
            result = data.copy()
        return result


class RequestPool:
    """ A helper class for managing a pool of requests. """

    def __init__(self):
        self._queue = collections.deque()
        self._cvar = threading.Condition()

    def add(self, req):
        """
        Add a request to the pool.
        :param req: An object instance that should be place in the pool.
        """
        with self._cvar:
            self._queue.append(req)
            self._cvar.notify()

    def next(self):
        """
        Get the next object in the pool. Blocks until an object is available.
        :return: The next object in the pool.
        """
        with self._cvar:
            while len(self._queue) == 0:
                self._cvar.wait()
            return self._queue.popleft()


