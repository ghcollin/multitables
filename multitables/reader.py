# Copyright (C) 2021 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

import struct
import multiprocessing
import threading
from contextlib import contextmanager
import numpy as np

import sys
_PYTHON3 = sys.version_info > (3, 0)

if _PYTHON3:
    import queue
else:
    import Queue as queue

from . import shared_mem
from . import numpy_utils
from . import shared_queue
from . import signals
from .signals import QueueClosed
from . import dataset
from . import request

from . import msgpack_ext
request_packer = msgpack_ext.msgpack_registry
return_packer = msgpack_ext.PickleWrapper()

def _Reader__read_process(self):
    """
    The main read process for fielding requests. This function is defined outside of the reader class as
    older versions of Python have issues with processes being launched with class methods as targets.
    :param self: An instance of the Reader class.
    """
    try:
        h5_file = self._open_h5_file()
        # Define caches for nodes and buffers.
        nodes = {}
        bufs = {}

        while not self._stop.is_set():
            
            try:
                # Attempt to get a request from the request queue.
                with self._queue.get_direct(timeout=0.1) as data:
                    req = request_packer.unpack(data)
                data = None
            except queue.Empty:
                # If the attempt timed out, look through the buffers and release any
                # which have been unlinked. Then go back to waiting on a new request.
                for k in list(bufs.keys()):
                    if bufs[k]._is_unlinked():
                        bufs[k].close()
                        del bufs[k]
                continue

            # If the reader object has been closed, start the shutdown procedure.
            if req is QueueClosed:
                self._queue.put(request_packer.pack(QueueClosed))
                break

            try:

                # Caching of the shared memory is a larger performance improvement
                map_id = req.map_id
                try:
                    shm_ary = bufs[map_id]
                except KeyError:
                    shm_ary = shared_mem.SharedBuffer(map_id=map_id, size_nbytes=req.size_nbytes)
                    bufs[map_id] = shm_ary

                # If the request key is not present, it means that it has been stored in the stage instead
                if req.key is None:
                    # Retrieve the key from the stage
                    with shm_ary.get_direct() as buf:
                        keysize, = struct.unpack('@I', buf[-4:])
                        req.key = request_packer.unpack(buf[:keysize])
                else:
                    # Retrieve the key from the request data
                    req.key = request_packer.unpack(req.key)

                # Caching of the node is extraordinarily marginal performance improvement, but might as well
                path = req.key._path
                try:
                    node = nodes[path]
                except KeyError:
                    node = h5_file.get_node(path)
                    nodes[path] = node
                
                # This accesses the node and stores the reqeusted data in shm_ary, the data type and shape
                # of the result is returned.
                dtype, shape = req.key._apply(node, shm_ary)
                # Place the result meta-data into the notification queue.
                self._notify.put(return_packer.pack((req.req_id, numpy_utils._dtype_descr(dtype), shape)))

            # If there was an exception while accessing the data, notify the caller of the exception.
            except Exception as e:
                self._handle_exception(req.req_id, e)
    except KeyboardInterrupt as e:
        self._handle_exception(None, e)
    except Exception as e:
        # If an exception happened elsewhere, notify the reader object so it can shutdown gracefully.
        self._handle_exception(None, e)
    finally:
        # In exceptional circumstances, the process might not get as far as even allocating the h5_file variable.
        # Check for this case, and if everything is OK, close the HDF5 file.
        if 'h5_file' in locals():
            h5_file.close()

class Reader:
    """Provides methods for random access of HDF5 datasets."""

    def __init__(self, filename, n_procs=4, notify=None, **kw_args):
        """
        An object for reading data from filename. Additional key words arguments can be passed to the constructor.
        These arguments will be passed on to the open_file function from PyTables.

        :param filename: The HDF5 file to read from.
        :param n_procs: The number of background processes to use for fielding requests.
        :param notify: A function that takes one argument, will be called in a seperate thread whenever a request has been fulfilled.
        :param kw_args: Additional options for opening the HDF5 file.
        """
        # An exception could happen in the construction of the core, causing a further exception when __del__ 
        # is called, as the self._core attribute will not exist.
        self._core = None
        self._core = Reader.Core(filename, n_procs, notify, kw_args)

    class Core:
        def __init__(self, filename, n_procs, notify, kw_args):
            self._filename = filename
            self._h5_kw_args = kw_args
            # _queue is the request queue
            self._queue = shared_queue.SharedQueue(1024, 50)
            # Once requests have been handled, their result meta-data get placed in the _notify queue for dispatch.
            self._notify = shared_queue.SharedQueue(1024, 50)
            self._next_req_id = 0

            # Signal event for stopping the threads/processes launched by this object.
            self._stop = multiprocessing.Event()

            procs = []
            for _ in range(n_procs):
                process = multiprocessing.Process(target=_Reader__read_process, args=(
                    self,
                ))
                process.start()
                procs.append(process)

            def monitor():
                for p in procs:
                    p.join()
            
                # Once all reader processes have terminated, notify the notify spool that it should closed down.
                self._notify.put(return_packer.pack((QueueClosed,)))

            self._monitor_thread = threading.Thread(target=monitor)
            self._monitor_thread.start()

            # Signal event for closing the threads/processes launched by this object.
            self._close = threading.Event()

            # This dictionary keeps track of currently pending requests.
            self._open_reqs_dict = {}
            self._open_reqs_lock = threading.Lock()

            def notify_spool():
                """
                This function defines the notification spool. This spool waits until a request has been fulfilled
                by the reader processes, then it wakes up the associated request object. If an exception occured
                during the fulfillment of a request, the exception is passed to the request object so that it
                can be re-raised where the request was made.
                """
                while True:
                    # Get the next notification.
                    with self._notify.get_direct() as msg:
                        notification = return_packer.unpack(msg)
                    if len(notification) == 3:
                        # If the notification is for a fulfilled request.
                        with self._open_reqs() as open_reqs:
                            req_id, dtype, shape = notification # Get the result meta-data
                            # Find the request object, and notify it of the result + meta-data
                            open_reqs[req_id]._notify(np.dtype(dtype), shape)
                            # If the global notification call-back has been specified, also notify it
                            if notify is not None:
                                notify(open_reqs[req_id])
                            # Remove the request object from the pending request dictionary
                            del open_reqs[req_id]
                    else:
                        # Otherwise, this request means either an exception happened or the reader has closed.
                        notification = notification[0]
                        if notification is QueueClosed:
                            # If the reader has closed, begin the shutdown process.
                            if notify is not None:
                                # Let the global call-back know that this reader is shutting down.
                                notify(QueueClosed)
                            with self._open_reqs() as open_reqs:
                                # Now let all pending requests know that the reader has closed and they can no longer
                                # be fulfilled.
                                for key in open_reqs.keys():
                                    open_reqs[key]._notify(None, None, signals.QueueClosedException("This reader has been closed."))
                                open_reqs.clear()
                            break
                        elif isinstance(notification, Exception):
                            # If an exception happened
                            e = notification
                            if e.__req_id__ is None:
                                # Error happened when opening file, or reading stop event
                                # Something went very wrong, clear all and stop.
                                with self._open_reqs() as open_reqs:
                                    for key in open_reqs.keys():
                                        open_reqs[key]._notify(None, None, e)
                                    open_reqs.clear()
                                self.close()
                                raise signals.CreateSubprocessException(e)
                            else:
                                # Error happened when reading data, probably localised,
                                # so propagate it back to the offending request.
                                with self._open_reqs() as open_reqs:
                                    open_reqs[e.__req_id__]._notify(None, None, e)
                                    del open_reqs[e.__req_id__]

            self._notify_thread = threading.Thread(target=notify_spool)
            self._notify_thread.start()

        def _open_h5_file(self):
            """
            Convienience method for opening the HDF5 file this reader is bound to.
            :return: The pytables file object.
            """
            # Multi-process access to HDF5 seems to behave better there are no top level imports of PyTables.
            import tables as tb
            return tb.open_file(self._filename, 'r', **self._h5_kw_args)

        def _handle_exception(self, req_id, e):
            """
            Format the exception object and push it into the notification queue.
            :param req_id: The request ID number of the request that caused this exception.
            :param e: The raised exception.
            """
            import sys, traceback
            e.__traceback_str__ = traceback.format_tb(sys.exc_info()[2])
            e.__req_id__ = req_id
            self._notify.put(return_packer.pack((e,)))

        @contextmanager
        def _open_reqs(self):
            """
            Context manager for controlling access to the pending requests dictionary.
            """
            with self._open_reqs_lock:
                yield self._open_reqs_dict

        def _put_queue(self, obj):
            """
            Helper method for placing and object into the request queue.
            :param obj: The object to be placed in the queue.
            """
            self._queue.put_async(request_packer.pack(obj))

        def get_dataset(self, path):
            """
            Create a dataset proxy that can be used to create requests.
            :param path: The internal HDF5 path to the dataset within the HDF5 file.
            :return: A dataset proxy object.
            """
            import tables as tb
            h5_file = self._open_h5_file()
            h5_ary = h5_file.get_node(path)

            node_type = None
            if isinstance(h5_ary, tb.Table):
                node_type = dataset.TableDataset
            elif isinstance(h5_ary, tb.Array):
                node_type = dataset.ArrayDataset
            elif isinstance(h5_ary, tb.VLArray):
                node_type = dataset.VLArrayDataset
            else:
                raise RuntimeError("Selected dataset is not an array or table.")
        
            dtype = h5_ary.dtype
            shape = h5_ary.shape
            h5_file.close()

            return node_type(self, path, dtype, shape)

        def request(self, key, stage):
            """
            Generate and queue a request. The details of the request should be provided in the key argument, through
            operations on one of the dataset proxy objects generated by get_dataset. The result of the request will
            be stored in the provided stage. A request object will be returned, which can be used to wait on the result
            and access the result when it is ready.
            :param key: Operations created by a dataset proxy.
            :param stage: A stage or stage pool in which the result will be stored.
            :return: A request object.
            """
            if self._close.is_set():
                raise RuntimeError("Attempt to request data from a closed reader.")

            # Acquire the stage. Note the stage variable is overwritten, as the stage argument may actually be
            # a stage pool.
            stage, shm_buf = stage._acquire()

            # Serialise the key into bytes.
            keydata = request_packer.pack(key)
            if self._queue.elem_size() < (len(keydata) + 50) and len(keydata) <= (shm_buf.size() - 4):
                # If the key is too large to be stored in the request queue shared memory, but small enough that it
                # could be put into the stage, place it into the stage. This avoids passing it through the request
                # queue side channel. Note that 50 is added to the key data size to conservatively account for the
                # size of the RequestDetails object, and 4 is subtracted from the stage size to account for the size
                # of the integer that needs to be stored alongside the key data.
                key = None
            else:
                key = keydata
            
            with self._open_reqs() as open_reqs:
                req_id = self._next_req_id
                details = request.RequestDetails(req_id, key, shm_buf)
                
                req = request.Request(details, stage)
                open_reqs[req_id] = req
                self._next_req_id += 1

            if key is None:
                # If the key is to be stored in the stage.
                with shm_buf.get_direct() as buf:
                    self._put_queue(details)
                    buf[-4:] = struct.pack('@I', len(keydata)) # The last 4 bytes store the size of the key data.
                    buf[:len(keydata)] = keydata
            else:
                self._put_queue(details)
            
            return req

        def close(self, wait=False):
            """
            Close the reader. After this point, no more requests can be made. Pending requests will still be fulfilled.
            Any attempt to made additional requests will raise an exception. Once all requests have been fulfilled, the
            background processes and threads will be shut down.
            :param wait: If True, block until all background threads/processes have shut down. False by default.
            """
            if not self._close.is_set():
                self._close.set()
                self._queue.put(request_packer.pack(QueueClosed))

            if wait:
                self._notify_thread.join()

        def stop(self):
            """
            Stop the reader. All background processes and threads will immediately shut down. This will invalidate all
            pending requests. Attempts to access pending requests, or already waiting requests will raise an exception
            stating that the reader has stopped.
            """
            self.close()
            self._stop.set()

    def get_dataset(self, path):
        """
        Create a dataset proxy that can be used to create requests.
        :param path: The internal HDF5 path to the dataset within the HDF5 file.
        :return: A dataset proxy object.
        """
        return self._core.get_dataset(path)

    def request(self, key, stage):
        """
        Generate and queue a request. The details of the request should be provided in the key argument, through
        operations on one of the dataset proxy objects generated by get_dataset. The result of the request will
        be stored in the provided stage. A request object will be returned, which can be used to wait on the result
        and access the result when it is ready.
        :param key: Operations created by a dataset proxy.
        :param stage: A stage or stage pool in which the result will be stored.
        :return: A request object.
        """
        return self._core.request(key, stage)

    def close(self, wait=False):
        """
        Close the reader. After this point, no more requests can be made. Pending requests will still be fulfilled.
        Any attempt to made additional requests will raise an exception. Once all requests have been fulfilled, the
        background processes and threads will be shut down.
        
        :param wait: If True, block until all background threads/processes have shut down. False by default.
        """
        self._core.close(wait)

    def stop(self):
        """
        Stop the reader. All background processes and threads will immediately shut down. This will invalidate all
        pending requests. Attempts to access pending requests, or already waiting requests will raise an exception
        stating that the reader has stopped.
        """
        self._core.stop()

    def __del__(self):
        if self._core is not None:
            self.stop()
            self._core = None
