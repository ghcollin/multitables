# Copyright (C) 2021 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

import threading

import sys
_PYTHON3 = sys.version_info > (3, 0)

if _PYTHON3:
    import queue
else:
    import Queue as queue

from . import reader
from . import signals
from .signals import QueueClosed
from . import request
from . import stage
from . import dataset_ops

class Streamer:
    """Provides methods for streaming data out of HDF5 files."""

    def __init__(self, filename, **kw_args):
        """
        An object for streaming data from filename. Additional key words arguments can be passed to the constructor.
        These arguments will be passed on to the open_file function from PyTables.

        :param filename: The HDF5 file to read from.
        :param kw_args: Additional options for opening the HDF5 file.
        """
        self._filename = filename
        self._h5_kw_args = kw_args
        self._closed = threading.Event()

    def __get_batch(self, path, length, field=None, last=False):
        """
        Get a block of data from the node at path.

        :param path: The path to the node to read from.
        :param length: The length along the outer dimension to read.
        :param last: True if the remainder elements should be read.
        :return: A copy of the requested block of data as a numpy array.
        """
        import tables
        h5_file = tables.open_file(self._filename, 'r')
        h5_node = h5_file.get_node(path)

        node_shape = h5_node.shape

        if node_shape[0] == 0:
            raise RuntimeError("Cannot read from empty dataset.")

        # If the length isn't specified, then fall back to default values.
        if length is None:
            chunk_shape = h5_node.chunkshape
            if chunk_shape is not None and chunk_shape[0] == 0:
                import warnings
                warnings.warn(("Outer dimension of chunk is zero {}. This shouldn't happen," + \
                        " but multitables will assume this means there is no chunk information.").format(chunk_shape), RuntimeWarning)
                chunk_shape = None
            # If the array isn't chunked, then try to make the block close to 128KB.
            if chunk_shape is None:
                if field is None:
                    row_nbytes = h5_node[0].nbytes
                else:
                    row_nbytes = h5_node[0][field].nbytes
                default_length = 128*2**10//row_nbytes  # Divides by one row of the dataset.
                length = min(h5_node.shape[0], default_length)
            # If it is chunked, then use the chunkshape for best performance.
            else:
                chunk_length = chunk_shape[0]
                length = chunk_length # try to aim for 3MB

        if last:
            example = h5_node[length*(len(h5_node)//length):]
        else:
            example = h5_node[:length]

        if field is not None:
            example = example[field]
        example = example.copy()

        h5_file.close()
        return example

    def get_remainder(self, path, block_size):
        """
        Get the remainder elements. These elements will not be read in the direct queue access cyclic=False mode.

        :param path: The HDF5 path to the dataset to be read.
        :param block_size: The block size is used to calculate which elements will remain.
        :return: A copy of the remainder elements as a numpy array.
        """
        return self.__get_batch(path, length=block_size, last=True)

    class Queue:
        """Abstract queue that is backed by the internal circular buffer."""
        def __init__(self, request_pool, stop, block_size):
            """
            A queue to provide a clean access API to the user.
            :param cbuf: The internal circular buffer.
            :param stop: The stop Event.
            :param block_size: The block size.
            """
            self._pool = request_pool
            self.stop = stop
            self.block_size = block_size

        def get(self):
            """
            Get the next element from the queue of data. This method returns a guard object that synchronises access
            to the underlying buffer. The guard, when placed in a with statement, returns a reference to the next
            available element in the buffer.
            This method blocks until data is available.

            :return: A guard object that returns a reference to the element.
            """
            next_req = self._pool.next()
            if next_req is QueueClosed:
                self._pool.add(QueueClosed)
                return next_req
            else:
                return next_req.get_proxy()

        def iter(self):
            """
            Convenience method for easy iteration over elements in the queue.
            Each iteration of the iterator will block until an element is available to be read.

            :return: An iterator for the queue.
            """
            while True:
                guard = self.get()
                if guard is QueueClosed:
                    break
                else:
                    yield guard

        def close(self):
            """Signals to the background processes to stop, and closes the queue."""
            self.stop.set()

        def __del__(self):
            self.close()

    def get_queue(self, path, n_procs=None, read_ahead=None, cyclic=False, block_size=None, ordered=False, field=None, remainder=False):
        """
        Get a queue that allows direct access to the internal buffer. If the dataset to be read is chunked, the
        block_size should be a multiple of the chunk size to maximise performance. In this case it is best to leave it
        to the default. When cyclic=False, and block_size does not divide the dataset evenly, the remainder elements
        will not be returned by the queue. When cyclic=True, the remainder elements will be part of a block that wraps
        around the end and includes element from the beginning of the dataset. By default, blocks are returned in the
        order in which they become available. The ordered option will force blocks to be returned in on-disk order.

        :param path: The HDF5 path to the dataset that should be read.
        :param n_procs: The number of background processes used to read the datset in parallel.
        :param read_ahead: The number of blocks to allocate in the internal buffer.
        :param cyclic: True if the queue should wrap at the end of the dataset.
        :param block_size: The size along the outer dimension of the blocks to be read. Defaults to a multiple of
            the chunk size, or to a 128KB sized block if the dataset is not chunked.
        :param ordered: Force the reader return data in on-disk order. May result in performance penalty.
        :param field: The field or column name to read. If omitted, all fields/columns are read.
        :param remainder: Also return the remainder elements, these will be returned as array smaller than the block size.
        :return: A queue object that allows access to the internal buffer.
        """
        # Get a block_size length of elements from the dataset to serve as a template for creating the buffer.
        # If block_size=None, then get_batch calculates an appropriate block size.
        example = self.__get_batch(path, block_size)
        block_size = example.shape[0]

        if n_procs is None:
            n_procs = 4

        if read_ahead is None:
            # 2x No. of processes for writing, 1 extra for reading.
            read_ahead = 2*n_procs + 1
        if read_ahead == 0:
            raise RuntimeError("The read_ahead parameter should be a strictly positive number or None.")

        request_pool = request.RequestPool()
        if ordered:
            def notify(req):
                pass
        else:
            def notify(req):
                request_pool.add(req)
        
        self._dataset_reader = reader.Reader(self._filename, n_procs, notify=notify, **self._h5_kw_args)

        dataset = self._dataset_reader.get_dataset(path)

        stage_pool = stage.StagePool(dataset, block_size, read_ahead, timeout=0.1)

        self._stop = threading.Event()
        
        def request_spool():
            i = 0
            try:
                while not self._stop.is_set() and not self._closed.is_set():
                    start_idx, stop_idx = i, i + block_size
                    if stop_idx > dataset.shape[0]:
                        if cyclic:
                            split_idx = dataset.shape[0]
                            stop_idx = block_size - (split_idx - start_idx)
                            op = dataset_ops.JoinedSlicesOp(path, field, start_idx, split_idx, None, 0, stop_idx, None)
                        else:
                            stop_idx = dataset.shape[0]
                            if remainder and start_idx < stop_idx:
                                op = dataset[start_idx:stop_idx]
                            else:
                                break
                    else:
                        op = dataset[start_idx:stop_idx]
                    try:
                        req = self._dataset_reader.request(op, stage_pool)
                    except queue.Empty:
                        # Raised when stage pool is empty
                        continue
                    if ordered:
                        request_pool.add(req)
                    i = stop_idx
            finally:
                if ordered:
                    request_pool.add(QueueClosed)
                self._dataset_reader.close()

        self._request_thread = threading.Thread(target=request_spool)
        #self._request_thread.daemon = True
        self._request_thread.start()
                    
        return Streamer.Queue(request_pool, self._stop, block_size)

    def get_generator(self, path, n_procs=None, read_ahead=None, cyclic=False, block_size=None, ordered=False, field=None, remainder=True):
        """
        Get a generator that allows convenient access to the streamed data.
        Elements from the dataset are returned from the generator one row at a time.
        Unlike the direct access queue, this generator also returns the remainder elements.
        Additional arguments are forwarded to get_queue.
        See the get_queue method for documentation of these parameters.

        :param path:
        :return: A generator that iterates over the rows in the dataset.
        """
        q = self.get_queue(path=path, n_procs=n_procs, read_ahead=read_ahead, cyclic=cyclic, block_size=block_size, ordered=ordered, field=field, remainder=remainder)

        try:
            # This generator just implements a standard access pattern for the direct access queue.
            for guard in q.iter():
                with guard as batch:
                    batch_copy = batch.copy()

                for row in batch_copy:
                    yield row

        finally:
            q.close()

    def __del__(self):
        self._closed.set()
