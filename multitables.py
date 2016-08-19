# Copyright (C) 2016 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

import numpy as np
import multiprocessing
import threading

__author__ = "G. H. Collin"
__version__ = "1.0.0"


class QueueClosed:
    """Signals that the queue has closed and no more elements will be added."""
    pass


class BarrierImpl:
    """Actual implementation of a barrier."""

    def __init__(self, n_procs):
        """
        Create a barrier that waits for n_procs processes.

        :param n_procs: The number of processes to wait for.
        """
        self.n_procs = n_procs
        self.count = multiprocessing.Value('i', 0, lock=False)
        self.cvar = multiprocessing.Condition()

    def wait(self):
        """Wait until all processes have reached the barrier."""
        with self.cvar:
            self.count.value += 1
            self.cvar.notify_all()
            while self.count.value < self.n_procs:
                self.cvar.wait()

    def reset(self):
        """Re-set the barrier so that it can be used again."""
        with self.cvar:
            self.count.value = 0


class Barrier:
    """Self re-setting synchronisation barrier. Resets after all processes have passed."""

    def __init__(self, n_procs):
        """
        Create a self re-setting barrier that waits for n_procs processes.

        :param n_procs: The number of processes to wait for.
        """
        self.barrier_A, self.barrier_B = BarrierImpl(n_procs), BarrierImpl(n_procs)

    def wait(self):
        """Wait until all processes have reached the barrier."""
        self.barrier_A.wait()
        # The current barrier (barrier_A) is switched with the reserve barrier.
        # This is because the current barrier cannot be safely reset until the reserve barrier has been passed.
        self.barrier_A, self.barrier_B = self.barrier_B, self.barrier_A
        self.barrier_A.reset()


class SharedCircBuf:
    """A circular buffer for numpy arrays that uses shared memory for inter-process communication."""
    def __init__(self, queue_size, ary_template):
        """
        Create the circular buffer. An array template must be passed to determine the size of the buffer elements.

        :param queue_size: Number of arrays to use as buffer elements.
        :param ary_template: Buffer elements match this array in shape and data-type.
        """
        import multiprocessing.sharedctypes
        # The buffer uses two queues to synchonise access to the buffer.
        # Element indices are put and got from these queues.
        # Elements that are ready to be written to go into the write_queue.
        # Elements that are ready to be read go into the read_queue.
        # This is essentially a token passing process. Tokens are taken out of queues and are not put back until
        # operations are complete.
        self.read_queue = multiprocessing.Queue()
        self.write_queue = multiprocessing.Queue()

        elem_n_bytes = ary_template.nbytes
        elem_dtype = ary_template.dtype
        elem_size = ary_template.size
        elem_shape = ary_template.shape
        self.arys = []
        for i in range(queue_size):
            sarray = multiprocessing.sharedctypes.RawArray('b', elem_n_bytes)
            # Elements are numpy arrays that point into allocated shared memory.
            self.arys.append(np.frombuffer(sarray, dtype=elem_dtype, count=elem_size).reshape(elem_shape))
            # The queue of elements ready to be written to is initially populated with all elements.
            self.write_queue.put(i)

    class Guard:
        """with statement guard object for synchronisation of access to the buffer."""
        def __init__(self, queue, idx, ary):
            """
            The guard object returns ary, and once the guard ends the value of idx is put into queue.
            Used to put an element index representing a token back into the buffer queues once operations on the
            element are complete.

            :param queue: The queue to be populated when the guard ends.
            :param idx: The value to put into the queue.
            :param ary: Value to return in the with statement.
            """
            self.queue = queue
            self.idx = idx
            self.ary = ary

        def __enter__(self):
            return self.ary

        def __exit__(self, *args):
            self.queue.put(self.idx)

    class Closed(Exception):
        pass

    def put(self, in_ary):
        """
        Convenience method to put in_ary into the buffer.
        Blocks until there is room to write into the buffer.

        :param in_ary: The array to place into the buffer.
        :return:
        """
        with self.put_direct() as ary:
            ary[:] = in_ary

    def put_direct(self):
        """
        Allows direct access to the buffer element.
        Blocks until there is room to write into the buffer.

        :return: A guard object that returns the buffer element.
        """
        # Get the next element that can be written to.
        write_idx = self.write_queue.get()

        if write_idx is QueueClosed:
            self.write_queue.put(QueueClosed)
            raise self.Closed("Queue closed")

        # Once the guard is released, write_idx will be placed into read_queue.
        return self.Guard(self.read_queue, write_idx, self.arys[write_idx])

    def get(self):
        """
        Convenience method to get a copy of an array in the buffer.
        Blocks until there is data to be read.

        :return: A copy of the next available array.
        """
        with self.get_direct() as ary:
            result = ary.copy()
        return result

    def get_direct(self):
        """
        Allows direct access to the buffer element.
        Blocks until there is data that can be read.

        :return: A guard object that returns the buffer element.
        """
        # Get the next element that can be written.
        read_idx = self.read_queue.get()
        if read_idx is QueueClosed:
            self.read_queue.put(QueueClosed)
            return QueueClosed
        else:
            # Once the guard is released, read_idx will be placed into write_queue.
            return self.Guard(self.write_queue, read_idx, self.arys[read_idx])

    def close(self):
        """Close the queue, signalling that no more data can be put into the queue."""
        self.read_queue.put(QueueClosed)
        self.write_queue.put(QueueClosed)


class Streamer:
    """Provides methods for streaming data out of HDF5 files."""

    def __init__(self, filename, **kw_args):
        """
        An object for streaming data from filename. Additional key words arguments can be passed to the constructor.
        These arguments will be passed on to the open_file function from PyTables.

        :param filename: The HDF5 file to read from.
        :param kw_args: Additional options for opening the HDF5 file.
        """
        self.filename = filename
        self.h5_kw_args = kw_args

    @staticmethod
    def __read_process(self, path, read_size, cbuf, stop, barrier, cyclic, offset, read_skip):
        """
        Main function for the processes that read from the HDF5 file.

        :param self: A reference to the streamer object that created these processes.
        :param path: The HDF5 path to the node to be read from.
        :param read_size: The length of the block along the outer dimension to read.
        :param cbuf: The circular buffer to place read elements into.
        :param stop: The Event that signals the process to stop reading.
        :param barrier: The Barrier that synchonises read cycles.
        :param cyclic: True if the process should read cyclically.
        :param offset: Offset into the dataset that this process should start reading at.
        :param read_skip: How many element to skip on each iteration.
        :return: Nothing
        """
        # Multi-process access to HDF5 seems to behave better there are no top level imports of PyTables.
        import tables as tb
        h5_file = tb.open_file(self.filename, 'r', **self.h5_kw_args)
        ary = h5_file.get_node(path)

        i = offset
        while not stop.is_set():
            vals = ary[i:i + read_size]
            # If the read goes off the end of the dataset, then wrap to the start.
            if i + read_size > len(ary):
                vals = np.concatenate([vals, ary[0:read_size - len(vals)]])

            with cbuf.put_direct() as put_ary:
                put_ary[:] = vals

            i += read_skip
            if cyclic:
                # If the next iteration is past the end of the dataset, wrap it around.
                if i >= len(ary):
                    i %= len(ary)
                    barrier.wait()
            else:
                # But if cyclic mode is disabled, break the loop as the work is now done.
                if i + read_size > len(ary):
                    break

    def __get_batch(self, path, length, last=False):
        """
        Get a block of data from the node at path.

        :param path: The path to the node to read from.
        :param length: The length along the outer dimension to read.
        :param last: True if the remainder elements should be read.
        :return: A copy of the requested block of data as a numpy array.
        """
        import tables
        h5_file = tables.open_file(self.filename, 'r')
        h5_node = h5_file.get_node(path)

        if len(h5_node) == 0:
            raise Exception("Cannot read from empty dataset.")

        # If the length isn't specifed, then fall back to default values.
        if length is None:
            chunkshape = h5_node.chunkshape
            # If the array isn't chunked, then try to make the block close to 128KB.
            if chunkshape is None:
                default_length = 128*2**10//h5_node[0].nbytes  # Divides by one row of the dataset.
                length = min(h5_node.shape[0], default_length)
            # If it is chunked, then use the chunkshape for best performance.
            else:
                length = chunkshape[0]

        if last:
            example = h5_node[length*(len(h5_node)//length):].copy()
        else:
            example = h5_node[:length].copy()

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
        def __init__(self, cbuf, stop, block_size):
            """
            A queue to provide a clean access API to the user.
            :param cbuf: The internal circular buffer.
            :param stop: The stop Event.
            :param block_size: The block size.
            """
            self.cbuf = cbuf
            self.stop = stop
            self.block_size = block_size

        def get(self):
            """
            Get the next element from the queue of data. This method returns a guard object that shnchonises access
            to the underlying buffer. The guard, when placed in a with statement, returns a reference to the next
            available element in the buffer.
            This method blocks until data is available.

            :return: A guard object that returns a reference to the element.
            """
            return self.cbuf.get_direct()

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

    def get_queue(self, path, n_procs=4, read_ahead=5, cyclic=False, block_size=None):
        """
        Get a queue that allows direct access to the internal buffer. If the dataset to be read is chunked, the
        block_size should be a multiple of the chunk size to maximise performance. In this case it is best to leave it
        to the default. When cyclic=False, and block_size does not divide the dataset evenly, the remainder elements
        will not be returned by the queue. When cyclic=True, the remainder elements will be part of a block that wraps
        around the end and includes element from the beginning of the dataset.

        :param path: The HDF5 path to the dataset that should be read.
        :param n_procs: The number of background processes used to read the datset in parallel.
        :param read_ahead: The number of blocks to allocate in the internal buffer.
        :param cyclic: True if the queue should wrap at the end of the dataset.
        :param block_size: The size along the outer dimension of the blocks to be read. Defaults to a multiple of
            the chunk size, or to a 128KB sized block if the dataset is not chunked.
        :return: A queue object that allows access to the internal buffer.
        """
        # Get a block_size length of elements from the dataset to serve as a template for creating the buffer.
        # If block_size=None, then get_batch calculates an appropriate block size.
        example = self.__get_batch(path, block_size)
        block_size = example.shape[0]

        if read_ahead is None:
            # 2x No. of processes for writing, 1 extra for reading.
            read_ahead = 2*n_procs + 1

        cbuf = SharedCircBuf(read_ahead, example)
        stop = multiprocessing.Event()
        barrier = Barrier(n_procs)

        procs = []
        for i in range(n_procs):
            # Each process is offset in the dataset by i*block_size
            # The skip length is set to n_procs*block_size so that no block is read by 2 processes.
            process = multiprocessing.Process(target=Streamer.__read_process, args=(
                self, path, block_size, cbuf, stop, barrier, cyclic,
                i * block_size, n_procs * block_size
            ))
            process.daemon = True
            process.start()
            procs.append(process)

        # If the queue is not cyclic, then the cessation of reading data needs to be monitored.
        if not cyclic:

            # This closure defines a background thread that waits until all processes have finished.
            # At this point, all data from the dataset has been read, and the buffer is closed.
            def monitor():
                for p in procs:
                    p.join()
                cbuf.close()

            monitor_thread = threading.Thread(target=monitor)
            monitor_thread.daemon = True
            monitor_thread.start()

        return Streamer.Queue(cbuf, stop, block_size)

    def get_generator(self, path, n_procs=4, read_ahead=None, cyclic=False, block_size=None):
        """
        Get a generator that allows convenient access to the streamed data.
        Elements from the dataset are returned from the generator one row at a time.
        Unlike the direct access queue, this generator also returns the remainder elements.
        See the get_queue method for documentation on the parameters.

        :param path:
        :param n_procs:
        :param read_ahead:
        :param cyclic:
        :param block_size:
        :return: A generator that iterates over the rows in the dataset.
        """
        q = self.get_queue(path=path, read_ahead=read_ahead, n_procs=n_procs, cyclic=cyclic, block_size=block_size)

        # This generator just implements a standard access pattern for the direct access queue.
        for guard in q.iter():
            with guard as batch:
                batch_copy = batch.copy()

            for row in batch_copy:
                yield row

        last_batch = self.get_remainder(path, q.block_size)
        for row in last_batch:
            yield row

        q.close()
