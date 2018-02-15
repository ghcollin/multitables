# Copyright (C) 2016 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

import numpy as np
import multiprocessing
import threading

__author__ = "G. H. Collin"
__version__ = "1.1.1"


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


class OrderedBarrier:
    """Synchronization barrier that allows processes through one at a time using an index based ordering."""

    class Guard:
        """with statement guard for ordered barrier"""
        def __init__(self, sync, index, next_index):
            """
            The guard waits on entry until the barrier is ready to let the process identified by index to pass.
            Once the guard exits, it increments the shared counter by the skip value and notifies waiting processes.
            :param sync: The parent OrderedBarrier object.
            :param index: The order to wait for.
            :param next_index: After exiting, release the process waiting on next_index.
            """
            self.sync = sync
            self.index = index
            self.next_index = next_index

        def __enter__(self):
            with self.sync.cvar:
                # Wait until index equals the internal shared counter.
                while self.sync.sval.value != self.index:
                    self.sync.cvar.wait()

        def __exit__(self, exc_type, exc_val, exc_tb):
            with self.sync.cvar:
                # Increase the shared counter by the skip value.
                self.sync.sval.value = self.next_index
                # Notifying the remaining processes allows the process with index = (this index + skip) to pass
                self.sync.cvar.notify_all()

    def __init__(self):
        """
        Create an ordered barrier. When processes wait on this barrier, they are let through one at a time based
        on the provided index. The first process to be let through should provide an index of zero. Each subsequent
        process to be let through should provide an index equal to the current value of the internal counter.
        """
        import multiprocessing.sharedctypes
        self.cvar = multiprocessing.Condition()
        self.sval = multiprocessing.sharedctypes.RawValue('L')
        self.sval.value = 0

    def wait(self, index, next_index=None):
        """
        Block until it is the turn indicated by index.
        :param index:
        :param next_index: Set the index to this value after finishing. Releases the process waiting on next_index.
            Defaults to incrementing index by 1.
        :return:
        """
        return OrderedBarrier.Guard(self, index, index+1 if next_index is None else next_index)


class GuardSynchronizer:
    """Synchronises access to the entering an exiting of a resource using ordered barriers."""

    class Guard:
        """Compositional guard between the provided resource and the internal ordered barriers."""
        def __init__(self, sync, inner_guard, index, next_index):
            """
            Creates a guard that will wait on the first barrier to enter the resource. Then wait on the second
            barrier to exit the resource.
            :param sync: The parent GuardSynchronizer object.
            :param inner_guard: The resource to control access to.
            :param index: The order to wait for.
            """
            self.sync = sync
            self.index = index, next_index
            self.inner_guard = inner_guard

        def __enter__(self):
            with self.sync.barrier_in.wait(*self.index):
                rval = self.inner_guard.__enter__()
            return rval

        def __exit__(self, exc_type, exc_val, exc_tb):
            with self.sync.barrier_out.wait(*self.index):
                self.inner_guard.__exit__(exc_type, exc_val, exc_tb)

    def __init__(self):
        """
        Create a synchronizer that consists of two ordered barriers.
        """
        self.barrier_in = OrderedBarrier()
        self.barrier_out = OrderedBarrier()

    def do(self, guard, index, next_index):
        """
        Create a guard that requires the resource guard to be entered and exited based on the order provided by index.
        :param guard: The context manager for the resource.
        :param index: The order to wait for.
        :param next_index: The next index to release.
        :return:
        """
        return GuardSynchronizer.Guard(self, guard, index, next_index)


class SafeQueue:
    """multiprocessing.Queue serialises python objects and stuffs them into a Pipe object.
    This serialisation happens in a background thread, and it not tied to the put() call.
    As such, no guarantee can be made about the order in which objects are put in the queue during threaded access.
    This poses a problem for the ordered mode, as it requires this guarantee.
    This class implements a very simple bounded queue that can guarantee ordering of inserted items."""
    def __init__(self, size):
        # The size of the queue is increased by one to give space for a QueueClosed signal.
        size += 1
        import multiprocessing.sharedctypes
        # The condition variable is used to both lock access to the internal resources and signal new items are ready.
        self.cvar = multiprocessing.Condition()
        # A shared array is used to store items in the queue
        sary = multiprocessing.sharedctypes.RawArray('b', 8*size)
        self.vals = np.frombuffer(sary, dtype=np.int64, count=size)
        self.vals[:] = -1
        # tail is the next item to be read from the queue
        self.tail = multiprocessing.sharedctypes.RawValue('l', 0)
        # size is the current number of items in the queue. head = tail + size
        self.size = multiprocessing.sharedctypes.RawValue('l', 0)

    def put(self, v):
        """
        Put an unsigned integer into the queue. This method always assumes that there is space in the queue.
        ( In the circular buffer, this is guaranteed by the implementation )
        :param v: The item to insert. Must be >= 0, as -2 is used to signal a queue close.
        :return:
        """
        if v is QueueClosed:
            v = -2
        else:
            assert(v >= 0)
        with self.cvar:
            assert(self.size.value < len(self.vals))

            head = (self.tail.value + self.size.value) % len(self.vals)
            self.vals[head] = v
            self.size.value += 1
            self.cvar.notify()

    def get(self):
        """
        Fetch the next item in the queue. Blocks until an item is ready.
        :return: The next unsigned integer in the queue.
        """
        with self.cvar:
            while True:
                if self.size.value > 0:
                    rval = self.vals[self.tail.value]
                    self.tail.value = (self.tail.value + 1) % len(self.vals)
                    self.size.value -= 1
                    if rval == -2:
                        return QueueClosed
                    assert(rval >= 0)
                    return rval
                self.cvar.wait()


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
        # Element indices are put and fetched from these queues.
        # Elements that are ready to be written to go into the write_queue.
        # Elements that are ready to be read go into the read_queue.
        # This is essentially a token passing process. Tokens are taken out of queues and are not put back until
        # operations are complete.
        self.read_queue = SafeQueue(queue_size)
        self.write_queue = SafeQueue(queue_size)

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
        def __init__(self, out_queue, arys, idx_op):
            """
            The guard object returns ary, and once the guard ends the value of idx is put into queue.
            Used to put an element index representing a token back into the buffer queues once operations on the
            element are complete.

            :param queue: The queue to be populated when the guard ends.
            :param idx: The value to put into the queue.
            :param ary: Value to return in the with statement.
            """
            self.out_queue = out_queue
            self.arys = arys
            self.idx_op = idx_op

        def __enter__(self):
            self.idx = self.idx_op()

            return self.arys[self.idx]

        def __exit__(self, *args):
            self.out_queue.put(self.idx)

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

    def __put_idx(self):
        write_idx = self.write_queue.get()

        if write_idx is QueueClosed:
            self.write_queue.put(QueueClosed)
            raise self.Closed("Queue closed")

        return write_idx

    def put_direct(self):
        """
        Allows direct access to the buffer element.
        Blocks until there is room to write into the buffer.

        :return: A guard object that returns the buffer element.
        """

        # Once the guard is released, write_idx will be placed into read_queue.
        return self.Guard(self.read_queue, self.arys, self.__put_idx)

    def get(self):
        """
        Convenience method to get a copy of an array in the buffer.
        Blocks until there is data to be read.

        :return: A copy of the next available array.
        """
        with self.get_direct() as ary:
            result = ary.copy()
        return result

    def __get_idx(self):
        read_idx = self.read_queue.get()

        if read_idx is QueueClosed:
            self.read_queue.put(QueueClosed)
            return QueueClosed

        return read_idx

    def get_direct(self):
        """
        Allows direct access to the buffer element.
        Blocks until there is data that can be read.

        :return: A guard object that returns the buffer element.
        """

        read_idx = self.__get_idx()

        if read_idx is QueueClosed:
            return QueueClosed

        # Once the guard is released, read_idx will be placed into write_queue.
        return self.Guard(self.write_queue, self.arys, lambda: read_idx)

    def close(self):
        """Close the queue, signalling that no more data can be put into the queue."""
        self.read_queue.put(QueueClosed)
        self.write_queue.put(QueueClosed)


def _Streamer__read_process(self, path, read_size, cbuf, stop, barrier, cyclic, offset, read_skip, sync):
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
    :param sync: GuardSynchonizer to order writes to the buffer.
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

        if sync is None:
            # If no ordering is requested, then just write to the next available space in the buffer.
            with cbuf.put_direct() as put_ary:
                put_ary[:] = vals
        else:
            # Otherwise, use the sync object to ensure that writes occur in the order provided by i.
            # So i = 0 will write first, then i = block_size, then i = 2*block_size, etc...
            # The sync object has two ordered barriers so that acquisition and release of the buffer spaces
            # are synchronized in order, but the actual writing to the buffer can happen simultaneously.
            # If only one barrier were used, writing to the buffer would be linearised.
            with sync.do(cbuf.put_direct(), i, (i+read_size) % len(ary)) as put_ary:
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

        # If the length isn't specified, then fall back to default values.
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

    def get_queue(self, path, n_procs=4, read_ahead=None, cyclic=False, block_size=None, ordered=False):
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

        # If ordering has been requested, create a synchronizer.
        sync = GuardSynchronizer() if ordered else None

        procs = []
        for i in range(n_procs):
            # Each process is offset in the dataset by i*block_size
            # The skip length is set to n_procs*block_size so that no block is read by 2 processes.
            process = multiprocessing.Process(target=_Streamer__read_process, args=(
                self, path, block_size, cbuf, stop, barrier, cyclic,
                i * block_size, n_procs * block_size, sync
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

    def get_generator(self, path, *args, **kw_args):
        """
        Get a generator that allows convenient access to the streamed data.
        Elements from the dataset are returned from the generator one row at a time.
        Unlike the direct access queue, this generator also returns the remainder elements.
        Additional arguments are forwarded to get_queue.
        See the get_queue method for documentation of these parameters.

        :param path:
        :return: A generator that iterates over the rows in the dataset.
        """
        q = self.get_queue(path=path, *args, **kw_args)

        try:
            # This generator just implements a standard access pattern for the direct access queue.
            for guard in q.iter():
                with guard as batch:
                    batch_copy = batch.copy()

                for row in batch_copy:
                    yield row

            last_batch = self.get_remainder(path, q.block_size)
            for row in last_batch:
                yield row

        finally:
            q.close()
