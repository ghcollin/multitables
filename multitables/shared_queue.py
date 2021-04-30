# Copyright (C) 2019 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

import multiprocessing
import threading
import time
import struct
import collections
from contextlib import contextmanager

import sys
_PYTHON3 = sys.version_info > (3, 0)

if _PYTHON3:
    import queue
else:
    import Queue as queue

def HeartbeatCondition(heartbeat, *args, **kw_args):
    """
    The default multiprocessing implementation of a condition variable has a performance regression when
    waiting on the variable with a timeout. As not all platforms implement a condition variable with a
    timeout, the default implementation runs a spin-lock on the variable, sleeping for increasing periods
    of time. This results in poor performance.

    This function defines a condition variable that is periodically woken up by a heartbeat notificaiton.
    This allows for a timeout-like behaviour while avoiding a spin-lock on the variable. This approach 
    effectively trades better wake performance for increased variability in when the timout actually occurs
    and spirious wake-ups, which is acceptable in this scenario.
    
    :param heartbeat: The period at which the heartbeat occurs, this should be reasonably close to the desired timeout period.
    :param args: Positional parameters which are forwarded to the underlying condition variable.
    :param kw_args: Keyword parameters which are forwarded to the underlting condition variable.
    :return: The condition variable with a heartbeat thread attached.
    """
    cvar = multiprocessing.Condition(*args, **kw_args)

    def heart():
        while True:
            with cvar:
                cvar.notify_all()
            time.sleep(heartbeat)
    
    timeout_thread = threading.Thread(target=heart)
    timeout_thread.daemon=True
    timeout_thread.start()
    cvar.__heartbeat_thread = timeout_thread
    return cvar

class SharedQueue:
    """
    multiprocessing.queue serialises python objects and stuffs them into a Pipe object.
    This serialisation happens in a background thread, and it not tied to the put() call.
    As such, additional unnecessary pickle serialisation happens when the queue input is
    already an array of bytes.

    This class defines a queue that is backed by a segment of shared memory which can be
    written to directly. A multiprocessing.queue side-channel is implemented for inputs
    that exceed the size of the shared memory.
    """
    def __init__(self, elem_size, queue_len):
        """
        Create the shared queue.
        :param elem_size: The size, in bytes, of each element in the queue.
        :param queue_len: The number of free elements to allocate for the queue.
        """
        self._elem_size = elem_size
        self._queue_len = queue_len

        import multiprocessing.sharedctypes
        # This lock controls access to the shared memory.
        self._vals_lock = multiprocessing.Lock()
        # Separate condition variables are used for putting and getting elements from the queue, which share the same lock,
        # as they encode different signals for the same resource.
        self._cvar_putters = multiprocessing.Condition(self._vals_lock)
        self._cvar_getters = HeartbeatCondition(0.1, self._vals_lock)
        
        # Define the size in bytes of the 'size' value, which is written into the memory to store how long the input is.
        self._size_format = '@L'
        self._size_t_size = len(struct.pack(self._size_format, 0))
        self._size_offset = self._elem_size

        # Define the size in bytes of the 'flag' value, which signifies when an input has been routed through the side-channel.
        self._flag_format = '@?'
        self._flag_size = len(struct.pack(self._flag_format, False))
        self._flag_offset = self._size_offset + self._size_t_size

        # Define the size of the metadata.
        self._meta_size = self._flag_size + self._size_t_size
        # Define the size of the 'block', which is the metadata-element data pair.
        self._block_size = self._meta_size + elem_size

        # A shared array is used to store items in the queue
        self._sary = multiprocessing.sharedctypes.RawArray('b', self._block_size*queue_len)
        self._vals = None

        # tail is the next item to be read from the queue
        self._tail = multiprocessing.sharedctypes.RawValue('l', 0)
        # size is the current number of items in the queue. head = tail + size
        self._size = multiprocessing.sharedctypes.RawValue('l', 0)

        self._side_channel = multiprocessing.Queue()

    def _init_delayed(self):
        """
        When Windows launches a new process, it attempt to transmit the current 
        execution state through pickle. The following objects cannot be pickled,
        and so they are only initialised after the process starts.
        """
        if _PYTHON3:
            # Python3 memoryview requires a cast to bytes but Python 2 has no cast attribute.
            self._vals = memoryview(self._sary).cast('B')
        else:
            self._vals = memoryview(self._sary)
        
        # If a request to put an input into the queue happen when the queue is full, it will be put into a buffer which feeds
        # the element in when the queue empties.
        self._put_buffer_cvar = threading.Condition()
        self._put_buffer = collections.deque()
        self._put_buffer_thread = threading.Thread(target=self._buffer_thread)
        self._put_buffer_thread.daemon = True
        # Note the thread is not started yet. It is started on demand.

    def _buffer_thread(self):
        """
        The controling thread for the input buffer.
        """
        while True:
            with self._put_buffer_cvar:
                while len(self._put_buffer) == 0:
                    self._put_buffer_cvar.wait()
                bytes = self._put_buffer.popleft()
            self._put_sync(bytes, block=True)

    def elem_size(self):
        """
        Get the size of the elements in this queue, i.e. the maximum input size.
        :return: The size in bytes.
        """
        return self._elem_size

    def put(self, bytes, block=True):
        """
        Place a value into the queue. If the size of the value is larger than the element size, it is placed in
        the side channel instead. If the queue is full, and block=False, a queue.Full exception is raised.

        :param bytes: The array of bytes to be placed into the queue.
        :param block: Whether to block if the queue is full. Default to True.
        """
        # Perform the delayed initialisation if necessary.
        if self._vals is None:
            self._init_delayed()
        
        # First check if there's anything ahead of this in the buffer.
        while True:
            with self._put_buffer_cvar:
                if len(self._put_buffer) == 0:
                    # The buffer is clear, now try the requested value.
                    self._put_sync(bytes, block)
                    break
                buffered_bytes = self._put_buffer[0]
                try:
                    self._put_sync(buffered_bytes, block)
                except queue.Full:
                    raise
                else:
                    self._put_buffer.popleft()


    def _put_sync(self, bytes, block):
        """
        Internal method for attempting to synchronously put a value into the queue.
        """
        if len(bytes) > self._elem_size:
            # If the input exceeds the element size, it goes into the side channel.
            success = self._put_shared(True, block) # Place a flag into the shared memory to signify the side-channel use.
            if success:
                # If there was enough room in the queue for the flag, put the input into the side-channel.
                self._side_channel.put(bytes)
        else:
            # Input is small enough for the shared memory, put it directly there.
            success = self._put_shared(False, block, bytes)
        if not success:
            raise queue.Full()

    def _place_block(self, bytes, flag):
        """
        Internal method for actually writing to the shared memory. Assumes the condition variable is taken.
        """
        assert(self._size.value < self._queue_len)
        # Calculate where the head of the queue is, wrapping around the end of the memory.
        head = (self._tail.value + self._size.value) % self._queue_len
        # Find the offset in bytes where the head of the queue is.
        ptr = head * self._block_size
        # Grab the next block as 
        block_m = self._vals[ptr:ptr+self._block_size]

        # Always write the value of the flag.
        block_m[self._flag_offset:self._flag_offset+self._flag_size] = struct.pack(self._flag_format, flag)

        # If the flag is not True, actually write the bytes as well.
        if not flag:
            # Write the bytes to the start of the block.
            block_m[:len(bytes)] = bytes
            # The input size (and the flag) are placed at the end of the block.
            block_m[self._size_offset:self._size_offset+self._size_t_size] = struct.pack(self._size_format, len(bytes))
        
        # Increase the size of the queue.
        self._size.value += 1
        self._cvar_getters.notify()

    def _put_shared(self, flag, block, bytes=b''):
        """
        Internal method for managing writing to the shared memory.
        """
        assert(len(bytes) <= self._elem_size)
        with self._vals_lock:
            while self._size.value >= self._queue_len: # Only terminate this loop once there's room in the queue.
                assert(self._size.value == self._queue_len)
                if not block:
                    # If a non-blocking put is requested, terminate the method now and report failure.
                    return False
                self._cvar_putters.wait()

            # At this point, there is room in the queue, so actually write to memory and report success.
            self._place_block(bytes, flag)
            return True
    
    def put_async(self, bytes):
        """
        Place a value into the queue. If the size of the value is larger than the element size, it is placed in
        the side channel instead. If the queue is full, the input is placed into a buffer to be placed into the
        queue when space is available.

        :param bytes: An array of bytes to place into the queue.
        """
        try:
            self.put(bytes, block=False)
        except queue.Full:
            with self._put_buffer_cvar:
                # First check if the buffer thread is running. If it isn't, start it now.
                # This happens here to prevent issues with forking when threads are present.
                if self._put_buffer_thread is not None and not self._put_buffer_thread.is_alive():
                    self._put_buffer_thread.start()
                
                # Place the input into the buffer and notify it of a new value.
                self._put_buffer.append(bytes)
                self._put_buffer_cvar.notify()

    @contextmanager
    def get_direct(self, block=True, timeout=None):
        """
        Get a value from the queue, with direct access to the underlying memory controlled by a context manager.
        If the queue is empty, a queue.Empty exception is raised.

        :param block: Whether to block and wait for the next value to appear in the queue.
        :param timeout: In conjunction with block=True, how long to wait before raising queue.Empty.
        :return: A context manager that yields a memoryview into the underlying memory.
        """
        # Perform the delayed initialisation if necessary.
        if self._vals is None:
            self._init_delayed()
        
        # If a timeout is requested, start the clock.
        if timeout is not None:
            t_start = time.time()
        
        with self._vals_lock:
            # Wait while the queue is empty.
            while self._size.value <= 0:
                assert(self._size.value == 0)
                # If non-blocking get is requested, raise the Empty exception.
                # If blocking get is requested with timeout, check if timeout has expired and if so, raise the Empty exception.
                if (not block) or (timeout is not None and time.time() - t_start >= timeout):
                    raise queue.Empty()

                self._cvar_getters.wait()
 
            #while True:
            #    if self._size.value > 0:
            # Find the offset in bytes of where the tail is located in memory.
            ptr = self._tail.value * self._block_size
            # Get the tail of the queue as a memoryview.
            block_m = self._vals[ptr:ptr+self._block_size]

            flag, = struct.unpack(self._flag_format, block_m[self._flag_offset:self._flag_offset+self._flag_size])

            if flag:
                # If a flag was raised, attempt to get the value from the side-channel.
                rval = self._side_channel.get(block=block)
                # If self._side_channel.get is called with block=False, and the value hasn't made it through the
                # side-channel yet, then a queue.Empty exception is raised and allowed to propagate back.
                # In this case, the tail of the queue will not be updated, so the next get request will
                # return to this exact situation again, until the value is available.
            else:
                # Otherwise, pull it from the memory.
                # First get the size of the value from the metadata.
                rsize, = struct.unpack(self._size_format, block_m[self._size_offset:self._size_offset+self._size_t_size])
                # Then get the value itself.
                rval = block_m[:rsize]
        
            try:
                yield rval
            finally:
                # If the value was yielded, make sure to remove the element from the queue.
                self._tail.value = (self._tail.value + 1) % self._queue_len
                self._size.value -= 1
                self._cvar_putters.notify()