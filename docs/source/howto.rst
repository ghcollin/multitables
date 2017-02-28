How To
******

All uses of the library start with creating a ``Streamer`` object.

.. code:: python

    import multitables
    stream = multitables.Streamer(filename="/path/to/h5/file", **kw_args)

Additional flags to pytables’ ``open_file`` function can be passed
through the optional keyword arguments.

Direct access
=============

*multitables* allows low level access to the internal queue buffer. This
access is synchronised with a guard object. When the guard object is
created, an element of the buffer is reserved until the guard is
released.

.. code:: python

    queue = stream.get_queue(
        path='/h5/path/', # Path to dataset within the H5file.
        n_procs=4,        # Number of processes to launch for parallel reads. Defaults to 4.
        read_ahead=5,     # Size of internal buffer in no. of blocks. Defaults to 2*n_proc+1.
        cyclic=False,     # A cyclic reader wraps at the end of the dataset. Defaults to False.
        block_size=32,    # Size (along the outer dimension) of the blocks that will be read.
                          # Defaults to a multiple of the dataset chunk size, or a 128KB block.
                          # Should be left to the default or carefully chosen for chunked arrays,
                          # else performance degradation can occur.
        ordered=False     # Force the stream to return blocks in on-disk order. Useful if two
                          # datasets need to be read synchronously. This option may have a
                          # performance penalty.
    )

    while True:
        guard = queue.get() # Get the guard object, will block until data is ready.
        if guard is multitables.QueueClosed:
            break # Terminate the loop once the dataset is finished.
        with guard as block: # The guard returns the next block of data in the buffer.
            do_something(block) # Perform actions on the data

Note that ``block`` here is a numpy reference to the internal buffer.
Once the guard is released, ``block`` is no longer guaranteed to point
to valid data. If the data need to be saved for later use, make a copy
of it with ``block.copy()``.

Iterator
--------

A convenience iterator is supplied to make loop termination easier.

.. code:: python

    for guard in queue.iter():
        with guard as block:
            do_something(block)

Remainder elements
------------------

In all the previous cases, if the supplied ``read_size`` does not evenly
divide the dataset then the *remainder* elements will not be read. If
needed, these remainder elements can be accessed using the following
method

.. code:: python

    last_block = stream.get_remainder(path, queue.block_size)

Cyclic access
-------------

When the cyclic mode is enabled, the readers will wrap around the end of
the dataset. The check for the end of the queue is no longer needed in
this case.

.. code:: python

    while True:
        with queue.get() as block:
            do_something(block)

In cyclic access mode, the remainder elements are returned as part of a
wrapped block that includes elements from the end and beginning of the
dataset.

Once finished, the background processes can be stopped with
``queue.close()``.

Generator
=========

The generator provides higher level access to the streamed data.
Elements from the dataset are returned one row at a time. These rows
belong to a copied array, so they can be safely stored for later use.
The remainder elements are also included in this mode.

.. code:: python

    gen = stream.get_generator(path, n_procs, read_ahead, cyclic, block_size)

    for row in gen:
        do_something_else(row)

This is supposed to be in analogy to

.. code:: python

    dataset = h5_file.get_node(path)

    for row in dataset:
        do_something_else(row)

When cyclic mode is enabled, the generator has no end and will continue
until the loop is manually broken.

Concurrent access
=================

Python iterators and generators are not thread safe. The low level
direct access interface is thread safe.