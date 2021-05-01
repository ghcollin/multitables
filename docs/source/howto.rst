How To
******

All uses of the library start with creating a ``Streamer`` object, or a
``Reader`` object.

Streamer
********

The ``Streamer`` is designed for reading data from the dataset in approximately
(or optionally forced) sequential order.

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

Reader 
******

The ``Reader`` is designed for random access, using an interface that is
as close as possible to *numpy* indexing operations.

.. code:: python

    import multitables
    reader = multitables.Reader(filename="/path/to/h5/file", **kw_args)

Additional flags to pytables’ ``open_file`` function can be passed
through the optional keyword arguments.

Dataset and stage 
=================

The basic workflow is to first open the desired dataset using the internal
HDF5 path.

.. code:: python

    dataset = reader.get_dataset(path='/internal/h5/path')

Then, a stage must be created to host random access requests. This stage is
an area of shared memory that is allocated and shared with the background
reader processes. The result of all requests made with this stage must fit
inside the allocated memory of the stage.

.. code:: python

    stage = dataset.create_stage(shape=10)

The provided ``shape`` parameter may be the full shape of the stage using
the datatype of the dataset. Or, the shape may be left incomplete and the
missing shape dimensions will be filled with the dataset shape. In this
example, only the first dimension is specified, as so this stage has room
for 10 rows of the dataset.

Requests
========

Requests happen through three operations. First, the description of a request
is made through an indexing operation on the dataset.

.. code:: python

    req = dataset['col_A'][30:35]

Next, a future is made and a background task scheduled to fetch the requested
data and load it into the provided stage.

.. code:: python

    future = reader.request(req, stage)

Finally, the future is waited upon using a get operation. Four types of
get operations are provided. The first and simplest blocks on the task and
returns a copy of the data.

.. code:: python

    data = request.get()

In the next type, a copy is avoided by providing a function that will be
run with the data as the only argument. This get operation also blocks
until the data is available and the provided function finishes.

.. code:: python

    def do_something(data):
        pass
    data = request.get_direct(do_something)

The remaining two get operations use context managers to control access
to the shared memory resource without creating a copy. The first is unsafe,
in that if the resulting reference is not properly disposed of, memory
errors may result.

.. code:: python

    with future.get_unsafe() as data:
        do_something(data)
    data = None # Properly dispose of the reference

The final uses a wrapper object on the returned data, so that if the
reference is not properly disposed of, an exception will be safely called.

.. code:: python

    with future.get_unsafe() as data:
        do_something(data)
    data = None # Properly dispose of the reference

Cleaning up 
===========

Once finished, call the ``close`` method on the reader object.

.. code:: python

    reader.close(wait=True)

If the provided ``wait`` parameter is ``True``, the ``close`` call will
block until all background threads and processes have cleanly shut down.

Concurrent access pattern 
=========================

The following is an example of launching and reading requests in separate
threads. This uses the ``create_stage_pool`` method, that creates ``N_stages``
separate stages and places them in a rotating pool.

The ``RequestPool`` object is then used to create a queue of pending futures
that returns futures in the same order that they are inserted.

.. code:: python

    N_stages = 10

    stage_pool = dataset.create_stage_pool(1, N_stages)

    reqs = multitables.RequestPool()

    table_len = dataset.shape[0]
    def loader():
        for idx in range(table_len):
            reqs.add(reader.request(dataset[idx:idx+1], stage_pool))

    loader_thread = threading.Thread(target=loader)
    loader_thread.start()

    for idx in range(table_len):
        do_something(reqs.next().get())

    reader.close(wait=True)
