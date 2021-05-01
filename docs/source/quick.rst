Quick Start
***********

Installation
============

::

    pip install multitables

Alternatively, to install from HEAD, run

::

    pip install git+https://github.com/ghcollin/multitables.git

You can also `download <https://github.com/ghcollin/multitables/archive/master.zip>`_
or `clone the repository <https://github.com/ghcollin/multitables>`_ and run

::

    python setup.py install

``multitables`` depends on ``tables`` (the pytables package), ``numpy``, ``msgpack``, and ``wrapt``.
The package is compatible with the latest versions of Python 3, as pytables no longer supports Python 2.

Quick start: Streaming
======================

.. code:: python

    import multitables
    stream = multitables.Streamer(filename='/path/to/h5/file')
    for row in stream.get_generator(path='/internal/h5/path'):
        do_something(row)

Quick start: Random access
==========================

.. code:: python

    import multitables
    reader = multitables.Reader(filename='/path/to/h5/file')

    dataset = reader.get_dataset(path='/internal/h5/path')
    stage = dataset.create_stage(10) # Size of the shared
                                        # memory stage in rows

    req = dataset['col_A'][30:35] # Create a request as you
                                     # would index normally.

    future = reader.request(req, stage) # Schedule the request
    with future.get_unsafe() as data:
        do_something(data)
    data = None # Always set data to None after get_unsafe to
                # prevent a dangling reference

    # ... or use a safer proxy method

    req = dataset.col('col_A')[30:35,...,:100]

    future = reader.request(req, stage)
    with future.get_proxy() as data:
        do_something(data)

    # ... or provide a function to run on the data

    req = dataset.read_sorted('col_C', checkCSI=True, start=200, stop=300)

    future = reader.request(req, stage)
    future.get_direct(do_something)

    # ... or get a copy of the data

    req = dataset['col_A'][30:35,np.arange(500) > 45]

    future = reader.request(req, stage)
    do_something(future.get())

    # once done, close the reader
    reader.close(wait=True)

Examples
========

See the :doc:`How-To <howto>` for more in-depth documentation, and the
`unit tests <https://github.com/ghcollin/multitables/blob/master/multitables_test.py>`_ for complete examples.