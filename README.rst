``multitables`` is a python library designed for high speed access to HDF5 files.
Access to HDF5 is provided by the PyTables library (``tables``).
Multiple processes are launched to read a HDF5 in parallel, allowing concurrent decompression.
Data is streamed back to the invoker by use of shared memory space, removing the usual multiprocessing
communication overhead.

The data is organised by rows of an array (elements of the outer-most dimension), and groups of these rows form blocks.
Due to the concurrent nature of the library, there is **no guarantee** on the ordering of the rows and/or blocks
returned to the user. They are returned as they become available.

`Performance gains <http://multitables.readthedocs.io/en/latest/benchmark.html>`_ of at
least 2x can be achieved when reading from an SSD.

Licence
=======

This software is distributed under the MIT licence. 
See the `LICENSE.txt <https://github.com/ghcollin/multitables/blob/master/LICENSE.txt>`_ file for details.

Installation
============

::

    pip install git+https://github.com/ghcollin/multitables.git

or download and run
::

    python setup.py install

``multitables`` depends on ``tables`` (the pytables package) and ``numpy``.
The package is compatible with the latest versions of python 2 and 3.

Quick start
===========

.. code:: python

    import multitables
    stream = multitables.Streamer(filename='/path/to/h5/file')
    for row in stream.get_generator(path='/internal/h5/path'):
        do_something(row)

Examples
========

See the `unit tests <https://github.com/ghcollin/multitables/blob/master/multitables_test.py>`_ for complete examples.

Documentation
=============

`Online documentation <http://multitables.readthedocs.io/en/latest/>`_ is available.
A `how to <http://multitables.readthedocs.io/en/latest/howto.html>`_ gives a basic overview of the library.
A `benchmark <http://multitables.readthedocs.io/en/latest/benchmark.html>`_ tests the speed of the library using various
compression algorithms and hardware configurations.

Offline documentation can be built from the ``docs`` folder using ``sphinx``.