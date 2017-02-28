.. multitables documentation master file, created by
   sphinx-quickstart on Thu Aug 18 17:42:35 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

multitables documentation
*************************

`multitables <https://github.com/ghcollin/multitables>`_ is a python library designed for high speed access to HDF5 files.
Access to HDF5 is provided by the PyTables library (``tables``).
Multiple processes are launched to read a HDF5 in parallel, allowing concurrent decompression.
Data is streamed back to the invoker by use of shared memory space, removing the usual multiprocessing
communication overhead.

The data is organised by rows of an array (elements of the outer-most dimension), and groups of these rows form blocks.
By default, there is **no guarantee** on the ordering of the rows and/or blocks returned to the user, due to the
concurrent nature of the library. They are returned as they become available. On-disk ordering can be forced using
the ``ordered`` option, which may result in a performance penalty.

Performance gains of at least 2x can be achieved when reading from an SSD.

Contents
========

.. toctree::
   :maxdepth: 2

   quick
   howto
   benchmark
   reference

Licence
=======

This software is distributed under the MIT licence. See the
`LICENSE.txt <https://github.com/ghcollin/multitables/blob/master/LICENSE.txt>`_ file for details.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

