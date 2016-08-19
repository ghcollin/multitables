Quick Start
***********

Installation
============

::

    pip install git+https://github.com/ghcollin/multitables.git

Alternatively, `download <https://github.com/ghcollin/multitables/archive/master.zip>`_
or `clone the repository <https://github.com/ghcollin/multitables>`_ and run

::

    python setup.py install

``multitables`` depends on ``tables`` (the pytables package) and
``numpy``. The package is compatible with the latest versions of python
2 and 3.

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