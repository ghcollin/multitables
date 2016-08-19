.. |nbsp| unicode:: 0xA0
   :trim:

Benchmarking
************

These benchmarks have been performed with `multitables_benchmark.py <https://github.com/ghcollin/multitables/blob/master/multitables_benchmark.py>`_.
Two compression methods are benchmarked, along with three different storage devices.

Note that the data used are random numbers from a normal distribution, which is not compressible.
Thus, the numbers here reflect **only** the decompression overhead, and not the performance benefit that compression can give.
They are intended to give a rough idea on the number of default processes to use, as well as the possible performance benefit of using this library.

Your mileage may vary, with factors including the workload, compression ratio, specific storage configuration, system memory, and dataset size.
If your dataset fits wholly within the filesystem cache, your reads speeds will be significantly higher.

The following benchmarks use a 4GB file, reading two cycles, on a Haswell-E linux machine.

Using blosc
===========

SATA III SSD
------------

Direct queue
^^^^^^^^^^^^

.. csv-table:: pytables complevel (down) vs. number of parallel processes (across)
   :header-rows: 1

   "","1","2","3","4","5","6"
   "0","272 MB/s","471 MB/s","499 MB/s","521 MB/s","520 MB/s","523 MB/s"
   "3","259 MB/s","470 MB/s","499 MB/s","521 MB/s","523 MB/s","523 MB/s"
   "9","211 MB/s","440 MB/s","479 MB/s","508 MB/s","530 MB/s","532 MB/s"

Generator
^^^^^^^^^

.. csv-table:: pytables complevel (down) vs. number of parallel processes (across)
   :header-rows: 1

   "","1","2","3","4","5","6"
   "0","291 MB/s","471 MB/s","498 MB/s","520 MB/s","520 MB/s","521 MB/s"
   "3","289 MB/s","470 MB/s","498 MB/s","520 MB/s","522 MB/s","523 MB/s"
   "9","217 MB/s","443 MB/s","495 MB/s","505 MB/s","528 MB/s","529 MB/s"


2x SATA III SSD in raid0
------------------------

Direct queue
^^^^^^^^^^^^

.. csv-table:: pytables complevel (down) vs. number of parallel processes (across)
   :header-rows: 1

   "","1","2","3","4","5","6"
   "0","407 MB/s","825 MB/s","911 MB/s","961 MB/s","1016 MB/s","1023 MB/s"
   "3","376 MB/s","829 MB/s","911 MB/s","967 MB/s","1020 MB/s","1025 MB/s"
   "9","302 MB/s","591 MB/s","878 MB/s","926 MB/s","954 MB/s","1024 MB/s"


Generator
^^^^^^^^^

.. csv-table:: pytables complevel (down) vs. number of parallel processes (across)
   :header-rows: 1

   "","1","2","3","4","5","6"
   "0","459 MB/s","813 MB/s","906 MB/s","949 MB/s","1014 MB/s","1020 MB/s"
   "3","453 MB/s","827 MB/s","912 MB/s","966 MB/s","1016 MB/s","1023 MB/s"
   "9","337 MB/s","610 MB/s","810 MB/s","934 MB/s","1001 MB/s","1033 MB/s"


SATA III 7200 RPM HDD
---------------------

Direct queue
^^^^^^^^^^^^

.. csv-table:: pytables complevel (down) vs. number of parallel processes (across)
   :header-rows: 1

   "","1","2","3","4","5","6"
   "0","97 MB/s","97 MB/s","69 MB/s","67 MB/s","68 MB/s","62 MB/s"
   "3","103 MB/s","94 MB/s","69 MB/s","66 MB/s","68 MB/s","62 MB/s"
   "9","101 MB/s","92 MB/s","65 MB/s","66 MB/s","70 MB/s","63 MB/s"



Generator
^^^^^^^^^

.. csv-table:: pytables complevel (down) vs. number of parallel processes (across)
   :header-rows: 1

   "","1","2","3","4","5","6"
   "0","119 MB/s","94 MB/s","72 MB/s","69 MB/s","68 MB/s","62 MB/s"
   "3","121 MB/s","94 MB/s","69 MB/s","70 MB/s","67 MB/s","62 MB/s"
   "9","119 MB/s","92 MB/s","60 MB/s","66 MB/s","70 MB/s","63 MB/s"


Using zlib 
==========

2x SATA III SSD in raid0
------------------------

Direct queue
^^^^^^^^^^^^

.. csv-table:: pytables complevel (down) vs. number of parallel processes (across)
   :header-rows: 1

   "","1","2","3","4","5","6"
   "0","436 MB/s","785 MB/s","914 MB/s","968 MB/s","1012 MB/s","1024 MB/s"
   "3","229 MB/s","415 MB/s","564 MB/s","722 MB/s","847 MB/s","990 MB/s"
   "9","179 MB/s","390 MB/s","554 MB/s","704 MB/s","819 MB/s","993 MB/s"


Generator
^^^^^^^^^

.. csv-table:: pytables complevel (down) vs. number of parallel processes (across)
   :header-rows: 1

   "","1","2","3","4","5","6"
   "0","470 MB/s","783 MB/s","903 MB/s","962 MB/s","1009 MB/s","1015 MB/s"
   "3","175 MB/s","390 MB/s","588 MB/s","740 MB/s","901 MB/s","973 MB/s"
   "9","184 MB/s","388 MB/s","551 MB/s","737 MB/s","890 MB/s","1012 MB/s"

Conclusion
==========

Parallel reads **hurt** performance on HDDs. This is expected, as seek time is a major limiter in this case.

Parallel reads can give at least a 2x performance increase when using SSDs. Diminishing returns kick in above 4 processes.

While high levels of compression can have a serious processing overhead on single processor reads,
parallel reads can achieve parity with an uncompressed dataset. Thus, the compression ratio of the data will translate
directly to increased read performance.

There is no appreciable difference between the direct, low level access and the generator access method.
The limiting factor in this synthetic test is the read speed.
This conclusion may not hold valid for higher read speeds and/or heavy workloads.

Running the benchmark
=====================

Running the benchmark requires HDF5 to be built with the ``--enable-direct-vfd`` configure option
(and then a recompile of pytables), to enable bypassing of the filesystem cache.
If the direct driver is not available on your system, the driver may be turned off.
However, in this case alternative measures must be taken to avoid the filesystem cache
(such as using an appropriately large benchmarking file).

Additionally the benchmark requires the ``tqdm`` python package.

The most accurate results for your use case can only be obtained by testing the library directly in your application.

