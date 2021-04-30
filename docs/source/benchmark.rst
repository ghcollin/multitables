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

NVMe SSD
------------

Direct queue
^^^^^^^^^^^^

.. csv-table:: pytables complevel (down) vs. number of parallel processes (across)
   :header-rows: 1

   "","1","2","3","4","5","6","7","8","9","10","11","12"
   "0","954 MB/s","1694 MB/s","2061 MB/s","2380 MB/s","2399 MB/s","2400 MB/s","2252 MB/s","2346 MB/s","2221 MB/s","2377 MB/s","2252 MB/s","2260 MB/s"
   "3","952 MB/s","1729 MB/s","2087 MB/s","2368 MB/s","2379 MB/s","2372 MB/s","2285 MB/s","2143 MB/s","2368 MB/s","2369 MB/s","2286 MB/s","2237 MB/s"
   "9","394 MB/s","777 MB/s","1143 MB/s","1509 MB/s","1748 MB/s","2107 MB/s","2088 MB/s","2217 MB/s","2221 MB/s","2323 MB/s","2339 MB/s","2314 MB/s"

Generator
^^^^^^^^^

.. csv-table:: pytables complevel (down) vs. number of parallel processes (across)
   :header-rows: 1

   "","1","2","3","4","5","6","7","8","9","10","11","12"
   "0","841 MB/s","1466 MB/s","1691 MB/s","1765 MB/s","1762 MB/s","1765 MB/s","1741 MB/s","1755 MB/s","1698 MB/s","1886 MB/s","1654 MB/s","1705 MB/s"
   "3","834 MB/s","1430 MB/s","1668 MB/s","1693 MB/s","1712 MB/s","1659 MB/s","1669 MB/s","1618 MB/s","1647 MB/s","1644 MB/s","1572 MB/s","1559 MB/s"
   "9","369 MB/s","737 MB/s","1073 MB/s","1393 MB/s","1573 MB/s","1578 MB/s","1669 MB/s","1587 MB/s","1634 MB/s","1524 MB/s","1478 MB/s","1588 MB/s"


2x SATA III SSD in raid1
------------------------

Direct queue
^^^^^^^^^^^^

.. csv-table:: pytables complevel (down) vs. number of parallel processes (across)
    :header-rows: 1

   "","1","2","3","4","5","6","7","8","9","10","11","12"
   "0","358 MB/s","706 MB/s","821 MB/s","914 MB/s","960 MB/s","984 MB/s","996 MB/s","1009 MB/s","1012 MB/s","1027 MB/s","1026 MB/s","1013 MB/s"
   "3","355 MB/s","686 MB/s","810 MB/s","915 MB/s","953 MB/s","985 MB/s","996 MB/s","1006 MB/s","1011 MB/s","1023 MB/s","1023 MB/s","1032 MB/s"
   "9","237 MB/s","477 MB/s","687 MB/s","847 MB/s","907 MB/s","957 MB/s","988 MB/s","1012 MB/s","1033 MB/s","1048 MB/s","1056 MB/s","1062 MB/s"


Generator
^^^^^^^^^

.. csv-table:: pytables complevel (down) vs. number of parallel processes (across)
    :header-rows: 1

   "Complevel/n_proc","1","2","3","4","5","6","7","8","9","10","11","12"
   "0","338 MB/s","661 MB/s","797 MB/s","906 MB/s","941 MB/s","974 MB/s","980 MB/s","970 MB/s","999 MB/s","998 MB/s","1001 MB/s","1003 MB/s"
   "3","338 MB/s","657 MB/s","796 MB/s","889 MB/s","938 MB/s","952 MB/s","977 MB/s","962 MB/s","981 MB/s","989 MB/s","982 MB/s","976 MB/s"
   "9","239 MB/s","473 MB/s","677 MB/s","822 MB/s","898 MB/s","942 MB/s","968 MB/s","985 MB/s","994 MB/s","995 MB/s","1004 MB/s","1002 MB/s"


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

NVMe SSD
------------------------

Direct queue
^^^^^^^^^^^^

.. csv-table:: pytables complevel (down) vs. number of parallel processes (across)
    :header-rows: 1

   "","1","2","3","4","5","6","7","8","9","10","11","12"
   "0","958 MB/s","1773 MB/s","2132 MB/s","2202 MB/s","2393 MB/s","2249 MB/s","2345 MB/s","2243 MB/s","2373 MB/s","2262 MB/s","2290 MB/s","2171 MB/s"
   "3","301 MB/s","597 MB/s","902 MB/s","1208 MB/s","1497 MB/s","1766 MB/s","1869 MB/s","2073 MB/s","2116 MB/s","2302 MB/s","2149 MB/s","2390 MB/s"
   "9","269 MB/s","524 MB/s","787 MB/s","1047 MB/s","1234 MB/s","1499 MB/s","1621 MB/s","1647 MB/s","1684 MB/s","1934 MB/s","2021 MB/s","1934 MB/s"


Generator
^^^^^^^^^

.. csv-table:: pytables complevel (down) vs. number of parallel processes (across)
    :header-rows: 1

   "","1","2","3","4","5","6","7","8","9","10","11","12"
   "0","830 MB/s","1444 MB/s","1629 MB/s","1706 MB/s","1599 MB/s","1721 MB/s","1746 MB/s","1761 MB/s","1740 MB/s","1773 MB/s","1872 MB/s","1689 MB/s"
   "3","297 MB/s","581 MB/s","869 MB/s","1153 MB/s","1412 MB/s","1590 MB/s","1575 MB/s","1653 MB/s","1623 MB/s","1655 MB/s","1644 MB/s","1546 MB/s"
   "9","258 MB/s","504 MB/s","766 MB/s","1004 MB/s","1192 MB/s","1402 MB/s","1486 MB/s","1478 MB/s","1517 MB/s","1601 MB/s","1542 MB/s","1554 MB/s"

Conclusion
==========

Parallel reads **hurt** performance on HDDs. This is expected, as seek time is a major limiter in this case.

Parallel reads can give at least a 2x performance increase when using SSDs. Diminishing returns kick in above 4 processes.

While high levels of compression can have a serious processing overhead on single processor reads,
parallel reads can achieve parity with an uncompressed dataset. Thus, the compression ratio of the data will translate
directly to increased read performance.

There is no appreciable difference between the direct, low level access and the generator access method and low read speeds.
The limiting factor in that regime is the read speed.
At high read speeds, a significant difference is observed; therefore, one should use the direct, low-level access method when high speed NVMe storage is available.

Running the benchmark
=====================

Running the benchmark requires HDF5 to be built with the ``--enable-direct-vfd`` configure option
(and then a recompile of pytables), to enable bypassing of the filesystem cache.
If the direct driver is not available on your system, the driver may be turned off.
However, in this case alternative measures must be taken to avoid the filesystem cache
(such as using an appropriately large benchmarking file).

Additionally the benchmark requires the ``tqdm`` python package.

The most accurate results for your use case can only be obtained by testing the library directly in your application.

