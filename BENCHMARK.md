These benchmarks have been performed with [multitables_benchmark.py](https://github.com/ghcollin/multitables/blob/master/multitables_benchmark.py). 
Two compression methods are benchmarked, along with three different storage devices.

Note that the data used are random numbers from a normal distribution, which is not compressible.
Thus, the numbers here reflect **only** the decompression overhead, and not the performance benefit that compression can give.
They are intended to give a rough idea on the number of default processes to use, as well as the possible performance benefit of using this library.

Your mileage may vary, with factors including the workload, compression ratio, specific storage configuration, system memory, and dataset size.
If your dataset fits wholly within the filesystem cache, your reads speeds will be significantly higher.

The following benchmarks use a 4GB file, reading two cycles, on a Haswell-E linux machine.

# Using blosc

## SATA III SSD:

### Direct

<table>
        <tr>
                <td>Complevel/n_proc</td> <td>1</td> <td>2</td> <td>3</td> <td>4</td> <td>5</td> <td>6</td> 
        </tr> <tr>
                <td>0</td> <td>272 MB/s</td> <td>471 MB/s</td> <td>499 MB/s</td> <td>521 MB/s</td> <td>520 MB/s</td> <td>523 MB/s</td> 
        </tr> <tr>
                <td>3</td> <td>259 MB/s</td> <td>470 MB/s</td> <td>499 MB/s</td> <td>521 MB/s</td> <td>523 MB/s</td> <td>523 MB/s</td> 
        </tr> <tr>
                <td>9</td> <td>211 MB/s</td> <td>440 MB/s</td> <td>479 MB/s</td> <td>508 MB/s</td> <td>530 MB/s</td> <td>532 MB/s</td> 
        </tr> 
</table>

### Generator

<table>
        <tr>
                <td>Complevel/n_proc</td> <td>1</td> <td>2</td> <td>3</td> <td>4</td> <td>5</td> <td>6</td> 
        </tr> <tr>
                <td>0</td> <td>291 MB/s</td> <td>471 MB/s</td> <td>498 MB/s</td> <td>520 MB/s</td> <td>520 MB/s</td> <td>521 MB/s</td> 
        </tr> <tr>
                <td>3</td> <td>289 MB/s</td> <td>470 MB/s</td> <td>498 MB/s</td> <td>520 MB/s</td> <td>522 MB/s</td> <td>523 MB/s</td> 
        </tr> <tr>
                <td>9</td> <td>217 MB/s</td> <td>443 MB/s</td> <td>495 MB/s</td> <td>505 MB/s</td> <td>528 MB/s</td> <td>529 MB/s</td> 
        </tr> 
</table>

## 2x SATA III SSD in raid0

### Direct

<table>
        <tr>
                <td>Complevel/n_proc</td> <td>1</td> <td>2</td> <td>3</td> <td>4</td> <td>5</td> <td>6</td> 
        </tr> <tr>
                <td>0</td> <td>407 MB/s</td> <td>825 MB/s</td> <td>911 MB/s</td> <td>961 MB/s</td> <td>1016 MB/s</td> <td>1023 MB/s</td> 
        </tr> <tr>
                <td>3</td> <td>376 MB/s</td> <td>829 MB/s</td> <td>911 MB/s</td> <td>967 MB/s</td> <td>1020 MB/s</td> <td>1025 MB/s</td> 
        </tr> <tr>
                <td>9</td> <td>302 MB/s</td> <td>591 MB/s</td> <td>878 MB/s</td> <td>926 MB/s</td> <td>954 MB/s</td> <td>1024 MB/s</td> 
        </tr> 
</table>

### Generator

<table>
        <tr>
                <td>Complevel/n_proc</td> <td>1</td> <td>2</td> <td>3</td> <td>4</td> <td>5</td> <td>6</td> 
        </tr> <tr>
                <td>0</td> <td>459 MB/s</td> <td>813 MB/s</td> <td>906 MB/s</td> <td>949 MB/s</td> <td>1014 MB/s</td> <td>1020 MB/s</td> 
        </tr> <tr>
                <td>3</td> <td>453 MB/s</td> <td>827 MB/s</td> <td>912 MB/s</td> <td>966 MB/s</td> <td>1016 MB/s</td> <td>1023 MB/s</td> 
        </tr> <tr>
                <td>9</td> <td>337 MB/s</td> <td>610 MB/s</td> <td>810 MB/s</td> <td>934 MB/s</td> <td>1001 MB/s</td> <td>1033 MB/s</td> 
        </tr> 
</table>

## SATA III 7200 RPM HDD

### Direct

<table>
        <tr>
                <td>Complevel/n_proc</td> <td>1</td> <td>2</td> <td>3</td> <td>4</td> <td>5</td> <td>6</td> 
        </tr> <tr>
                <td>0</td> <td>97 MB/s</td> <td>97 MB/s</td> <td>69 MB/s</td> <td>67 MB/s</td> <td>68 MB/s</td> <td>62 MB/s</td> 
        </tr> <tr>
                <td>3</td> <td>103 MB/s</td> <td>94 MB/s</td> <td>69 MB/s</td> <td>66 MB/s</td> <td>68 MB/s</td> <td>62 MB/s</td> 
        </tr> <tr>
                <td>9</td> <td>101 MB/s</td> <td>92 MB/s</td> <td>65 MB/s</td> <td>66 MB/s</td> <td>70 MB/s</td> <td>63 MB/s</td> 
        </tr> 
</table>


### Generator

<table>
        <tr>
                <td>Complevel/n_proc</td> <td>1</td> <td>2</td> <td>3</td> <td>4</td> <td>5</td> <td>6</td> 
        </tr> <tr>
                <td>0</td> <td>119 MB/s</td> <td>94 MB/s</td> <td>72 MB/s</td> <td>69 MB/s</td> <td>68 MB/s</td> <td>62 MB/s</td> 
        </tr> <tr>
                <td>3</td> <td>121 MB/s</td> <td>94 MB/s</td> <td>69 MB/s</td> <td>70 MB/s</td> <td>67 MB/s</td> <td>62 MB/s</td> 
        </tr> <tr>
                <td>9</td> <td>119 MB/s</td> <td>92 MB/s</td> <td>60 MB/s</td> <td>66 MB/s</td> <td>70 MB/s</td> <td>63 MB/s</td> 
        </tr> 
</table>

# With zlib 

## 2x SATA III SSD in raid0

### Direct

<table>
        <tr>
                <td>Complevel/n_proc</td> <td>1</td> <td>2</td> <td>3</td> <td>4</td> <td>5</td> <td>6</td> 
        </tr> <tr>
                <td>0</td> <td>436 MB/s</td> <td>785 MB/s</td> <td>914 MB/s</td> <td>968 MB/s</td> <td>1012 MB/s</td> <td>1024 MB/s</td> 
        </tr> <tr>
                <td>3</td> <td>229 MB/s</td> <td>415 MB/s</td> <td>564 MB/s</td> <td>722 MB/s</td> <td>847 MB/s</td> <td>990 MB/s</td> 
        </tr> <tr>
                <td>9</td> <td>179 MB/s</td> <td>390 MB/s</td> <td>554 MB/s</td> <td>704 MB/s</td> <td>819 MB/s</td> <td>993 MB/s</td> 
        </tr> 
</table>

#### Generator

<table>
        <tr>
                <td>Complevel/n_proc</td> <td>1</td> <td>2</td> <td>3</td> <td>4</td> <td>5</td> <td>6</td> 
        </tr> <tr>
                <td>0</td> <td>470 MB/s</td> <td>783 MB/s</td> <td>903 MB/s</td> <td>962 MB/s</td> <td>1009 MB/s</td> <td>1015 MB/s</td> 
        </tr> <tr>
                <td>3</td> <td>175 MB/s</td> <td>390 MB/s</td> <td>588 MB/s</td> <td>740 MB/s</td> <td>901 MB/s</td> <td>973 MB/s</td> 
        </tr> <tr>
                <td>9</td> <td>184 MB/s</td> <td>388 MB/s</td> <td>551 MB/s</td> <td>737 MB/s</td> <td>890 MB/s</td> <td>1012 MB/s</td> 
        </tr> 
</table>

# Conclusion

Parallel reads **hurt** performance on HDDs. This is expected, as seek time is a major limiter in this case.

Parallel reads can give at least a 2x performance increase when using SSDs. Diminishing returns kick in above 4 processes. 

While high levels of compression can have a serious processing overhead on single processor reads, 
parallel reads can achieve parity with an uncompressed dataset. Thus, the compression ratio of the data will translate 
directly to increased read performance.

There is no appreciable difference between the direct, low level access and the generator access method.
The limiting factor in this synthetic test is the read speed.
This conclusion may not hold valid for higher read speeds and/or heavy workloads. 

# Running the benchmark
Running the benchmark requires HDF5 to be built with the `--enable-direct-vfd` configure option 
(and then a recompile of pytables), to enable bypassing of the filesystem cache.
If the direct driver is not available on your system, the driver may be turned off.
However, in this case alternative measures must be taken to avoid the filesystem cache 
(such as using an appropriately large benchmarking file).

Additionally the benchmark requires the `tqdm` python package.

The most accurate results for your use case can only be obtained by testing the library directly in your application.