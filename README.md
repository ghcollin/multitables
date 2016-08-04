High performance concurrent reading of HDF5 files using pytables. 
This library uses multiprocessing and shared memory to safely perform parallel reads.

Blocks of data are streamed to the user. 
There is **_no guarantee_** on the ordering of the blocks.

Performance gains of at least 2x can be achieved when reading from an SSD. 

# Licence
This software is distributed under the MIT licence. 
See the [LICENSE.txt](https://github.com/ghcollin/multitables/blob/master/LICENSE.txt) file for details.

# Installation
```
pip install git+https://github.com/ghcollin/multitables.git
```
or download and run
```
python setup.py install
```

`multitables` depends on `tables` (the pytables package) and `numpy`.
The package is compatible with the latest versions of python 2 and 3.

# Quick start
```
stream = multitables.Streamer(filename='/path/to/h5/file)
for row in stream.get_generator(path='/internal/h5/path'):
    do_something(row)
```

# Examples
See the [unit tests](https://github.com/ghcollin/multitables/blob/master/multitables_test.py) for complete examples.

# How To
Documentation on how to use the library can be found in [HOWTO.md](https://github.com/ghcollin/multitables/blob/master/HOWTO.md).

# Benchmark
A benchmark suite is included. Results are shown in [BENCHMARK.md](https://github.com/ghcollin/multitables/blob/master/BENCHMARK.md).