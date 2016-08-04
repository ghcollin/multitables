# Copyright (C) 2016 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

import tempfile
import os
import numpy as np
import shutil
import time
import tqdm

import multitables

file_shape = (0, 2**10)
file_type = np.dtype('Float32')
file_type_size = 4

class BenchmarkFile:
    def __init__(self, filename, n_rows, complib, complevel):
        self.filename = filename
        self.n_rows = n_rows
        self.complib = complib
        self.complevel = complevel

    def __enter__(self):
        import tables
        if self.filename is None:
            self.filedir = tempfile.mkdtemp()
            self.filename = os.path.join(self.filedir, 'bench.h5')
        else:
            self.filedir = None

        h5_file = tables.open_file(self.filename, 'w')
        array_kw_args = {}
        if self.complevel > 0:
            array_kw_args['filters'] = tables.Filters(complib=self.complib, complevel=self.complevel)

        array_path = '/bench'
        #ary = h5_file.create_array(h5_file.root, array_path[1:],
        #                           np.arange(np.prod(file_shape), dtype=file_type).reshape(file_shape))
        ary = h5_file.create_earray(h5_file.root, array_path[1:], atom=tables.Atom.from_dtype(file_type),
                                    shape=file_shape, expectedrows=self.n_rows, **array_kw_args)
        for _ in range(0, self.n_rows, 2**10):
            ary.append(2**8*np.random.randn(2**10, *file_shape[1:]))
        print(ary.shape)

        h5_file.close()

        return self.filename, array_path

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.filedir is None:
            os.remove(self.filename)
        else:
            shutil.rmtree(self.filedir)


def bench_generator(filename, node_path, n_procs, read_iters, **kwargs):
    stream = multitables.Streamer(filename, **kwargs)
    gen = stream.get_generator(node_path, n_procs=n_procs, cyclic=True)

    start = time.time()
    for i, row in tqdm.tqdm(enumerate(gen), total=read_iters):
        if i >= read_iters:
            break
    end = time.time()

    return end - start


def bench_direct(filename, node_path, n_procs, read_iters, **kwargs):
    stream = multitables.Streamer(filename, **kwargs)
    q = stream.get_queue(node_path, n_procs=n_procs, cyclic=True)

    start = time.time()
    for _ in tqdm.tqdm(range(0, read_iters, q.block_size)):
        with q.get() as block:
            pass
    end = time.time()

    q.close()

    return end - start


def make_table(results, max_procs, read_size):
    table = [["Complevel/n_proc"]]
    for i in range(max_procs):
        table[0].append(str(i+1))
    for complevel in sorted(results.keys()):
        table_row = [str(complevel)]
        for i in range(max_procs):
            table_row.append(str(int(read_size/results[complevel][i]/2**20)) + " MB/s")
        table.append(table_row)
    return table


def make_html(table):
    html = "<table>\n\t"
    for table_row in table:
        html += "<tr>\n\t\t"
        for elem in table_row:
            html += "<td>" + elem + "</td> "
        html += "\n\t</tr> "
    html += "\n</table>"
    return html


def print_table(table):
    size_outer = len(table)
    size_inner = len(table[0])

    col_widths = []
    for j in range(size_inner):
        max_width = 0
        for i in range(size_outer):
            max_width = max(max_width, len(table[i][j]))
        col_widths.append(max_width)

    for i in range(size_outer):
        line = ""
        for j in range(size_inner):
            elem = table[i][j]
            line += " " + elem + " "*(col_widths[j] - len(elem)) + " "
            if j > 0:
                line += "|"
        print(line)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--size', type=int, default=1,
                        help="Size of benchmarking file in GB.")
    parser.add_argument('--cycles', type=int, default=2,
                        help="Number of times to cycle through the benchmark file.")
    parser.add_argument('--max_procs', type=int, default=6,
                        help="Maximum number of processes to test.")
    parser.add_argument('--complib', default='blosc',
                        help="Pytables compression library to use.")
    parser.add_argument('--file',
                        help="File name of benchmarking file to create.")
    parser.add_argument('--H5FD_DIRECT_OFF', action='store_true',
                        help="Turn off the HDF5 DIRECT file access driver."
                            + "WARNING: Benchmarking results may be incorrect.")
    args = parser.parse_args()

    n_cycles = args.cycles
    max_procs = args.max_procs

    file_size = int(args.size)*2**30
    n_rows = int(file_size/np.prod(file_shape[1:])/file_type_size)
    read_iters = n_cycles * n_rows
    read_size = n_cycles * file_size

    kwargs = {}
    if not args.H5FD_DIRECT_OFF:
        kwargs['DRIVER'] = 'H5FD_DIRECT'

    direct_results = {}
    generator_results = {}
    for complevel in [0, 3, 9]:
        print("Creating benchmark file with complevel " + str(complevel))
        with BenchmarkFile(args.file, n_rows=n_rows, complib=args.complib, complevel=complevel) as file_details:
            filename, node_path = file_details
            print("Benchmark file created at: " + filename)

            direct_times = []
            for i in range(max_procs):
                print("Running direct queue benchmark with n_procs=" + str(i+1))
                direct_times.append( bench_direct(filename, node_path, i+1, read_iters, **kwargs) )

            generator_times = []
            for i in range(max_procs):
                print("Running generator benchmark with n_procs=" + str(i+1))
                generator_times.append( bench_generator(filename, node_path, i+1, read_iters, **kwargs) )

        direct_results[complevel] = direct_times
        generator_results[complevel] = generator_times

    names_and_results = [ ("Direct queue", direct_results), ("Generator", generator_results)]
    names_and_tables = [ (name, make_table(results, max_procs, read_size)) for name,results in names_and_results ]

    for name, table in names_and_tables:
        print(name + ":")
        print(make_html(table))
    for name, table in names_and_tables:
        print(name + ":")
        print_table(table)


        #print("Results:")
        #print("n_procs\tGenerator \t(MB/s) \t\tDirect \t(MB/s)")
        #for i in range(max_procs):
        #    gen_speed = read_size/generator_times[i]/2**20
        #    dir_speed = read_size/direct_times[i]/2**20
        #    print("%i\t%f\t(%f)\t%f\t(%f)" % (i+1, generator_times[i], gen_speed, direct_times[i], dir_speed))

if __name__ == '__main__':
    main()