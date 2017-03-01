# Copyright (C) 2016 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

import unittest
import numpy as np
import tempfile
import os
import shutil
import tables
import threading

import multitables

__author__ = "G. H. Collin"
__version__ = "1.1.0"


def lcm(a,b):
    import fractions
    return abs(a * b) // fractions.gcd(a, b) if a and b else 0

test_table_col_A_shape = (100,200)
test_table_col_B_shape = (7,49)


class TestTableRow(tables.IsDescription):
    col_A = tables.UInt8Col(shape=test_table_col_A_shape)
    col_B = tables.Float64Col(shape=test_table_col_B_shape)


def get_batches(array, size):
    return [ array[i:i+size] for i in range(0, len(array), size)]


def assert_items_equal(self, a, b, key):
    self.assertEqual(len(a), len(b))
    if key is not None:
        a_sorted, b_sorted = sorted(a, key=key), sorted(b, key=key)
    else:
        a_sorted, b_sorted = a, b
    for i in range(len(a)):
        self.assertTrue(np.all(a_sorted[i] == b_sorted[i]),
                        msg=str(i) + "/" + str(len(a)) + "): LHS: \n" + str(a_sorted[i]) + "\n RHS: \n" + str(b_sorted[i]))


class MultiTablesTest(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_filename = os.path.join(self.test_dir, 'test.h5')
        test_file = tables.open_file(self.test_filename, 'w')

        self.test_array = np.arange(100*1000).reshape((1000, 10, 10))
        self.test_array_path = '/test_array'
        array = test_file.create_array(test_file.root, self.test_array_path[1:], self.test_array)

        self.test_table_ary = np.array([ (
            np.random.randint(256, size=np.prod(test_table_col_A_shape)).reshape(test_table_col_A_shape),
            np.random.rand(*test_table_col_B_shape)) for _ in range(100) ],
                                       dtype=tables.dtype_from_descr(TestTableRow))
        self.test_table_path = '/test_table'
        table = test_file.create_table(test_file.root, self.test_table_path[1:], TestTableRow)
        table.append(self.test_table_ary)

        test_file.close()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_get_batches(self):
        array = np.arange(8)
        batches = get_batches(array, 3)
        assert_items_equal(self, batches,
                           [np.arange(3), np.arange(3, 6), np.arange(6, 8)],
                           key=lambda x: x[0])

    def test_generator(self):
        reader = multitables.Streamer(filename=self.test_filename)

        ary_gen = reader.get_generator(path=self.test_array_path)

        assert_items_equal(self,
                           list(ary_gen),
                           list(self.test_array),
                           key=lambda x: x[0, 0])

        ary_gen.close()

        table_gen = reader.get_generator(path=self.test_table_path)

        assert_items_equal(self,
                           list(table_gen),
                           list(self.test_table_ary),
                           key=lambda x: x['col_B'][0][0])

        table_gen.close()

    def test_ordered(self):
        reader = multitables.Streamer(filename=self.test_filename)

        ary_gen = reader.get_generator(path=self.test_array_path, ordered=True)

        assert_items_equal(self,
                           list(ary_gen),
                           list(self.test_array),
                           key=None)

        ary_gen.close()

        table_gen = reader.get_generator(path=self.test_table_path, ordered=True)

        assert_items_equal(self,
                           list(table_gen),
                           list(self.test_table_ary),
                           key=None)

        table_gen.close()

    def test_direct(self):
        block_size = None
        reader = multitables.Streamer(filename=self.test_filename)

        queue = reader.get_queue(path=self.test_array_path, block_size=block_size)

        result = []
        while True:
            guard = queue.get()
            if guard is multitables.QueueClosed:
                break
            else:
                with guard as batch:
                    result.append(batch.copy())
        result.append(reader.get_remainder(path=self.test_array_path, block_size=queue.block_size))
        assert_items_equal(self,
                           result,
                           get_batches(self.test_array, queue.block_size),
                           key=lambda x: x[0, 0, 0])
        queue.close()

        block_size = 16
        queue = reader.get_queue(path=self.test_array_path, block_size=block_size)

        result = []
        for guard in queue.iter():
            with guard as batch:
                result.append(batch.copy())
        result.append(reader.get_remainder(path=self.test_array_path, block_size=queue.block_size))
        assert_items_equal(self,
                           result,
                           get_batches(self.test_array, queue.block_size),
                           key=lambda x: x[0, 0, 0])
        queue.close()

    def test_cycle(self):
        block_size = 45
        num_cycles = lcm(block_size, len(self.test_array))//len(self.test_array)
        if num_cycles < 3:
            num_cycles = 4
        elif num_cycles == 3:
            num_cycles = 6
        reader = multitables.Streamer(filename=self.test_filename)

        ary = reader.get_generator(path=self.test_array_path, cyclic=True, block_size=block_size)

        result = []
        for i, row in enumerate(ary):
            if i >= num_cycles*len(self.test_array):
                #print("Terminating at " + str(row[0,0]))
                break
            #print(row[0, 0])
            result.append(row)

        assert_items_equal(self,
                           result,
                           list(self.test_array)*num_cycles,
                           key=lambda x: x[0, 0])
        #self.assertEqual(len(result), 4*len(self.test_array))
        ary.close()

    def test_cycle_ordered(self):
        block_size = 45
        num_cycles = lcm(block_size, len(self.test_array))//len(self.test_array)
        if num_cycles < 3:
            num_cycles = 4
        elif num_cycles == 3:
            num_cycles = 6
        reader = multitables.Streamer(filename=self.test_filename)

        ary = reader.get_generator(path=self.test_array_path, cyclic=True, block_size=block_size, ordered=True)

        result = []
        for i, row in enumerate(ary):
            if i >= num_cycles*len(self.test_array):
                print("Terminating at " + str(row[0,0]))
                break
            #print(row[0, 0])
            result.append(row)

        #print(np.bincount(np.array(result)[:,0,0]/100))

        assert_items_equal(self,
                           result,
                           list(self.test_array)*num_cycles,
                           key=None)

        ary.close()

    def test_threaded(self):
        block_size = len(self.test_array)//100
        reader = multitables.Streamer(filename=self.test_filename)

        queue = reader.get_queue(path=self.test_array_path, n_procs=4, block_size=block_size)

        lock = threading.Lock()
        result = []

        def read():
            while True:
                guard = queue.get()
                if guard is multitables.QueueClosed:
                    break
                with guard as batch:
                    batch_copy = batch.copy()
                with lock:
                    result.append(batch_copy)

        threads = []
        for i in range(100):
            threads.append(threading.Thread(target=read))

        for t in threads:
            t.start()

        last_batch = reader.get_remainder(path=self.test_array_path, block_size=queue.block_size)
        if 100*block_size == len(self.test_array):
            self.assertEqual(len(last_batch), 0)
        else:
            result.append(last_batch)

        for t in threads:
            t.join()

        assert_items_equal(self,
                           result,
                           get_batches(self.test_array, block_size),
                           key=lambda x: x[0, 0, 0])

        queue.close()

    def test_quickstart(self):
        do_something = lambda x: x
        stream = multitables.Streamer(filename=self.test_filename)
        for row in stream.get_generator(path=self.test_array_path):
            do_something(row)

    def test_howto(self):
        kw_args = {}
        stream = multitables.Streamer(filename=self.test_filename, **kw_args)
        do_something = lambda x: x
        do_something_else = lambda x: x

        queue = stream.get_queue(
            path=self.test_array_path,  # Path to dataset within the H5file.
            n_procs=4,  # Number of processes to launch for parallel reads. Defaults to 4.
            read_ahead=5,  # Size of internal buffer in no. of blocks. Defaults to 2*n_proc+1.
            cyclic=False,  # A cyclic reader wraps at the end of the dataset. Defaults to False.
            block_size=32,  # Size (along the outer dimension) of the blocks that will be read.
            # Defaults to a multiple of the dataset chunk size, or a 128KB block.
            # Should be left to the default or carefully chosen for chunked arrays,
            # else performance degradation can occur.
            ordered=False  # Force the stream to return blocks in on-disk order. Useful if two
            # datasets need to be read synchronously. This option may have a
            # performance penalty.
        )

        while True:
            guard = queue.get()  # Get the guard object, will block until data is ready.
            if guard is multitables.QueueClosed:
                break  # Terminate the loop once the dataset is finished.
            with guard as block:  # The guard returns the next block of data in the buffer.
                do_something(block)  # Perform actions on the data

        while True:
            guard = queue.get()  # Get the guard object, will block until data is ready.
            if guard is multitables.QueueClosed:
                break  # Terminate the loop once the dataset is finished.
            with guard as block:  # The guard returns the next block of data in the buffer.
                do_something(block)  # Perform actions on the data

        for guard in queue.iter():
            with guard as block:
                do_something(block)

        last_block = stream.get_remainder(self.test_array_path, queue.block_size)

        queue.close()

        queue = stream.get_queue(
            path=self.test_array_path,  # Path to dataset within the H5file.
            n_procs=2,  # Number of processes to launch for parallel reads. Defaults to 2.
            read_ahead=5,  # Size of internal buffer in no. of blocks. Defaults to 2*n_proc+1.
            cyclic=True,  # A cyclic reader wraps at the end of the dataset. Defaults to False.
        )

        while True:
            with queue.get() as block:
                do_something(block)
                break

        gen = stream.get_generator(self.test_array_path, n_procs=4, read_ahead=9, cyclic=False, block_size=32)

        for row in gen:
            do_something_else(row)

        gen.close()

if __name__ == '__main__':
    unittest.main()


