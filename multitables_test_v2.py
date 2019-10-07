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
__version__ = "2.0.0"

test_table_col_A_shape = (500,200)
test_table_col_B_shape = (7,49)


class TestTableRow(tables.IsDescription):
    col_A = tables.UInt8Col(shape=test_table_col_A_shape)
    col_B = tables.Float64Col(shape=test_table_col_B_shape)
    col_C = tables.Float32Col()


def assert_items_equal(self, a, b, key):
    self.assertEqual(len(a), len(b))
    if key is not None:
        a_sorted, b_sorted = sorted(a, key=key), sorted(b, key=key)
    else:
        a_sorted, b_sorted = a, b
    for i in range(len(a)):
        self.assertTrue(np.all(a_sorted[i] == b_sorted[i]),
                        msg=str(i) + "/" + str(len(a)) + "): LHS: \n" + str(a_sorted[i]) + "\n RHS: \n" + str(b_sorted[i]))

N_PROCS = 4

class MultiTablesTestV2(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_filename = os.path.join(self.test_dir, 'test.h5')
        test_file = tables.open_file(self.test_filename, 'w')

        self.test_array = np.arange(100*1000).reshape((1000, 10, 10))
        self.test_array_path = '/test_array'
        array = test_file.create_array(test_file.root, self.test_array_path[1:], self.test_array)

        self.test_table_ary = np.array([ (
                np.random.randint(256, size=np.prod(test_table_col_A_shape)).reshape(test_table_col_A_shape),
                np.random.rand(*test_table_col_B_shape),
                np.random.rand()
            ) for _ in range(1000) ],
            dtype=tables.dtype_from_descr(TestTableRow))
        self.test_table_path = '/test_table'
        table = test_file.create_table(test_file.root, self.test_table_path[1:], TestTableRow)
        table.append(self.test_table_ary)
        table.cols.col_C.create_csindex()

        self.test_byte_ary = np.random.randint(256, size=1000*1000)
        self.test_byte_ary_path = '/test_byte_array'
        byte_array = test_file.create_array(test_file.root, self.test_byte_ary_path[1:], self.test_byte_ary)

        test_file.close()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_random_access(self):
        reader = multitables.Reader(filename=self.test_filename, n_procs=N_PROCS)

        test_array = reader.get_dataset(path=self.test_array_path)
        array_stage = test_array.create_stage(2)

        array_idxs = np.random.permutation(np.arange(0, test_array.shape[0], 2))
        for idx in array_idxs:
            with reader.request(test_array[idx:idx+2, :, :], array_stage).get_unsafe() as data:
                np.testing.assert_array_equal(data, self.test_array[idx:idx+2, :, :])

        test_table = reader.get_dataset(path=self.test_table_path)
        table_stage = test_table.create_stage(2)

        table_idxs = np.random.permutation(np.arange(0, test_table.shape[0], 2))
        for idx in table_idxs:
            with reader.request(test_table[idx:idx+2], table_stage).get_unsafe() as data:
                np.testing.assert_array_equal(data, self.test_table_ary[idx:idx+2])

        data = None
        reader.close()

    def test_overlapping_access_ary(self):
        reader = multitables.Reader(filename=self.test_filename, n_procs=N_PROCS)

        test_array = reader.get_dataset(path=self.test_array_path)
        N_stages = 30#30
        array_stages = []
        for _ in range(N_stages):
            array_stages.append(test_array.create_stage(1))

        array_len = test_array.shape[0]
        #for _ in range(120):
        for start in range(0, array_len, N_stages):
            reqs = []
            for i, array_idx in enumerate(range(start, min(start + N_stages, array_len))):
                #print(array_idx)
                reqs.append((array_idx, reader.request(test_array[array_idx], array_stages[i])))
            for array_idx, req in reqs:
                #print(array_idx)
                np.testing.assert_array_equal(req.get(), self.test_array[array_idx])

        reader.close()

    def test_overlapping_access_tbl(self):
        reader = multitables.Reader(filename=self.test_filename, n_procs=N_PROCS)

        test_table = reader.get_dataset(path=self.test_table_path)
        N_stages = 10
        table_stages = []
        for _ in range(N_stages):
            table_stages.append(test_table.create_stage(1))

        table_len = test_table.shape[0]
        for start in range(0, table_len, N_stages):
            reqs = []
            for i, table_idx in enumerate(range(start, min(start + N_stages, table_len), 1)):
                reqs.append((table_idx, reader.request(test_table[table_idx:table_idx+1], table_stages[i])))
            for table_idx, req in reqs:
                np.testing.assert_array_equal(req.get(), self.test_table_ary[table_idx:table_idx+1])

        reader.close()

    def test_threading_access_tbl(self):
        reader = multitables.Reader(filename=self.test_filename, n_procs=N_PROCS)

        test_table = reader.get_dataset(path=self.test_table_path)
        N_stages = 10

        import collections
        table_stages = collections.deque()
        stages_cvar = threading.Condition()
        for _ in range(N_stages):
            table_stages.append(test_table.create_stage(1))

        reqs = collections.deque()
        reqs_cvar = threading.Condition()

        table_len = test_table.shape[0]
        def loader():
            for idx in range(table_len):
                with stages_cvar:
                    while len(table_stages) == 0:
                        stages_cvar.wait()
                    stage = table_stages.popleft()
                with reqs_cvar:
                    reqs.append( (idx, stage, reader.request(test_table[idx:idx+1], stage)) )
                    reqs_cvar.notify()

        loader_thread = threading.Thread(target=loader)
        loader_thread.start()

        for _ in range(table_len):
            with reqs_cvar:
                while len(reqs) == 0:
                    reqs_cvar.wait()
                idx, stage, req = reqs.popleft()
            np.testing.assert_array_equal(req.get(), self.test_table_ary[idx:idx+1])
            with stages_cvar:
                table_stages.append(stage)
                stages_cvar.notify()


        reader.close()

    def test_pool_tbl(self):
        reader = multitables.Reader(filename=self.test_filename, n_procs=N_PROCS)

        test_table = reader.get_dataset(path=self.test_table_path)
        N_stages = 10

        stage_pool = test_table.create_stage_pool(1, N_stages)

        reqs = multitables.RequestPool()

        table_len = test_table.shape[0]
        def loader():
            for idx in range(table_len):
                reqs.add(reader.request(test_table[idx:idx+1], stage_pool))

        loader_thread = threading.Thread(target=loader)
        loader_thread.start()

        for idx in range(table_len):
            np.testing.assert_array_equal(reqs.next().get(), self.test_table_ary[idx:idx+1])


        reader.close()

    def test_array_getslice(self):
        reader = multitables.Reader(filename=self.test_filename, n_procs=N_PROCS)

        test_array = reader.get_dataset(path=self.test_array_path)
        array_stage = test_array.create_stage(2)

        array_idxs = np.arange(0, test_array.shape[0], 2)
        for idx in array_idxs:
            #with reader.request(test_array[idx:idx+2], array_stage).get_direct() as data:
            #    np.testing.assert_array_equal(data, self.test_array[idx:idx+2])
            reader.request(test_array[idx:idx+2], array_stage).get_direct(lambda data: np.testing.assert_array_equal(data, self.test_array[idx:idx+2]))
        
        array_stage.close()

        reader.close()

    def test_indexing(self):
        reader = multitables.Reader(filename=self.test_filename, n_procs=N_PROCS)

        test_array = reader.get_dataset(path=self.test_array_path)
        array_stage = test_array.create_stage(10)
        test_table = reader.get_dataset(path=self.test_table_path)
        table_stage = test_table.create_stage(10)

        req = test_table['col_A'][30:35]
        self.assertIsInstance(req, multitables.dataset_ops.ReadOpTable)
        with reader.request(req, table_stage).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_table_ary['col_A'][30:35])

        req = test_table[30:35]['col_A']
        self.assertIsInstance(req, multitables.dataset_ops.ReadOpTable)
        self.assertEqual(req._col, 'col_A')
        with reader.request(req, table_stage).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_table_ary[30:35]['col_A'])

        req = test_table.col('col_A')[30:35]
        self.assertIsInstance(req, multitables.dataset_ops.ReadOpTable)
        with reader.request(req, table_stage).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_table_ary['col_A'][30:35])

        req = test_table.col('col_A')[30:35,...,:100]
        self.assertIsInstance(req, multitables.dataset_ops.ColOp)
        with reader.request(req, table_stage).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_table_ary['col_A'][30:35,...,:100])

        req = test_table['col_A'][30:35,np.arange(500) > 45]
        self.assertIsInstance(req, multitables.dataset_ops.ColOp)
        with reader.request(req, table_stage).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_table_ary[...]['col_A'][30:35,np.arange(500) > 45])

        req = test_table[:]['col_A'][30:35,np.arange(500) > 45]
        self.assertIsInstance(req, multitables.dataset_ops.ReadOpTable)
        with reader.request(req, table_stage).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_table_ary[...]['col_A'][30:35,np.arange(500) > 45])

        req = test_array[4:16:2, ...]
        self.assertIsInstance(req, multitables.dataset_ops.ReadOpArray)
        with reader.request(req, array_stage).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_array[4:16:2, ...])

        req = test_table['col_A'][[1,2,3]]
        self.assertIsInstance(req, multitables.dataset_ops.CoordOp)
        with reader.request(req, table_stage).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_table_ary['col_A'][[1,2,3]])

        req = test_table[[1,2,3]]['col_A']
        self.assertIsInstance(req, multitables.dataset_ops.CoordOp)
        self.assertEqual(req._col, 'col_A')
        with reader.request(req, table_stage).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_table_ary[[1,2,3]]['col_A'])

        req = test_table['col_A'][0]
        self.assertIsInstance(req, multitables.dataset_ops.ReadScalarOpTable)
        with reader.request(req, table_stage).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_table_ary['col_A'][0])

        req = test_table[0]['col_A']
        self.assertIsInstance(req, multitables.dataset_ops.ReadScalarOpTable)
        self.assertEqual(req._col, 'col_A')
        with reader.request(req, table_stage).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_table_ary[0]['col_A'])

        array_stage_big = test_array.create_stage(1000)

        req = test_array[..., :105]
        self.assertIsInstance(req, multitables.dataset_ops.IndexOp)
        with reader.request(req, array_stage_big).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_array[..., :105])

        req = test_array[:][:, self.test_array[0] > 5]
        self.assertIsInstance(req, multitables.dataset_ops.ReadOpArray)
        with reader.request(req, array_stage_big).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_array[:][:, self.test_array[0] > 5])

        req = test_array[self.test_array > 5]
        self.assertIsInstance(req, multitables.dataset_ops.IndexOp)
        with reader.request(req, array_stage_big).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_array[self.test_array > 5])

        req = test_array[[1,2,3], [1,2,3], [1,2,3]]
        self.assertIsInstance(req, multitables.dataset_ops.IndexOp)
        with reader.request(req, array_stage_big).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_array[[1,2,3], [1,2,3], [1,2,3]])

        req = test_array[:][np.array([1,2,3])[:, np.newaxis], [1,2,3], [1,2,3]]
        self.assertIsInstance(req, multitables.dataset_ops.ReadOpArray)
        with reader.request(req, array_stage_big).get_unsafe() as data:
            np.testing.assert_array_equal(data, self.test_array[:][np.array([1,2,3])[:, np.newaxis], [1,2,3], [1,2,3]])

        table_stage_big = test_table.create_stage(100)

        req = test_table.where('col_C > x', condvars={'x':0.1}, start=300, stop=400)
        self.assertIsInstance(req, multitables.dataset_ops.WhereOp)
        table_subset = self.test_table_ary[300:400]
        with reader.request(req, table_stage_big).get_unsafe() as data:
            np.testing.assert_array_equal(data['col_C'], table_subset[table_subset['col_C'] > 0.1]['col_C'])
            np.testing.assert_array_equal(data, table_subset[table_subset['col_C'] > 0.1])

        req = test_table.read_sorted('col_C', checkCSI=True, start=200, stop=300)
        self.assertIsInstance(req, multitables.dataset_ops.SortOp)
        table_subset = self.test_table_ary.copy()
        table_subset.sort(axis=0, order='col_C')
        table_subset = table_subset[200:300]
        with reader.request(req, table_stage_big).get_unsafe() as data:
            np.testing.assert_array_equal(data['col_C'], table_subset['col_C'])
            np.testing.assert_array_equal(data, table_subset)

        data = None
        reader.close()

    def test_bad_indexing(self):
        reader = multitables.Reader(filename=self.test_filename, n_procs=N_PROCS)

        test_array = reader.get_dataset(path=self.test_array_path)
        test_table = reader.get_dataset(path=self.test_table_path)

        with self.assertRaises(IndexError):
            test = test_array[...][:5]

        array_stage_big = test_array.create_stage(1000)

        # Pytables should complain about this
        with self.assertRaises(IndexError):
            with reader.request(test_array[:, self.test_array[0] > 5], array_stage_big).get_unsafe() as data:
                np.testing.assert_array_equal(data, self.test_array[:, self.test_array[0] > 5])
        
        # Pytables should complain about this
        with self.assertRaises(IndexError):
            with reader.request(test_array[:, [1,2,3], [1,2,3]], array_stage_big).get_unsafe() as data:
                np.testing.assert_array_equal(data, self.test_array[:, [1,2,3], [1,2,3]])

        # Pytables should complain about this
        with self.assertRaises(IndexError):
            with reader.request(test_array[np.array([1,2,3])[:, np.newaxis], [1,2,3], [1,2,3]], array_stage_big).get_unsafe() as data:
                np.testing.assert_array_equal(data, self.test_array[np.array([1,2,3])[:, np.newaxis], [1,2,3], [1,2,3]])

        data = None
        array_stage_big.close()
        reader.close()

    def test_stage_size(self):
        reader = multitables.Reader(filename=self.test_filename, n_procs=N_PROCS)

        test_array = reader.get_dataset(path=self.test_array_path)
        array_stage = test_array.create_stage(10)
        test_table = reader.get_dataset(path=self.test_table_path)
        table_stage = test_table.create_stage(10)

        with self.assertRaises(multitables.SharedMemoryError):
            test = reader.request(test_array[:], array_stage).get()

        with self.assertRaises(multitables.SharedMemoryError):
            test = reader.request(test_table[:], table_stage).get()

        reader.close()

    def test_large_access(self):
        reader = multitables.Reader(filename=self.test_filename, n_procs=N_PROCS)

        test_byte_array = reader.get_dataset(path=self.test_byte_ary_path)
        array_stage = test_byte_array.create_stage(1000*1000)

        for i in range(20):
            test = reader.request(test_byte_array[np.arange(1000*1000)], array_stage).get()
            np.testing.assert_array_equal(test, self.test_byte_ary)

            test = reader.request(test_byte_array[:10], array_stage).get()
            np.testing.assert_array_equal(test, self.test_byte_ary[:10])

        array_stage.close()
        reader.close()

    def test_del(self):
        reader = multitables.Reader(filename=self.test_filename, n_procs=N_PROCS)

        test_array = reader.get_dataset(path=self.test_array_path)
        array_stage = test_array.create_stage(10)

        test = reader.request(test_array[:10], array_stage).get()


if __name__ == '__main__':
    unittest.main()

    