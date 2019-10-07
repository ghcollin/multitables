# Copyright (C) 2019 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

import numpy as np

from . import msgpack_ext
from .msgpack_ext import msgpack_registry

import sys
_PYTHON3 = sys.version_info > (3, 0)

if _PYTHON3:
    genrange = range
else:
    genrange = xrange

def _predict_idx_shape_col(dtype, shape, name):
    """
    A helper method for predicting the data type and shape of a column indexing operation in pytables.
    :param dtype: The numpy datatype of the array or table.
    :param shape: The shape of the array or table.
    :param name: The name of the requested column.
    :return: A tuple, the first element is the numpy datatype of the requested column, the second element
        is the shape for the result of the column indexing operation.
    """
    if name not in dtype.fields:
        raise NameError("Specified column name '" + name + "' not in dataset.")
    coldtype = dtype[name]
    subdtype = coldtype.subdtype
    if subdtype is None:
        colshape = ()
    else:
        coldtype, colshape = subdtype
    new_shape = list(shape) + list(colshape)
    return coldtype, tuple(new_shape)

def _predict_idx_shape_slice(shape, slice):
    """
    A helper function for predicting the shape of an array after performing a slice operation.
    :param shape: The shape of the array.
    :param slice: The slice operation that will be performed on the array.
    :return: The shape of the resulting slice operation.
    """
    return ( len(genrange(*slice.indices(shape[0]))), ) + shape[1:]

def _is_simple_slice(key):
    """
    A helper function for determining if an operation is a simple slice. A simple slice is an indexing operation
    where the first coordinate is a slice, and all other coordinates are either not specified, are all index
    slices (":"), or are ellipses.
    :param key: The indexing operation.
    :return: True if the indexing operation is a simple slice.
    """
    return isinstance(key, tuple) and len(key) > 0 and isinstance(key[0], slice) and np.all([ (k is slice and k == np.s_[:]) or k is Ellipsis for k in key[1:]])

def _is_coords(key):
    """
    A helper function for determining if an operation is a coordinate a.k.a. point selection.
    :param key: The indexing operation.
    :return: True if the indexing operation is a point selection.
    """
    return isinstance(key, list) or (isinstance(key, np.ndarray) and np.issubdtype(key.dtype, np.integer))

class OpBase(msgpack_ext.MsgpackExtType):
    """ Base class type for indexing operations. Defines basic numpy style indexing. """

    def __init__(self, path):
        self._path = path
        self._index = None

    def _apply_index(self, ary):
        """
        Apply the indexing operations stored into this operation on the provided array and return the result.
        :param ary: The array on which to perform the indexing operations.
        :return: The result of the indexing operations.
        """
        if self._index is not None:
            for key in self._index:
                if hasattr(ary, 'size_on_disk') and isinstance(key, np.ndarray) and not key.flags.writeable:
                    # pytables can write to the key sometimes, so create a writable key
                    key = np.array(key, copy=True)
                ary = ary[key]
        return ary

    def __getitem__(self, key):
        """
        Upon using square bracket style indexing notation, store the requested index for use later.
        """
        if self._index is None:
            self._index = [key]
        else:
            self._index.append(key)
        return self

    _pack_map = msgpack_ext.MsgpackExtType._pack_map + ['_path', '_index']


class IndexOp(OpBase):
    """ Class for proxying basic numpy style indexing operations. """

    def __init__(self, path, key):
        """
        Create an instance that has the provided key as its first indexing operation.
        :param path: The path to the requested dataset.
        :param key: The indexing operation requested on the dataset.
        """
        super(IndexOp, self).__init__(path)
        self.__getitem__(key)

    def _apply(self, node, out_buf):
        """
        Apply this operation to the provided pytables node and store the result in the provided shared buffer.
        :param node: A pytables node that this indexing operation should be applied to.
        :param out_buf: The shared memory buffer where the result should be stored.
        :return: A tuple, the first element is the datatype of the operation result, the second element is the
            shape of the operation result.
        """
        result = self._apply_index(node)
        out_buf.set_to(result)
        return result.dtype, result.shape

    _pack_map = OpBase._pack_map
msgpack_registry.register_class(IndexOp)

class ColOp(OpBase):
    """ Class for proxing pytables column indexing. """

    def __init__(self, path, col):
        super(ColOp, self).__init__(path)
        self._col = col

    def _apply(self, node, out_buf):
        result = self._apply_index(node.col(self._col))
        out_buf.set_to(result)
        return result.dtype, result.shape

    def __getitem__(self, key):
        # Depending on the next index operation, various optimisations can be performed.
        if isinstance(key, int):
            # A column index, followed by a scalar index can be optimised to a read operation.
            return ReadScalarOpTable(self._path, self._col, key)
        elif isinstance(key, slice):
            # A column index, followed by a slice index, can be optimised to a read operation.
            return ReadOpTable(self._path, self._col, key.start, key.stop, key.step)
        elif _is_simple_slice(key):
            # As above.
            key = key[0]
            return ReadOpTable(self._path, self._col, key.start, key.stop, key.step)
        elif _is_coords(key):
            # A column index, followed by point selection, can be optimised to point selection.
            return CoordOp(self._path, self._col, key)
        else:
            return super(ColOp, self).__getitem__(key)

    _pack_map = OpBase._pack_map + ['_col']
msgpack_registry.register_class(ColOp)

class ReadOpBase(OpBase):
    """ A class for proxying pytables slice indexing. """

    def __init__(self, path, col, start, stop, step):
        super(ReadOpBase, self).__init__(path)
        self._col = col
        self._start = start
        self._stop = stop
        self._step = step

    def _dtype_shape(self, dtype, shape):
        """
        Helper method for calcuating the resultant datatype and shape of this indexing operation.
        """
        if self._col is not None:
            dtype, shape = _predict_idx_shape_col(dtype, shape, self._col)
        shape = _predict_idx_shape_slice(shape, slice(self._start, self._stop, self._step))
        return dtype, shape

    def _read(self, node):
        """
        Read the supplied node, applying this operation and any subsequent indexing operations to it.
        :param node: The pytables node to apply this indexing operation to.
        :return: The result of the indexing operation.
        """
        raise NotImplementedError("Abstract method.")

    def _read_to(self, node, out):
        """
        Read the supplied node, applying this operation and writing the result into the out parameter.
        :param node: The pytables node to apply this indexing operation to.
        :param out: The buffer into which the result should be written.
        """
        raise NotImplementedError("Abstract method.")

    def _apply_with_index(self, node, out_buf):
        """
        Apply this operation when there are subsequent indexing operations.
        """
        result = self._read(node)
        out_buf.set_to(result)
        return result.dtype, result.shape

    def _apply_without_index(self, node, out_buf):
        """
        Apply this operation when there are no subsequent indexing operations. This allows the optimisation
        of reading the pytables data directly into the shared buffer.
        """
        dtype, shape = node.dtype, node.shape
        dtype, shape = self._dtype_shape(dtype, shape)
        with out_buf.asarray_direct(dtype, shape) as ary:
            self._read_to(node, ary)
        return dtype, shape

    def _apply(self, node, out_buf):
        if self._index is None:
            return self._apply_without_index(node, out_buf)
        else:
            return self._apply_with_index(node, out_buf)
            

    _pack_map = OpBase._pack_map + ['_col', '_start', '_stop', '_step']

class ReadOpTable(ReadOpBase):
    """ Class for proxying pytables slice indexing to a Table node. """

    def _read_to(self, node, out):
        node.read(start=self._start, stop=self._stop, step=self._step, field=self._col, out=out)

    def _read(self, node):
        return self._apply_index(node.read(start=self._start, stop=self._stop, step=self._step, field=self._col))

    def __getitem__(self, key):
        if isinstance(key, str) and self._col is None:
            return ReadOpTable(self._path, key, self._start, self._stop, self._step)
        else:
            return super(ReadOpTable, self).__getitem__(key)

    _pack_map = ReadOpBase._pack_map
msgpack_registry.register_class(ReadOpTable)

class ReadOpArray(ReadOpBase):
    """ Class for proxying pytables slice indexing to an Array node. """

    def _read_to(self, node, out):
        node.read(start=self._start, stop=self._stop, step=self._step, out=out)

    def _read(self, node):
        return self._apply_index(node.read(start=self._start, stop=self._stop, step=self._step))

    _pack_map = ReadOpBase._pack_map
msgpack_registry.register_class(ReadOpArray)
    
class ReadScalarOpBase(ReadOpBase):
    """ Class for proxying scalar indexing. """

    def __init__(self, path, col, idx):
        super(ReadScalarOpBase, self).__init__(path, col, idx, idx+1, None)

    def _apply_without_index(self, node, out_buf):
        # When indexing a scalar element, a slice can be constructed and written directly into the buffer.
        dtype, shape = node.dtype, node.shape
        dtype, shape = self._dtype_shape(dtype, shape)
        with out_buf.asarray_direct(dtype, shape) as ary:
            self._read_to(node, ary)
        # Then the result shape can truncate out the first dimension, resulting in a scalar.
        return dtype, shape[1:]

    _pack_map = ReadOpBase._pack_map

class ReadScalarOpTable(ReadScalarOpBase, ReadOpTable):
    """ Class for proxying scalar indexing of Table nodes. """

    def _read(self, node):
        return self._apply_index(node.read(start=self._start, stop=self._stop, step=self._step, field=self._col)[0])

    def __getitem__(self, key):
        if isinstance(key, str) and self._col is None:
            return ReadScalarOpTable(self._path, key, self._start)
        else:
            return super(ReadScalarOpTable, self).__getitem__(key)

    _pack_map = ReadScalarOpBase._pack_map
msgpack_registry.register_class(ReadScalarOpTable)

class ReadScalarOpArray(ReadScalarOpBase, ReadOpArray):
    """ Class for proxying scalar indexing of Array nodes. """
        
    def _read(self, node):
        return self._apply_index(node.read(start=self._start, stop=self._stop, step=self._step)[0])

    _pack_map = ReadScalarOpBase._pack_map
msgpack_registry.register_class(ReadScalarOpArray)

class JoinedSlicesOp(OpBase):
    """ Class for proxying the operation of reading two slices, and then concatentating them. """

    def __init__(self, path, col, start1, stop1, step1, start2, stop2, step2):
        super(JoinedSlicesOp, self).__init__(path)
        self._col = col
        self._start1 = start1
        self._stop1 = stop1
        self._step1 = step1
        self._start2 = start2
        self._stop2 = stop2
        self._step2 = step2

    def _dtype_shape(self, dtype, shape):
        if self._col is not None:
            dtype, shape = _predict_idx_shape_col(dtype, shape, self._col)
        shape1 = _predict_idx_shape_slice(shape, slice(self._start1, self._stop1, self._step1))
        shape2 = _predict_idx_shape_slice(shape, slice(self._start2, self._stop2, self._step2))
        shape = (shape1[0] + shape2[0],) + shape1[1:]
        return dtype, shape

    def _read_to(self, node, out):
        out_stop1 = (self._stop1 - self._start1)
        if self._col is None:
            node.read(start=self._start1, stop=self._stop1, step=self._step1, out=out[:out_stop1])
            node.read(start=self._start2, stop=self._stop2, step=self._step2, out=out[out_stop1:])
        else:
            node.read(start=self._start1, stop=self._stop1, step=self._step1, field=self._col, out=out[:out_stop1])
            node.read(start=self._start2, stop=self._stop2, step=self._step2, field=self._col, out=out[out_stop1:])

    def _read(self, node):
        if self._col is None:
            slice1 = node.read(start=self._start1, stop=self._stop1, step=self._step1)
            slice2 = node.read(start=self._start2, stop=self._stop2, step=self._step2)
        else:
            slice1 = node.read(start=self._start1, stop=self._stop1, step=self._step1, field=self._col)
            slice2 = node.read(start=self._start2, stop=self._stop2, step=self._step2, field=self._col)
        return self._apply_index(np.concatenate((slice1, slice2), axis=0))

    def _apply(self, node, out_buf):
        if self._index is None:
            dtype, shape = node.dtype, node.shape
            dtype, shape = self._dtype_shape(dtype, shape)
            with out_buf.asarray_direct(dtype, shape) as ary:
                self._read_to(node, ary)
            return dtype, shape
        else:
            result = self._read(node)
            out_buf.set_to(result)
            return result.dtype, result.shape

    _pack_map = OpBase._pack_map + ['_col', '_start1', '_stop1', '_step1', '_start2', '_stop2', '_step2']
msgpack_registry.register_class(JoinedSlicesOp)

class CoordOp(OpBase):
    """ Class for proxying coordinate, a.k.a point selection operations. """
    def __init__(self, path, col, coords):
        super(CoordOp, self).__init__(path)
        self._col = col
        self._coords = coords

    def _apply(self, node, out_buf):
        result = self._apply_index(node.read_coordinates(self._coords, field=self._col))
        out_buf.set_to(result)
        return result.dtype, result.shape
    
    def __getitem__(self, key):
        if isinstance(key, str) and self._col is None:
            return CoordOp(self._path, key, self._coords)
        else:
            return super(CoordOp, self).__getitem__(key)

    _pack_map = OpBase._pack_map + ['_col', '_coords']
msgpack_registry.register_class(CoordOp)

class SortOp(OpBase):
    """ Class for proxying pytables sorted reads. """

    def __init__(self, path, sortby, checkCSI, col, start, stop, step):
        super(SortOp, self).__init__(path)
        self._sortby = sortby
        self._checkCSI = checkCSI
        self._col = col
        self._start = start
        self._stop = stop
        self._step = step

    def _apply(self, node, out_buf):
        result = self._apply_index(node.read_sorted(
            sortby = self._sortby, 
            checkCSI = self._checkCSI, 
            field = self._col,
            start = self._start,
            stop = self._stop,
            step = self._step))
        out_buf.set_to(result)
        return result.dtype, result.shape

    def __getitem__(self, key):
        if isinstance(key, str) and self._col is None:
            return SortOp(self._path, self._sortby, self._checkCSI, key, self._start, self._stop, self._step)
        else:
            return super(SortOp, self).__getitem__(key)

    _pack_map = OpBase._pack_map + ['_sortby', '_checkCSI', '_col', '_start', '_stop', '_step']
msgpack_registry.register_class(SortOp)

class WhereOp(OpBase):
    """ Class for proxying pytables conditional indexing. """

    def __init__(self, path, condition, condvars, start, stop, step):
        super(WhereOp, self).__init__(path)
        self._condition = condition
        self._condvars = condvars
        self._start = start
        self._stop = stop
        self._step = step

    def _apply(self, node, out_buf):
        result = self._apply_index(node.read_where(
            condition = self._condition,
            condvars = self._condvars,
            start = self._start,
            stop = self._stop,
            step = self._step
        ))
        out_buf.set_to(result)
        return result.dtype, result.shape

    _pack_map = OpBase._pack_map + ['_condition', '_condvars', '_start', '_stop', '_step']
msgpack_registry.register_class(WhereOp)