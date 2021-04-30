# Copyright (C) 2019 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

try:
    import collections.abc as collections_abc
except ImportError:
    import collections as collections_abc

from . import stage
from . import numpy_utils
from .dataset_ops import \
    ReadOpTable, ReadScalarOpTable, ReadOpArray, ReadScalarOpArray, \
    IndexOp, ColOp, CoordOp, SortOp, WhereOp, \
    _is_simple_slice, _is_coords

class DatasetBase(object):
    """ Base class type for proxying pytables datasets. """

    def __init__(self, reader, path, dtype, shape):
        self._reader = reader
        self._path = path

        self.dtype = dtype
        self.shape = shape

    def create_stage_natoms(self, size_natoms):
        """
        Create a stage that has a size in bytes of size_natoms * (atom size in bytes).
        :param size_natoms: The resulting stage will have
        """
        return stage.Stage(self.dtype.itemsize*size_natoms)


class Dataset(DatasetBase):
    """ Base class type for proxying pytables Arrays and Tables. """

    def _fill_shape(self, shape):
        """
        Given a desired partial shape, fill the rest of the shape dimensions with the dimensions of the dataset.

        :param shape: The partial shape, can be an integer or tuple.
        :return: The completed shape.
        """
        if not isinstance(shape, (collections_abc.Iterable, collections_abc.Sequence)):
            shape = [shape]
        shape = list(shape)
        if len(shape) < len(self.shape):
            shape = shape + [-1]*(len(self.shape) - len(shape))
        for i in range(len(shape)):
            if shape[i] is None or shape[i] == -1:
                shape[i] = self.shape[i]
        return shape

    def create_stage(self, shape):
        """
        Create a stage that can host requests with a size equal to the given shape.

        :param shape: A shape that specifies the size of the stage. It may be incomplete, where the remaining
            dimensions will be filled with the dimensions of this dataset.
        :return: A new stage with the requested size.
        """
        return stage.Stage(numpy_utils._calc_nbytes(self.dtype, self._fill_shape(shape)))

    def create_stage_pool(self, shape, num_stages):
        """
        Create a pool of stages. Each stage in the pool will be initialised with the given shape.

        :param shape: A shape that specifies the size of the stages in this pool. It may be incomplete, where
            the remaining dimensions will be filled with the dimensions of this dataset.
        :return: A stage pool, from which stages can be retrieved.
        """
        return stage.StagePool(self, shape, num_stages)

class TableDataset(Dataset):
    """ Proxy for dataset operations on pytables Tables. """

    def col(self, name):
        """
        Proxy a column retrieval operation. The interface for this method is equivalent to the pytables method
        of the same name.
        """
        return ColOp(self._path, name)

    def read(self, start=None, stop=None, step=None, field=None):
        """
        Proxy a read operation. The interface for this method is equivalent to the pytables method of the same
        name.
        """
        return ReadOpTable(self._path, field, start, stop, step)

    def read_coordinates(self, coords, field=None):
        """
        Proxy a coordinate read operation. The interface for this method is equivalent to the pytables method
        of the same name.
        """
        return CoordOp(self._path, field, coords)

    def read_sorted(self, sortby, checkCSI=False, field=None, start=None, stop=None, step=None):
        """
        Proxy a sorted read operation. The interface and requirements for this method are equivalent to the
        pytables method of the same name.
        """
        return SortOp(self._path, sortby, checkCSI, field, start, stop, step)

    def where(self, condition, condvars=None, start=None, stop=None, step=None):
        """
        Proxy a conditional selection operations. The interface for this method are equivalent to the pytables
        method of the same name.
        """
        if condvars is None:
            condvars = {}
        return WhereOp(self._path, condition, condvars, start, stop, step)

    def __getitem__(self, key):
        """
        Proxy an indexing operation on this dataset. The dataset indexing interface is equivalent to the pytables
        dataset indexing interface.
        """
        if key is Ellipsis:
            raise IndexError("Pytables does not support a single ellipsis index.")
        elif isinstance(key, str):
            return self.col(key)
        elif isinstance(key, int):
            return ReadScalarOpTable(self._path, None, key)
        elif isinstance(key, slice):
            return ReadOpTable(self._path, None, key.start, key.stop, key.step)
        elif _is_simple_slice(key):
            key = key[0]
            return ReadOpTable(self._path, None, key.start, key.stop, key.step)
        elif _is_coords(key):
            return CoordOp(self._path, None, key)
        return IndexOp(self._path, key)


class ArrayDataset(Dataset):

    def read(self, start=None, stop=None, step=None):
        """
        Proxy a read operation. The interface for this method is equivalent to the pytables method of the same
        name.
        """
        return ReadOpArray(self._path, None, start, stop, step)

    def __getitem__(self, key):
        """
        Proxy an indexing operation on this dataset. The dataset indexing interface is equivalent to the pytables
        dataset indexing interface.
        """
        if key is Ellipsis:
            raise IndexError("Pytables does not support a single ellipsis index.")
        elif isinstance(key, int):
            return ReadScalarOpArray(self._path, None, key)
        elif isinstance(key, slice):
            return ReadOpArray(self._path, None, key.start, key.stop, key.step)
        elif _is_simple_slice(key):
            key = key[0]
            return ReadOpArray(self._path, None, key.start, key.stop, key.step)
        return IndexOp(self._path, key)


class VLArrayDataset(DatasetBase):

    def read(self, start=None, stop=None, step=None):
        """
        Proxy a read operation. The interface for this method is equivalent to the pytables method of the same
        name.
        """
        return IndexOp(self._path, slice(start, stop, step))

    def __getitem__(self, key):
        """
        Proxy an indexing operation on this dataset. The dataset indexing interface is equivalent to the pytables
        dataset indexing interface.
        """
        if key is Ellipsis:
            raise IndexError("Pytables does not support a single ellipsis index.")
        return IndexOp(self._path, key)