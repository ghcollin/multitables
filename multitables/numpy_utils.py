# Copyright (C) 2019 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

import numpy as np

def _calc_nbytes(dtype, shape):
    """
    Calculate the number of bytes that a numpy array with a given data type and shape will occupy in memory.
    :param dtype: The numpy datatype.
    :param shape: The shape of the array.
    :return: The size in bytes.
    """
    N_elem = np.prod(shape, dtype=int)
    size_nbytes = dtype.itemsize * N_elem
    return size_nbytes

def _dtype_descr(dtype):
    """
    Get a string that describes a numpy datatype.
    :param dtype: The numpy datatype.
    :return: A string describing the type.
    """
    if dtype.kind == 'V':
        return dtype.descr
    else:
        return dtype.char