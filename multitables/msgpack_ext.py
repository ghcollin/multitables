# Copyright (C) 2019 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

import msgpack
import numpy as np

from . import numpy_utils

class MsgpackCustomExt:
    """ A custom extension type manager for msgpack """

    def __init__(self):
        self._packers = {}
        self._unpackers = {}
        self._super_types = []
    
    def register_class(self, class_type):
        """
        Register an object type that inherits from the MsgpackExtType class type. The packer and unpacker will be
        derived from the class.
        :param class_type: The type of the class that inherits from MsgpackExtType
        """
        self.register_obj(class_type, class_type.__msgpack__, class_type.__msgunpack__)

    def register_obj(self, obj_type, packer, unpacker):
        """
        Register an object type, creating a msgpack extension type to contain its description.

        :param obj_type: The python object type to register.
        :param packer: A function that takes an instance of the object type and returns a description of it.
        :param unpacker: A function that takes a description of an instance of the object type, and returns a new
            instance that matches the description.
        """
        if obj_type in self._packers:
            raise RuntimeError("This type has already been registered.")
        tid = len(self._packers)
        self._packers[obj_type] = (tid, packer)
        self._unpackers[tid] = unpacker

    def register_supertype(self, super_type, packer, unpacker):
        """
        Register a type such that any sub-type of the given type will use the provided packer and unpacker.
        :param super_type: The given super-type.
        :param packer: A function that takes an instance of the object type and returns a description of it.
        :param unpacker: A function that takes a description of an instance of the object type, and returns a new
            instance of the sub-type that matches the description.
        """
        self.register_obj(super_type, packer, unpacker)
        self._super_types.append(super_type)

    def pack(self, obj):
        """
        Pack an object using the extension types registered with this manager. Complex extension types will be
        recursively packed.
        :param obj: The object to pack.
        :return: A string of bytes that describes the object.
        """
        return msgpack.packb(obj, default=self._pack_obj, use_bin_type=True, strict_types=True)

    def unpack(self, data):
        """
        Unpack a string of bytes that uses the extension types registered with this manager. Complex extension
        types will be recursively unpacked.
        :param data: The string of bytes to unpack.
        :return: The object described by the data.
        """
        return msgpack.unpackb(data, ext_hook=self._unpack_obj, raw=False, use_list=True, strict_map_key=False)

    def _pack_obj(self, obj):
        """
        The msgpack hook that handles the creation of extension objects for the registered types.
        """
        try:
            tid, packer = self._packers[type(obj)]
            return msgpack.ExtType(tid, self.pack(packer(obj)))
        except KeyError:
            if isinstance(obj, int): # Convert int types directly to int, prevents some problematic edge cases.
                return int(obj)
            for super_type in self._super_types: # Check to see if the given object is a sub-type of a registered super-type.
                if isinstance(obj, super_type):
                    tid, packer = self._packers[super_type]
                    return msgpack.ExtType(tid, self.pack(packer(obj)))
            raise TypeError("Type '{}' of object '{}' unknown to msgpack serialiser.".format(type(obj), obj))

    def _unpack_obj(self, code, data):
        """
        The msgpack hook that handles the unpacking of extension objects for the registered types.
        """
        try:
            return self._unpackers[code](self.unpack(data))
        except KeyError:
            return msgpack.ExtType(code, data)


def create_registry():
    """
    Create a msgpack extension manager with some default types already registered.
    """
    msgpack_registry = MsgpackCustomExt()

    # Register slice type
    msgpack_registry.register_obj(slice,
        lambda obj: {i:x for i, x in enumerate([obj.start, obj.stop, obj.step]) if x is not None},
        lambda data: slice(*( ( data[i] if i in data else None) for i in range(3) )))

    # Register Ellipsis type
    msgpack_registry.register_obj(type(Ellipsis), lambda obj: u'', lambda data: Ellipsis)

    # Register tuple type. By default, msgpack converts tuples to lists which is problematic for indexing operations.
    # This behaviour is disabled by `strict_types=True` but this disables handling of tuples by msgpack, so a tuple
    # extension type needs to be created.
    msgpack_registry.register_obj(tuple, lambda obj: list(obj), lambda data: tuple(data))

    # Register numpy ndarray
    def _msgpack_ndarray(ary):
        # There is a speed break-even point in a custom encoding, at around 10 elements.
        # Size break-even is substainally larger, but time is more important.
        if ary.size > 10:
            return {0: numpy_utils._dtype_descr(ary.dtype), 
                    1: ary.shape,
                    2: ary.tobytes()}
        else:
            return ary.tolist()
    def _msgunpack_ndarray(data):
        if isinstance(data, list):
            return np.array(data)
        else:
            #return np.array(data[2], dtype=np.dtype(data[0])).reshape(data[1])
            return np.frombuffer(data[2], dtype=np.dtype(data[0])).reshape(data[1])
    msgpack_registry.register_obj(np.ndarray, _msgpack_ndarray, _msgunpack_ndarray)

    # Register numpy scalar types. The root super-type np.number is registered, and all sub-types directly encoded in
    # the description. 
    msgpack_registry.register_supertype(np.number, 
        lambda obj: obj.dtype.char.encode('utf-8') + obj.tobytes(), 
        lambda data: np.frombuffer(data[1:], dtype=np.dtype(data[:1]))[0])

    # Register numpy bool scalar type. This type is not a sub-type of np.number, so it requires seperate handling.
    msgpack_registry.register_obj(np.bool_, lambda obj: obj.item(), lambda data: data)

    # Register numpy string type. 
    def _msgunpack_npstring(dtype):
        return lambda data: np.frombuffer(data, dtype=dtype*(len(data)//(dtype*1).itemsize))[0]
    msgpack_registry.register_obj(np.str_, lambda obj: obj.tobytes(), _msgunpack_npstring(np.dtype(np.str_)) )
    if np.str_ is not np.unicode_: # np.str and np.unicode are different types in python 2.
        msgpack_registry.register_obj(np.unicode_, lambda obj: obj.tobytes(), _msgunpack_npstring(np.dtype(np.unicode_)) )

    return msgpack_registry


class MsgpackExtType(object):
    """ A base class that defines a packing and unpacking method that can be used by derived types. """
    # _pack_map is a list of attribute names that should be saved and reconstructed by the packer and unpacker.
    _pack_map = []

    def __msgpack__(self):
        """
        Packs this instance into a dictionary, where each element corresponds to an attribute of this object.
        :return: A dictionary describing this object.
        """
        desc = { i: getattr(self, name) for i, name in enumerate(type(self)._pack_map) if i is not None }
        return desc

    @classmethod
    def __msgunpack__(this, data):
        """
        Static method that unpacks the provided dictionary into a new instance of the derived type.
        :param this: The type of the derived class that this static class method was called with.
        :param data: The dictionary that describes an object instance.
        :return: A new instance of the derived type which matches the provided description.
        """
        result = this.__new__(this)
        for i, name in enumerate(this._pack_map):
            setattr(result, name, data[i] if i in data else None)
        return result

# A global registry instance.
msgpack_registry = create_registry()

import pickle

class PickleWrapper:
    """ A class that allows fallback to using pickle using the same API as the msgpack extension manager """
    
    def pack(self, obj):
        return pickle.dumps(obj)

    def unpack(self, data):
        return pickle.loads(data)
