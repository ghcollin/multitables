# Copyright (C) 2019 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

import wrapt
import numpy as np

class ResourceWrapper(wrapt.ObjectProxy):
    """ A porxy object where the proxied object can be released, freeing any held resources. """

    def release(self):
        """
        Remove the proxied object from this proxy. Any further attempts to access this proxy raises an
        exception.
        """
        self.__wrapped__ = ReleasedResource()

# Dunder == double underscore (special) methods. The following classes define most of the useful ones,
# from which we can grab their names.
_CLASSES_WITH_DUNDER_METHODS = (str(), object, float(), dict(), np.ndarray)
import functools, operator
# Create a set that contains the name of all double underscore methods.
_DUNDER_METHODS = functools.reduce(
    operator.or_, 
    [ set(dir(t)) for t in _CLASSES_WITH_DUNDER_METHODS ], 
    set(['__call__'])
) - set(['__class__', '__init__', '__new__', '__doc__', '__getattribute__'])

class ReleasedResourceError(Exception):
    """ An exception that should be raised when an attempt is made to access a released resource. """
    pass

# Define a function that will raise the exception.
def _raise_released_resource_error(*args, **kwargs):
    raise ReleasedResourceError("This resource has been released and is no longer accessible.")

class ReleasedResource(object):
    def __getattr__(self, name):
        # The wrapper requests __qualname__ from the object it wraps, and correctly handles the case where
        # __qualname__ does not exist. However, if all methods are overridden with _raise_released_resource_error
        # then a ReleasedResourceError exception is throw when accessing __qualname__ which wrapt cannot handle.
        # Thus, __getattr__ is overriden here to raise AttributeError when __qualname__ is requested, and raise
        # ReleasedResourceError for any other requested attribute.
        if name == '__qualname__':
            raise AttributeError()
        else:
            _raise_released_resource_error(name)

# Now take the empty ReleasedResource class and fill all of its special methods with the function that
# raises an exception.
for method_name in _DUNDER_METHODS:
    setattr(ReleasedResource, method_name, _raise_released_resource_error)
