# Copyright (C) 2019 G. H. Collin (ghcollin)
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE.txt file for details.

from . import msgpack_ext

class QueueClosedType(object):
    """ Singleton object which signals that the queue has closed and no more elements will be added. """
    def __new__(cls):
        return QueueClosed
    def __reduce__(self):
        return (QueueClosedType, ())
    def __copy__(self):
        return QueueClosed
    def __deepcopy__(self, memo):
        return QueueClosed
    def __call__(self, default):
        pass

# Create singleton
try:
    QueueClosed
except NameError:
    QueueClosed = object.__new__(QueueClosedType)
    
msgpack_ext.msgpack_registry.register_obj(type(QueueClosed), lambda obj: b'', lambda data: QueueClosed)

class QueueClosedException(Exception):
    pass

class SubprocessException(Exception):
    """ Base class for forwarding exceptions that happen inside a subprocess. """
    pass


def CreateSubprocessException(cause):
    """
    Given an instance of an exception which was raised inside a subprocess, create a SubprocessException
    that encapsulates it. The created exception will have a type that inherits from the type of the
    given cause.
    
    :param cause: An exception object.
    :return: A SubprocessException that inherits and encapsulates the given cause.
    """
    class SubprocessExceptionImpl(type(cause), SubprocessException):
        def __init__(self):
            super(SubprocessExceptionImpl, self).__init__()
            self.__cause__ = cause
        
        def __str__(self):
            out = super(SubprocessExceptionImpl, self).__str__()
            cause = self.__cause__
            if hasattr(cause, '__traceback_str__') and cause.__traceback_str__:
                tbs = cause.__traceback_str__
                out += "\n\nThe above exception was caused by the following exception:\n\n"
                out += ''.join(tbs + ["{}: {}".format(cause.__class__.__name__, cause)])
            return out

    return SubprocessExceptionImpl()