

import copy

from enum import Enum
from numpy import dtype, uint32, uint64
from typing import Any, Self, Iterable, SupportsIndex, Hashable, Literal

from coolruns.tool.meta import ObjectFILO

__all__ = ["opt"]

class __options_meta__(ObjectFILO):
    def __new__(cls, name: str, bases: tuple[type,...], attributes: dict[str, Any]):
        super(__options_meta__, cls).__new__(cls, name, bases, attributes)
        attributes.setdefault("__names__", [anam for anam in attributes.keys() if not anam.startswith("_") and isinstance(attributes[anam], property)])
        return super(__options_meta__, cls).__new__(cls, name, bases, attributes)

class options(metaclass=__options_meta__):
    __idex_dtyp__: type = uint32
    @property
    def window_index_dtype(self) -> type:
        return self.__idex_dtyp__
    @window_index_dtype.setter
    def window_index_dtype(self, __dtyp: dtype) -> None:
        if __dtyp not in (uint32, uint64):
            raise TypeError(f'Invalid index data type: "{__dtyp}". Must be uint32 or uint64.')
        self.__idex_dtyp__ = __dtyp

    __queue_lag__: float = 0.5
    @property
    def threading_queue_loop_lag(self) -> float:
        return self.__queue_lag__
    @threading_queue_loop_lag.setter
    def threading_queue_loop_lag(self, __secs: float) -> None:
        self.__queue_lag__ = __secs

    __max_threads__: int = 16
    @property
    def default_max_queue_size(self) -> int:
        return self.__max_threads__
    @default_max_queue_size.setter
    def default_max_queue_size(self, __numt: int ) -> None:
        self.__max_threads__ = __numt


    def __init__(self, **kwargs):
        pass
        [setattr(self,onam, oval) for onam, oval in kwargs.items() if onam in options.__names__]
        pass
    def __call__(self, **kwargs):
        return self.__class__(**kwargs)


opt: options = options()


__DIR = __all__ + [k for k in globals() if k.startswith("__") and k.endswith("__")]
__DIR_SET = frozenset(__DIR)

def __dir__() -> list[str]:
    return __DIR

def __getattr__(name: str) -> object:
    if name == "opt":
        return options.get()
    if name in __DIR_SET:
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")