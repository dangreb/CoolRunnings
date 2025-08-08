

import copy

from enum import Enum
from numpy import dtype, uint32, uint64
from typing import Any, Self, Iterable, SupportsIndex, Hashable, Literal

__all__ = ["opt"]

#class __options_meta__(ObjectFILO):
class __options_meta__(type):
    def __new__(cls, name: str, bases: tuple[type,...], attributes: dict[str, Any]):
        super(__options_meta__, cls).__new__(cls, name, bases, attributes)
        attributes.setdefault("__names__", [anam for anam in attributes.keys() if not anam.startswith("_") and isinstance(attributes[anam], property)])
        return super(__options_meta__, cls).__new__(cls, name, bases, attributes)

class options(metaclass=__options_meta__):
    __kept__: Self = None
    __kwrd__: dict = None

    __idex_dtyp__: type = uint32
    @property
    def window_index_dtype(self) -> type:
        return self.__idex_dtyp__
    @window_index_dtype.setter
    def window_index_dtype(self, __dtyp: dtype) -> None:
        if __dtyp not in (uint32, uint64):
            raise TypeError(f'Invalid index data type: "{__dtyp}". Must be uint32 or uint64.')
        self.__idex_dtyp__ = __dtyp

    __max_threads__: int = 16
    @property
    def default_max_threads(self) -> int:
        return self.__max_threads__
    @default_max_threads.setter
    def default_max_threads(self, __numt: int ) -> None:
        self.__max_threads__ = __numt

    __copy_dimms__: bool = True
    @property
    def copy_dimms(self) -> bool:
        return self.__copy_dimms__
    @copy_dimms.setter
    def copy_dimms(self, __cp: bool ) -> None:
        self.__copy_dimms__ = __cp

    def __init__(self, **kwargs):
        [setattr(self,onam, oval) for onam, oval in kwargs.items() if onam in options.__names__]
    def __call__(self, **kwargs):
        self.__kwrd__ = kwargs
        return self

    def __enter__(self):
        self.__kept__ = dict((anam,getattr(self,anam)) for anam in options.__names__)
        [setattr(self, onam, oval) for onam, oval in self.__kwrd__.items() if onam in options.__names__]
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        [setattr(self, onam, oval) for onam, oval in self.__kept__.items() if onam in options.__names__]


opt = options()