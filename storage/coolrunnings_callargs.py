
import gc
import copy
import warnings

import numpy as np
import pandas as pd
import weakref as wk

from typing import overload, Self, Optional

from pandas.api.extensions import register_dataframe_accessor

from coolruns.dimm.dimm import DimmSchema
from coolruns.typn import roll
from coolruns.dimm import Dimm, RootDimm
from coolruns.tool import AccessorPersistor

__all__ = ["CoolRunnings"]



@register_dataframe_accessor("coolruns")
class CoolRunnings(metaclass=AccessorPersistor):

    def __init__(self):
        self.root = None
        pass
    def __complete__(self, pobj: pd.DataFrame):
        self.root = RootDimm(data=pobj.values, base=self.root)

    def __next__(self) -> Dimm: ...
    def __iter__(self) -> Dimm:
        yield from self.root
    def __reversed__(self) -> Dimm:
        yield from reversed(self.root)
    def __contains__(self, name: str) -> bool:
        for dimm in self:
            if dimm.name == name:
                return True
        return False
    def __getitem__(self, name: str) -> Dimm:
        for dimm in self:
            if dimm.name == name:
                return dimm
        return self.leaf

    def free(self):
        """ palestine """
        self.root.free()
        self.root = None
        gc.collect()
        pass

    def __single_roll(self, roll: roll) -> Self:
        Dimm(self.root.leaf, **roll)
        return self


    @overload
    def __call__(self, wlen: int, name: str = None) -> Self:...
    @overload
    def __call__(self, shape: roll|list[roll]|tuple[list[roll]]) -> Self: ...
    def __call__(self, *args) -> Self:
        if "wlen" in kwargs or (args and isinstance(args[0], int)):
            pars = roll(dict(zip(("wlen", "name"),args)))
            pars.update(kwargs)
            self.__single_roll(pars)
            return self
        if args:
            if isinstance(args[0], list|tuple):
                [self.__single_roll(dmm) for dmm in args]
                return self
            if isinstance(args[0], roll):
                self.__single_roll(args[0])
                return self
        if "shape" in kwargs:
            if isinstance(kwargs["shape"], list|tuple):
                [self.__single_roll(dmm) for dmm in kwargs["shape"]]
                return self
            if isinstance(kwargs["shape"], roll):
                self.__single_roll(kwargs["shape"])
                return self
        return self
