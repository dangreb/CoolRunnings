
import numpy as np
import pandas as pd

from typing import Hashable

from coolruns.dimms import Dimm, DimmBase
from coolruns.persistor import PersistentAccessor, gcollect


class RootDimm(DimmBase):
    __base__: DimmBase

    @property
    def data(self) -> np.ndarray:
        return self.__base__.__data__
    @property
    def dloc(self) -> np.ndarray:
        return self.__base__.__data__
    @property
    def wdat(self) -> np.ndarray:
        return self.__base__.__data__


    def __init__(self, base: DimmBase):
        self.__base__ = base
        super(DimmBase, self).__init__(base.alias)
    def __setattr__(self, name: str, value):
        self.__base__.__setattr__(name, value) if name is not "__base__" else super().__setattr__(name, value)
    def __getattr__(self, name: str):
        return self.__getattribute__(name) if name in dir(self) else getattr(self.__base__, name, None)
    def __getitem__(self, item: str|int):
        return self[item] if item in self else self.__base__[item]



class CoolRunnings(DimmBase, PersistentAccessor, handle="coolruns", target=pd.DataFrame):
    __link__ = Dimm.__link__

    @property
    def data(self) -> np.ndarray:
        return self.last.__data__
    @property
    def dloc(self) -> np.ndarray:
        return self.last.dloc
    @property
    def wdat(self) -> np.ndarray:
        return self.last.wdat

    @property
    def ndim(self) -> np.ndarray:
        return self.data.ndim


    def __init__(self, obj: pd.DataFrame, *args, **kwargs) -> None:
        super(CoolRunnings, self).__init__(*args, **kwargs)
        self.__root__ = self.__last__ = RootDimm(self)
        self.__data__ = obj.values
        self.feld = tuple(obj.columns)

    def __call__(self, wlen: int, alias: Hashable = None, **kwargs) -> Dimm:
        self.attach(root=self.root, wlen=wlen, alias=alias, **kwargs)
        return self

    def __len__(self) -> int:
        return self.shape[0]


    @gcollect(late=True)
    def __free__(self, *args, **kwargs):
        """ palestine """
        [getattr(dimm, "__free__", bool)() for dimm in self if dimm.step > 0]
        self.__data__ = None
