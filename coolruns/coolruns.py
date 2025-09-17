
import numpy as np
import pandas as pd

from typing import Hashable

from coolruns.dimms import DimmBase, Dimm, RootDimm
from coolruns.persistor import PersistentAccessor, gcollect


__all__ = ["CoolRunningsBase"]



class CoolRunningsBase(DimmBase, PersistentAccessor):
    __link__ = Dimm.__link__

    @property
    def data(self) -> np.ndarray:
        return self.child.__data__
    @property
    def dloc(self) -> np.ndarray:
        return self.child.dloc
    @property
    def wdat(self) -> np.ndarray:
        return self.child.wdat

    @property
    def ndim(self) -> np.ndarray:
        return self.data.ndim
    @property
    def feld(self) -> dict[str,np.dtype]:
        return self.__feld__


    def __init__(self, obj: pd.DataFrame, *args, **kwargs) -> None:
        super(CoolRunningsBase, self).__init__(*args, **kwargs)
        self.__root__ = self.__last__ = RootDimm(self)

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
