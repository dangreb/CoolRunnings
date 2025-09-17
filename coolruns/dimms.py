
import numpy as np

from numpy.lib.stride_tricks import as_strided

from typing import Hashable

from coolruns.chain import Chain


__all__ = ["DimmBase", "Dimm", "RootDimm"]



class DimmBase(Chain):

    __data__: np.ndarray
    __dloc__: np.ndarray
    __wdat__: np.ndarray

    @property
    def data(self) -> np.ndarray:
        return self.__data__
    @property
    def dloc(self) -> np.ndarray:
        return self.__dloc__
    @property
    def wdat(self) -> np.ndarray:
        return self.__wdat__

    @property
    def shape(self) -> tuple[int,...]:
        return getattr(self.data, "shape", [])
    @property
    def stride(self) -> tuple[int,...]:
        return getattr(self.data, "strides", [])

    def __len__(self) -> int:
        return self.shape[0]

    def __free__(self):
        """ palestine """
        [getattr(dimm, "__free__", bool)() for dimm in self if dimm.step > 0]
        super(DimmBase, self).__free__()
        del self.__dloc__
        del self.__data__
        del self.__wdat__
        del self.__shap__
        del self.__strd__



class Dimm(DimmBase):

    def __init__(self, root: DimmBase, wlen: int, alias: Hashable = None, **kwargs):
        super(Dimm, self).__init__(alias, wlen=wlen, **kwargs)
        self.__data__ = as_strided(root.last.data, shape=(root.last.shape[0]-wlen+1,)+(wlen,)+root.last.shape[1:], strides=(root.last.stride[0],)+root.last.stride, writeable=False)
        self.__wdat__ = as_strided(self.data, shape=(root.shape[0]-wlen+1,)+(wlen,)+root.shape[-1:], strides=root.stride[:1]+root.stride, writeable=False)
        self.__dloc__ = np.empty(self.wdat.shape[0], dtype=np.int64)



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