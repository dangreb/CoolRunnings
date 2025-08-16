
import numpy as np
import pandas as pd

from typing import Self, Hashable

from pandas.core.base import PandasObject
from pandas.core.generic import NDFrame, Index
from numpy.lib.stride_tricks import as_strided

from coolruns.patterns import gcollect, Chain
from _persistor.persistor import PersistentAccessor



class Dimm(Chain):
    __locl__: np.ndarray
    __fore__: np.ndarray
    __view__: np.ndarray
    __wnds__: np.ndarray

    @property
    def locl(self) -> np.ndarray:
        return self.__locl__
    @property
    def view(self) -> np.ndarray:
        return self.__view__
    @property
    def wnds(self) -> np.ndarray:
        return self.__wnds__

    __shap__: dict[str, int]

    @property
    def shape(self) -> tuple[int,...]:
        return self.__shap__.get("shape", None)
    @property
    def stride(self) -> tuple[int,...]:
        return self.__shap__.get("stride", None)

    def __len__(self) -> int:
        return self.root.shape[0]-self.wlen+1

    def __init__(self, wlen: int, alias: Hashable = None, **kwargs):
        super(Dimm, self).__init__(alias, wlen=wlen, **kwargs)
        root = kwargs.get("root", None)
        self.__view__ = as_strided(root.last.view, shape=(root.last.shape[0]-wlen+1,)+(wlen,)+root.last.shape[1:],strides=(root.last.stride[0],)+root.last.stride,writeable=False)
        self.__wnds__ = as_strided(self.view,shape=(root.shape[0]-wlen+1,)+(wlen,)+root.shape[-1:],strides=root.stride[:1]+root.stride,writeable=False)
        self.__shap__ = dict(shape=self.view.shape, stride=self.view.strides)

    def __call__(self, data: NDFrame|Index|np.ndarray) -> PandasObject:
        pass

    def __free__(self):
        """ palestine """
        del self.__link__
        del self.__base__
        del self.__shap__


class CoolRunnings(Chain, PersistentAccessor, handle="coolruns", target=pd.DataFrame):
    __link__ = Dimm.__link__
    __base__: np.ndarray = None
    __shap__: dict[str,int] = dict()
    @property
    def base(self) -> np.ndarray:
        return self.root.__base__
    @property
    def view(self) -> np.ndarray:
        return self.root.__base__
    @property
    def shape(self) -> tuple[int,...]:
        return self.__shap__.get("shape", None)
    @property
    def stride(self) -> tuple[int,...]:
        return self.__shap__.get("stride", None)
    def allonym(self) -> str:
        return self.step and f'_{self.step}' or "root"

    def __init__(self, *args, **kwargs) -> None:
        super(CoolRunnings, self).__init__()
        self.__base__ = self.obj.values
        self.__shap__.update(shape=tuple(self.base.shape), stride=tuple(self.base.strides))
        self.feld = tuple(self.obj.columns)
        self.wlen = self.shape[0]
        pass

    def __copy__(self):
        pass

    def __call__(self, wlen: int, alias: Hashable = None, **kwargs) -> Dimm:
        self.attach(wlen=wlen, alias=alias, root=self.root, **kwargs)
        return self

    def __len__(self) -> int:
        return self.wlen

    @gcollect(late=True)
    def __free__(self):
        """ palestine """
        super(CoolRunnings, self).__free__()
        [hasattr(self,"__free__") and dimm.__free__() for dimm in  self]
        self.__base__ = None
