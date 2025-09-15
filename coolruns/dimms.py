
import numpy as np

from typing import Hashable

from numpy.lib.stride_tricks import as_strided

from coolruns.chain import Chain
from coolruns.iterops import IterOps



class DimmBase(Chain):

    __dloc__: np.ndarray
    @property
    def dloc(self) -> np.ndarray:
        return self.__dloc__

    __data__: np.ndarray
    @property
    def data(self) -> np.ndarray:
        return self.__data__

    __wdat__: np.ndarray
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

    class WindowRatio(IterOps):
        @property
        def columns(self):
            return "ac_mean", "bc_mean", "cc_mean", "dc_mean"
        @property
        def index(self):
            return super(Dimm.WindowRatio, self).index()

        def __call__(self, batch: np.ndarray) -> np.ndarray:
            #result = Dimm.WindowRatio().RunOper(maxw=8)(asdf=True)
            return part / np.mean(part, axis=1, keepdims=True)

    def __init__(self, root: DimmBase, wlen: int, alias: Hashable = None, **kwargs):
        super(Dimm, self).__init__(alias, wlen=wlen, **kwargs)
        self.__data__ = as_strided(root.last.data, shape=(root.last.shape[0]-wlen+1,)+(wlen,)+root.last.shape[1:], strides=(root.last.stride[0],)+root.last.stride, writeable=False)
        self.__wdat__ = as_strided(self.data, shape=(root.shape[0]-wlen+1,)+(wlen,)+root.shape[-1:], strides=root.stride[:1]+root.stride, writeable=False)
        self.__dloc__ = np.empty(self.wdat.shape[0], dtype=np.int64)


