
import numpy as np
from numpy.lib.stride_tricks import as_strided

import pandas as pd
from pandas.core.generic import NDFrame

from collections import deque
from abc import ABCMeta, abstractmethod
from typing import Self, Iterable, overload

from coolruns.typn import hstr, idict
from coolruns.tool import opt, Singleton
from coolruns.dimm.iter import CallableDeque


__all__ = ["Dimm", "RootDimm"]


class DimmDataABC(metaclass=ABCMeta):
    @property
    def data(self) -> np.ndarray:
        return None
    @data.setter
    def data(self,_) -> None: ...
    @property
    def wdat(self) -> np.ndarray:
        return None
    @wdat.setter
    def wdat(self,_) -> None: ...
    @abstractmethod
    def __init__(self, pdat: np.ndarray, wlen: int, shap: tuple, strd: tuple) -> None:...
    @abstractmethod
    def __call__(self, data: np.ndarray, columns: Iterable[str]) -> Self: ...
    @abstractmethod
    def __len__(self) -> int:...
    @abstractmethod
    def free(self):...



class Dimm:
    __parent__: Self = None
    __child__: Self = None

    @property
    def root(self) -> Self:
        return self.parent.root
    @root.setter
    def root(self,_) -> None: ...
    @property
    def child(self) -> Self:
        return self.__child__
    @child.setter
    def child(self,_) -> None: ...
    @property
    def parent(self) -> Self:
        return self.__parent__
    @parent.setter
    def parent(self, parent: Self) -> None:
        self.__parent__ = parent
        self.__parent__.__child__ = self


    class DimmData(DimmDataABC):
        """ Namespace for all Incorporated Source Data
            ==========================================
            CoolRunings predicates for optimal data provisioning for multidimensional
            datasets. Especially sliding windows on large data series, high depth, and
            similar performance penalizers.

            Instead of the usual mathemtic abstractions covering most case scenarios,
            attempts logical domain viability leveraging intense employment of NumPy
            strides to enable inumerous structural data compositions renderized from
            the same malloc dataset buffers.

            These premisses relate to a broader investigation regarding hypotetetic
            benefits of one "Window/Dimension Local Data Scope" toolset implementation.

            Dimmension/Window Local Data Scope
            ----------------------------------
            The local data scope is an abritrary visibility context, bound to an
            individual dimmension, its data windows, and their assigned data.

            An individual local scope primarily manifests in the form of available
            source data views, spawn as if reshaped only by that dimmension spec. This
            alone allows for important optimization opportunities when leveraged to
            prevent iterations over identical data windows endogenous to 3D+ depths.

            Another key aspect lies in novel data inception capacities. Deep
            datasets can now incorporate localy immanent features, whose values no
            longer roll-out from the original flat dataset. This means we can federate
            features of a given data series with counterparts whose values may vary
            across the different windows they appear along their native dimmension.

        """
        __data__: np.ndarray = None
        __wdat__: np.ndarray = None
        """ 
        Data viewed as from Full Dataframe Scope  
        Preceding dimensionnal shapes and depths inciding
        """
        @property
        def data(self) -> np.ndarray:
            """ Source data as fully reshaped at depth
            :return:
            """
            return self.__data__
        @data.setter
        def data(self,_) -> None: ...
        """ 
        Data viewed as from Local Dimmension Scope
        Shaped as if the current was the first slide through
        """
        @property
        def wdat(self) -> np.ndarray:
            """ Source data as shaped at local scope
            :return:
            """
            return self.__wdat__
        @wdat.setter
        def wdat(self,_) -> None: ...

        """PROVISORY"""
        __prvs__: np.ndarray = None
        __pcol__: list[str] = list
        @property
        def prvs(self) -> np.ndarray:
            return self.__prvs__
        @prvs.setter
        def prvs(self,_) -> None: ...
        """PROVISORY"""

        def __init__(self, pdat: np.ndarray, wlen: int, shap: tuple, strd: tuple) -> None:
            self.__data__ = as_strided(pdat, shape=((pdat.shape[0]-wlen)+1,)+(wlen,)+pdat.shape[1:], strides=(pdat.strides[0],)+pdat.strides, writeable=False)  # .swapaxes(1,-2)
            self.__wdat__ = as_strided(self.data, shape=(shap[0]-wlen+1,)+(wlen,)+shap[-1:], strides=strd[:1]+strd, writeable=False)

        def __call__(self, data: np.ndarray, columns: Iterable[str]) -> Self:
            """ Ingests data for a window-local data scope, rendered by this Dimm object.

                Data should be provided as 2D arrays, symetric to the shape of Dimm.wdat at
                axis zero, regardless of which and how deep the pertained dimmension.

                For data allotted at shallower dimmensions, an attempt to slide and
                thus prapagation to deeper dimmensions will. No similar behavior takes place
                aiming upward dimmensions.

            :param data:
            :param columns:
            :return:
            """
            # TODO:: Receive Window Scope Data
            #self.kept.append(data) # ,cols)
            self.prvs = data if self.prvs is None else np.concatenate((self.prvs, data), axis=0)
            self.__pcol__.extend(columns)
            return self

        def __len__(self) -> int:
            return len(self.__wdat__)

        def free(self):
            self.__data__ = None
            self.__wdat__ = None


    __data__: DimmData = None
    @property
    def data(self) -> np.ndarray:
        return self.__data__
    @data.setter
    def data(self,_) -> None: ...


    class DimmIterator(CallableDeque):
        def __init__(self, data: DimmDataABC, batch: int = 1024, stride: int = 1):
            self.data: Dimm.DimmData = data
            self.batch: int = len(data) if not batch or batch is Ellipsis else batch
            self.stride: int = stride,
            blen = len(data)//batch
            dlen = blen*batch
            asym = len(data)-dlen
            super(Dimm.DimmIterator, self).__init__(([data.wdat[:asym]] if asym else [])+np.vsplit(data.wdat[-dlen:], blen))
            pass
        def __call__(self, result: np.ndarray, columns: Iterable[str]) -> Self:
            self.data(data=np.atleast_2d(np.concatenate(result, axis=0)), columns=columns)


    def __init__(self, parent: Self, name: str, wlen: int) -> None:
        self.parent, self.name, self.wlen  = parent, name, wlen
        self.__data__ = Dimm.DimmData(pdat=parent.data.data, wlen=wlen, shap=self.root.data.data.shape, strd=self.root.data.data.strides)
        #self.__data__ = Dimm.DimmData(pdat=parent.data.data, wlen=wlen, shap=self.root.data.data.shape[:-1], strd=self.root.data.data.strides)
        self.depth = self.root.__registar__(self)
        self.length = len(self.data)

    def __bool__(self) -> bool:
        return bool(self.length)

    def __len__(self) -> int:
        return self.length

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}: {self.depth} {self.name} {self.data.data.shape[:-1]} at {hex(id(self))}>'

    def free(self):
        """ palestine """
        if self.__child__:
            self.__child__.free()
        self.data.free()
        self.__data__ = self.__child__ = self.__parent__ = None
        pass



class RootDimm(Dimm):
    __leaf__: Self = None
    __irec__: dict[int, np.ndarray] = dict()
    __reco__: idict[str, Dimm] = idict()

    @property
    def leaf(self) -> Self:
        return self.__reco__[-1] or self
    @leaf.setter
    def leaf(self, _) -> None:...
    @property
    def root(self) -> Self:
        return self
    @property
    def parent(self) -> Self:
        return self.__parent__
    @parent.setter
    def parent(self, parent: Self) -> None:...

    class DimmData:
        __data__: np.ndarray = None
        @property
        def data(self) -> np.ndarray:
            return self.__data__
        def __init__(self, data: np.ndarray):
            self.__data__ = data

    @overload
    def __init__(self, data: pd.DataFrame):...
    @overload
    def __init__(self, data: np.ndarray, feld: Iterable[hstr] = None):...
    def __init__(self, *args, **kwargs):
        None and super(RootDimm, self).__init__(None, None)
        data, feld = tuple(args+(None,None))[:2]
        data, feld = kwargs.pop('data', data), kwargs.pop('feld', feld)
        self.name, self.feld, self.__parent__, self.__data__ = "__root__", list(hstr(feld or "")), data, RootDimm.DimmData(data if isinstance(data, np.ndarray) else data.values)
        self.__idex__ = np.arange(data.shape[0], dtype=opt.window_index_dtype)
        pass

    def __getitem__(self, item: int|str) -> Self|Dimm:
        return self.__reco__[item]

    def __iter__(self) -> Dimm:
        dimm = self.child
        while dimm:
            yield dimm
            dimm = dimm.child

    def __registar__(self, dimm: Dimm) -> int:
        if dimm.name in self.__reco__:
            dimm.name = f"{dimm.name}_{len(self.__reco__)}"
        self.__reco__[dimm.name] = dimm
        return len(self.__reco__)

    def assess_bases(self):
        pass




if __name__ == "__main__":
    """
    dset = as_strided(np.arange(1024, dtype=np.float32), shape=(1024, 4), strides=(np.uint32().nbytes,0)).copy()

    rdmm = RootDimm(dset)
    Dimm(Dimm(Dimm(rdmm, "dimm1", 16), "dimm2", 8), "dimm3", 4)

    dmms: list[Dimm] = list(rdmm)
    data: list[np.ndarray] = [dimm.data for dimm in rdmm]
    name: list[np.ndarray] = [dimm.name for dimm in rdmm]

    list(rdmm.child)

    breakpoint()
    """