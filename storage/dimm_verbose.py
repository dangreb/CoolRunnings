
import numpy as np
from numpy.lib.stride_tricks import as_strided

import pandas as pd
from pandas.core.generic import NDFrame

from typing import Self, Iterable

from coolruns import CoolRunnings
from coolruns.typn import hstr, idict
from coolruns.tool import opt

__all__ = ["Dimm", "RootDimm"]



class Dimm:
    __data__: np.ndarray = None
    __wdat__: np.ndarray = None
    __idex__: np.ndarray|pd.Index = None
    __didx__: np.ndarray = None
    __child__: Self = None
    __parent__: Self = None

    @property
    def data(self) -> np.ndarray:
        return self.__data__
    @data.setter
    def data(self,_) -> None: ...
    @property
    def wdat(self) -> np.ndarray:
        return self.__wdat__
    @wdat.setter
    def wdat(self,_) -> None: ...
    @property
    def didx(self) -> np.ndarray:
        return self.__didx__
    @didx.setter
    def didx(self,_) -> None: ...
    @property
    def root(self) -> Self:
        return self.parent.root
    @root.setter
    def root(self,_) -> None: ...
    @property
    def idex(self) -> np.ndarray|pd.Index:
        return self.root.widx(self.wlen)
    @idex.setter
    def idex(self,_) -> None: ...
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
        if isinstance(parent, Dimm):
            self.__parent__.__child__ = self


    class DimmDataVeue:
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
            benefits of one "Window/Dimension Local Data Scope" feature implementation.

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
        __full_data__: np.ndarray = None
        __full_idex__: np.ndarray = None
        __dimm_data__: np.ndarray = None
        __dimm_idex__: np.ndarray = None

        """ 
        Data viewed as from Full Dataframe Scope  
        Preceding dimensionnal shapes and depths inciding
        """
        @property
        def fdat(self) -> np.ndarray:
            """ Source data as fully reshaped at depth
            :return:
            """
            return self.__full_data__
        @fdat.setter
        def fdat(self,_) -> None: ...
        @property
        def idex(self) -> np.ndarray|pd.Index:
            """ Numeric index as fully reshaped at depth #TODO:: Currently under assessment, may be removed
            :return:
            """
            return self.__base_idex__
        @idex.setter
        def idex(self,_) -> None: ...

        """ 
        Data and Index, viewed as from Local Dimmension Scope
        Shaped as if the current was the first slide through
        """
        @property
        def wdat(self) -> np.ndarray:
            """ Source data as shaped at local scope
            :return:
            """
            return self.__wndw_data__
        @wdat.setter
        def wdat(self,_) -> None: ...
        @property
        def widx(self) -> np.ndarray:
            """ Numeric index as shaped at local scope
            :return:
            """
            return self.__wndw_idex__
        @widx.setter
        def widx(self,_) -> None: ...

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
            #TODO:: Receive Window Scope Data
            return self


    class DimmIterator:
        """"""
        def __init__(self, dimm: type, iops: type = object, batch: int = 256, stride: int = 1, feld: Iterable[hstr] = Ellipsis):
        #def __init__(self, dimm: Dimm, iops: type = object, batch: int = 256, stride: int = 1, feld: Iterable[hstr] = Ellipsis):
            self.dimm, self.iops, self.batch, self.stride, self.columns, self.symetry = dimm, iops, batch or 1, stride, list(hstr(feld)), (len(dimm)//batch)*batch

        def __iter__(self) -> np.ndarray:
            for dlot in [self.dimm.wdat[0:len(self.dimm)-self.symetry]]+np.vsplit(self.dimm.wdat[-self.symetry:], len(self.dimm)//self.batch):
                yield hash(self.iops(dimm=self, data=dlot))

        def __hash__(self):
            return id(self)





    def __init__(self, parent: Self, name: str, wlen: int = 0, stride: int = 1) -> None:
        self.parent, self.name, self.wlen, self.stride  = parent, name, wlen, stride
        self.depth = self.root.__registar__(self)
        self.length = len(self.idex)
        #TODO:: Window Scope Data
        self.__data__ = self.__slide_data__(wlen, parent.data)
        self.__wdat__ = self.__slide_wdat__()
        self.__didx__ = self.__make_didx__()
        pass


    def __slide_data__(self, wlen: int, pdat: np.ndarray) -> np.ndarray:
        shape = ((pdat.shape[0]-wlen)+1,)+(wlen,)+pdat.shape[1:]
        strde = (pdat.strides[0],)+pdat.strides
        return as_strided(pdat, shape=shape, strides=strde, writeable=False).swapaxes(1,-2)

    def __slide_wdat__(self) -> np.ndarray:
        return as_strided(self.data, shape=self.idex.shape+self.data.shape[-1:], strides=self.idex.strides[:1]+self.idex.strides)

    def __make_didx__(self) -> np.ndarray:
        return as_strided(self.idex, shape=self.data.shape[:-1], strides=self.data.strides[:-1])

    def __bool__(self) -> bool:
        return bool(self.length)

    def __len__(self) -> int:
        return self.length

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}: {self.depth} {self.name} {self.data.shape[:-1]} at {hex(id(self))}>'

    def free(self):
        """ palestine """
        if self.__child__:
            self.__child__.free()
        self.__data__ = self.__idex__ = self.__child__ = self.__parent__ = None
        pass



class RootDimm(Dimm):
    __leaf__: Self = None
    __irec__: dict[int, np.ndarray] = dict()
    __reco__: idict[str, Dimm] = idict()
    @property
    def idex(self) -> np.ndarray:
        return self.__idex__
    @idex.setter
    def idex(self, _) -> None: ...
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
        return None
    @parent.setter
    def parent(self, parent: Self) -> None:...


    def __init__(self, data: np.ndarray, feld: Iterable[hstr] = None):
        None and super(RootDimm, self).__init__(None, None)
        self.name, self.feld, self.__data__, = "__root__", list(hstr(feld or "")), data
        self.__idex__ = np.arange(data.shape[0], dtype=opt.window_index_dtype)

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
        self.__make_idex__(wlen=dimm.wlen)
        return len(self.__reco__)

    def __midx__(self, wlen: int) -> np.ndarray:
        shape = ((self.idex.shape[0]-wlen)+1,)+(wlen,)
        strde = (self.idex.strides[0],)+self.idex.strides
        return as_strided(self.idex, shape=shape, strides=strde, writeable=False)

    def __make_idex__(self, wlen: int) -> np.ndarray:
        return self.__irec__[wlen] if wlen in self.__irec__ else self.__irec__.setdefault(wlen, self.__midx__(wlen))

    def widx(self, wlen: int) -> np.ndarray:
        return self.__irec__.get(wlen, None)

    def assess_bases(self):
        pass




if __name__ == "__main__":
    """
    dset = as_strided(np.arange(1024, dtype=np.float32), shape=(1024, 4), strides=(np.uint32().nbytes,0)).copy()

    rdmm = RootDimm(dset)
    Dimm(Dimm(Dimm(rdmm, "dimm1", 16), "dimm2", 8), "dimm3", 4)

    dmms: list[Dimm] = list(rdmm)
    data: list[np.ndarray] = [dimm.data for dimm in rdmm]
    didx: list[np.ndarray] = [dimm.idex for dimm in rdmm]
    name: list[np.ndarray] = [dimm.name for dimm in rdmm]

    list(rdmm.child)

    breakpoint()
    """