
import numpy as np
from numpy.lib.stride_tricks import as_strided

import pandas as pd
from pandas.core.generic import NDFrame, Index


from typing import Self

from coolruns.typn import idict
from coolruns._options import opt

__all__ = ["Dimm", "RootDimm"]



class Dimm:
    __sdat__: np.ndarray = None
    __idex__: np.ndarray|pd.Index = None
    __child__: Self = None
    __parent__: Self = None
    @property
    def sdat(self) -> np.ndarray:
        return self.__sdat__
    @sdat.setter
    def sdat(self,_) -> None: ...
    @property
    def root(self) -> Self:
        return self.parent.root
    @root.setter
    def root(self,_) -> None: ...
    @property
    def idex(self) -> np.ndarray|pd.Index:
        return self.root.widx(self.length)
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

    def free(self):
        """ palestine """
        if self.__child__:
            self.__child__.free()
        self.__sdat__ = self.__idex__ = self.__child__ = self.__parent__ = None
        pass

    def __init__(self, parent: Self, name: str, length: int = 0) -> None:
        self.parent, self.name, self.length  = parent, name, length
        self.__sdat__ = self.__runn__(length, parent.sdat)
        self.depth = self.root.__registar__(self)

    def __runn__(self, wlen: int, pdat: np.ndarray) -> np.ndarray:
        shape = ((pdat.shape[0]-wlen)+1,)+(wlen,)+pdat.shape[1:]
        strde = (pdat.strides[0],)+pdat.strides
        return as_strided(pdat, shape=shape, strides=strde, writeable=False).swapaxes(1,-2)

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}: {self.depth} {self.name} {self.sdat.shape[:-1]} at {hex(id(self))}>'


class RootClassMeta(type):
    def __call__(cls, *args, **kwargs):
        __root_class__ = (NpRootDimm,PdRootDimm)[int(isinstance(kwargs.get("data", (args[0] if args else None)), NDFrame))]
        return super(RootClassMeta,__root_class__).__call__(*args, **kwargs)


class RootDimm(Dimm, metaclass=RootClassMeta):
    __leaf__: Self = None
    __irec__: dict[int, Dimm] = dict()
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
        return None
    @parent.setter
    def parent(self, parent: Self) -> None:...

    def __registar__(self, dimm: Dimm) -> int:
        if dimm.name in self.__reco__:
            dimm.name = f"{dimm.name}_{len(self.__reco__)}"
        self.__reco__[dimm.name] = dimm
        self.__make_idex__(wlen=dimm.length)
        return len(self.__reco__)

    def __make_idex__(self, wlen: int) -> np.ndarray|pd.Index:...

    def widx(self, wlen: int) -> np.ndarray|pd.Index:
        self.__irec__.get(wlen, None)

    def assess_bases(self):
        pass

    def __getitem__(self, item: int|str) -> Self|Dimm:
        return self.__reco__[item]

    def __iter__(self) -> Dimm:
        dimm = self.child
        while dimm:
            yield dimm
            dimm = dimm.child



class NpRootDimm(RootDimm):
    @property
    def idex(self) -> np.ndarray:
        return self.__idex__
    @idex.setter
    def idex(self, _) -> None: ...


    def __init__(self, data: np.ndarray):
        None and super(RootDimm, self).__init__(None, None)
        self.name, self.__sdat__ = "__root__", data
        self.__idex__ = np.arange(data.shape[0], dtype=opt.window_index_dtype)


    def __midx__(self, wlen: int) -> np.ndarray:
        shape = ((self.idex.shape[0]-wlen)+1,)+(wlen,)
        strde = (self.idex.strides[0],)+self.idex.strides
        return as_strided(self.idex, shape=shape, strides=strde, writeable=False)

    def __make_idex__(self, wlen: int) -> np.ndarray:
        super(NpRootDimm, self).__make_idex__(wlen=wlen)
        self.__irec__.get(wlen, None) or self.__irec__.update(wlen=self.__midx__(wlen))



class PdRootDimm(RootDimm):
    @property
    def idex(self) -> pd.Index:
        return self.__idex__
    @idex.setter
    def idex(self,_) -> None: ...


    def __init__(self, data: pd.DataFrame):
        None and super(PdRootDimm, self).__init__(None, None)
        self.name, self.__sdat__ = "__root__", data.values
        self.__idex__: pd.Index = data.index

    def __midx__(self, wlen: int) -> np.ndarray:
        nidx = self.idex.values
        shape = ((nidx.shape[0]-wlen)+1,)+(wlen,)
        strde = (nidx.strides[0],)+nidx.strides
        nidx = as_strided(nidx, shape=shape, strides=strde, writeable=False)
        self.idex.get_indexer(nidx[0,...])
        pass

    def __make_idex__(self, wlen: int) -> np.ndarray:
        super(PdRootDimm, self).__make_idex__(wlen=wlen)
        self.__irec__.get(wlen, None) or self.__irec__.update(wlen=self.__midx__(wlen))





if __name__ == "__main__":
    dset = as_strided(np.arange(1024, dtype=np.float32), shape=(1024, 4), strides=(np.uint32().nbytes,0)).copy()

    rdmm = RootDimm(dset)
    Dimm(Dimm(Dimm(rdmm, "dimm1", 16), "dimm2", 8), "dimm3", 4)

    dmms: list[Dimm] = list(rdmm)
    sdat: list[np.ndarray] = [dimm.sdat for dimm in rdmm]
    didx: list[np.ndarray] = [dimm.idex for dimm in rdmm]
    name: list[np.ndarray] = [dimm.name for dimm in rdmm]

    list(rdmm.child)

    breakpoint()