
import os
import math

import numpy as np
from numpy.lib.stride_tricks import as_strided

import pandas as pd
from pandas.core.generic import NDFrame

from collections import deque
from abc import ABCMeta, abstractmethod
from typing import Any, Self, Iterable, overload

from concurrent.futures import Future, ThreadPoolExecutor

from coolruns.tool import Singleton
from coolruns.tool import PyDBOps
from coolruns.typn import hstr, idict

#from coolruns.dimm.datman import DataDeck


__all__ = ["Dimm", "RootDimm", "DimmIterator", "IterOps", "CallableDeque"]




class DimmSchema:
    __kwargs__: dict = dict()
    __parent__: Self = None
    __child__: Self = None

    @property
    def parent(self) -> Self:
        return self.__parent__
    @parent.setter
    def parent(self, parent: Self) -> None:
        self.__parent__ = parent
        if parent:
            self.__parent__.__child__ = self
    @property
    def child(self) -> Self:
        return self.__child__
    @child.setter
    def child(self,_) -> None: ...

    def __init__(self, parent: Self = None, **kwargs) -> None:
        self.parent, self.depth = parent, parent.depth+1 if parent else 0
        self.__kwargs__ = kwargs
        self(self)

    def __getattr__(self, item: str) -> Any:
        return self.__kwargs__.get(item, None) or super(DimmSchema, self).__getattribute__(item)

    def __call__(self, other: object) -> Self:
        [setattr(other, anam, aval) for anam, aval in self.__kwargs__.items() if not hasattr(other, anam)]
        return self

    def __next__(self):
        return self.child

    def __iter__(self) -> Self:
        schema = self.child
        while schema:
            yield schema
            schema = schema.child



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
    __schema__: DimmSchema = None
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
    @property
    def schema(self) -> DimmSchema:
        return self.__schema__
    @schema.setter
    def schema(self, schema: DimmSchema) -> None: ...

    # TODO::REPLACE DATA INFRASTRUCTURE
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
        __pcol__: list[str] = list()
        @property
        def prvs(self) -> np.ndarray:
            return self.__prvs__
        @prvs.setter
        def prvs(self,data: np.ndarray) -> None:
            self. __prvs__ = data

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
            self.prvs = data if self.prvs is None else np.concatenate((self.prvs, data), axis=0)
            self.__pcol__.extend(columns)
            return self

        def __len__(self) -> int:
            return self.__wdat__.shape[0]

        def free(self):
            self.__data__ = None
            self.__wdat__ = None
            self.__prvs__ = None
            self.__pcol__ = None


    __data__: DimmData = None
    @property
    def data(self) -> np.ndarray:
        return self.__data__
    @data.setter
    def data(self,_) -> None: ...


    def __init__(self, parent: Self, name: str, wlen: int) -> None:
        self.__schema__ = DimmSchema(parent=parent.schema, name=name, wlen=wlen)(self)
        self.parent, self.name, self.wlen  = parent, name, wlen
        # TODO::REPLACE DATA INFRASTRUCTURE
        self.__data__ = Dimm.DimmData(pdat=parent.data.data, wlen=wlen, shap=self.root.data.data.shape, strd=self.root.data.data.strides)
        self.root.__registar__(self)
        pass

    def __next__(self):
        return getattr(self, "parent" if self.root.__ritr__ else "child", None)

    def __iter__(self) -> Self:
        dimm = self
        while dimm:
            yield dimm
            dimm = next(dimm)

    def __reversed__(self) -> Self:
        self.root.__ritr__ = True
        yield from self
        self.root.__ritr__ = False

    def __bool__(self) -> bool:
        return bool(len(self))

    def __len__(self) -> int:
        return len(self.data)

    def __repr__(self) -> str:
        shape = self.data.data.shape
        return f'[ {self.__class__.__name__} : {hex(id(self))} ] [ Full Length : {math.prod(shape):,} | Windows : {math.prod(shape)//shape[-1]:,} | Depth : {len(shape)} ] ( {", ".join(map(str, shape))} )'

    def free(self):
        """ palestine """
        if self.__child__:
            self.__child__.free()
        self.data.free()
        self.__data__ = self.__child__ = self.__parent__ = None
        pass



class RootDimm(Dimm):
    __reco__: idict[str, Dimm] = idict()
    __ritr__: bool = False

    @property
    def leaf(self) -> Self:
        return self.__reco__[-1] or self
    @leaf.setter
    def leaf(self, _) -> None:...
    @property
    def root(self) -> Self:
        return self
    @property
    def parent(self) -> np.ndarray|NDFrame:
        return self.__parent__
    @parent.setter
    def parent(self, parent: Self) -> None:...

    # TODO::REPLACE DATA INFRASTRUCTURE
    class DimmData:
        __data__: np.ndarray = None
        @property
        def data(self) -> np.ndarray|NDFrame:
            return self.__data__
        def __init__(self, data: np.ndarray):
            self.__data__ = data
        def __len__(self) -> int:
            return len(self.data)
        def free(self):
            self.__data__ = None

    @overload
    def __init__(self, data: pd.DataFrame, *, schema: DimmSchema = None):...
    @overload
    def __init__(self, data: np.ndarray, feld: Iterable[hstr] = None, *, schema: DimmSchema = None):...
    def __init__(self, *args, base=None, **kwargs):
        None and super(RootDimm, self).__init__(None, None)
        data, feld = tuple(args+(None,None))[:2]
        data, feld = kwargs.pop('data', data), kwargs.pop('feld', feld)
        #TODO::REPLACE DATA INFRASTRUCTURE
        self.__parent__, self.__data__ = data, RootDimm.DimmData(data if isinstance(data, np.ndarray) else data.values)
        self.__schema__ = DimmSchema(name="__root__", wlen=len(self.__data__), feld=feld)(self)
        self.__reco__ = idict({self.name: self})
        [Dimm(self.leaf, name=schm.name, wlen=schm.wlen) for schm in base.schema] if base and base.schema else None

    def __repr__(self) -> str:
        return f'[ {self.__class__.__name__} : {hex(id(self))} ] [ Source Data : {type(self.data.data).__module__}.{type(self.data.data).__name__} ( {", ".join(map(str, self.data.data.shape))} ) ]'

    def __bool__(self):
        return True

    def __next__(self):
        return None if self.__ritr__ else super(RootDimm, self).__next__()

    def __reversed__(self) -> None:
        self.__ritr__ = True
        yield from reversed(self.leaf)

    def __getitem__(self, item: int|str) -> Self|Dimm:
        return self.__reco__[item]

    def __registar__(self, dimm: Dimm):
        if dimm.name in self.__reco__:
            dimm.name = f"{dimm.name}_{len(self.__reco__)}"
        self.__reco__[dimm.name] = dimm

    def assess_bases(self):
        pass



class CallableDeque(deque):
    def __init__(self, *args, **kwargs):
        super(CallableDeque, self).__init__(*args, **kwargs)
    def __call__(self, result: np.ndarray, columns: Iterable[hstr]):...



class DimmIterator(CallableDeque):
        def __init__(self, data: Dimm|Dimm.DimmData, batch: int = 1024, stride: int = 1):
            self.data: Dimm.DimmData = data if isinstance(data, Dimm.DimmData) else data.data
            self.batch: int = len(self.data) if not batch or batch is Ellipsis else batch
            self.stride: int = stride,
            blen = len(self.data)//batch
            dlen = blen*batch
            asym = len(self.data)-dlen
            super(DimmIterator, self).__init__(([self.data.wdat[:asym]] if asym else [])+np.vsplit(self.data.wdat[-dlen:], blen))
        def __call__(self, result: np.ndarray, columns: Iterable[str]) -> Self:
            self.data(data=np.atleast_2d(np.concatenate(result, axis=0)), columns=columns)
        def __repr__(self) -> str:
            return f'[ {self.__class__.__name__} : {hex(id(self))} ] [ Full Length : {(self.batch*(len(self)-1))+len(self[0])} | Batches : {len(self):,} | Batch Size : {self.batch:,} | Stride : {self.stride:,} ]'



class IterOps(metaclass=Singleton):
    def __init__(self, maxw: int = round(os.cpu_count()*0.75)):
        self.maxw: int = maxw
    def __call__(self, pack: CallableDeque, oper: Callable, columns: Iterable[hstr], **kwargs) -> Any:
        with ThreadPoolExecutor(max_workers=self.maxw, thread_name_prefix=self.__class__.__name__) as executor:
            futures: list[Future] = [executor.submit(oper, *(item,), **kwargs) for item in pack]
            pack([f.result() for f in futures], columns=columns)
    def __repr__(self) -> str:
        return f'[ {self.__class__.__name__} : {hex(id(self))} ] [ Max Workers : {self.maxw:,} ]'




if __name__ == "__main__":
    pass