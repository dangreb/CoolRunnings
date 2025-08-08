
import os
import math

import functools
from importlib.metadata import metadata

import numpy as np
from numpy.lib.stride_tricks import as_strided

import pandas as pd
from pandas.core.generic import NDFrame

from collections import deque
from abc import ABCMeta, abstractmethod
from typing import Any, Self, Iterable, Iterator, Reversible, overload, Hashable, Sequence, SupportsIndex, Tuple

from coolruns.tool import Singleton, allonym
from coolruns.typn import wk, wkRef, Schematics, hstr, idict
from coolruns.tool import LifetimeHook, LifetimeSentinel





class HookBank:
    __hook__: idict[LifetimeHook] = idict()
    def __set_name__(self, owner, name):
        self.owner = owner
        self.name = name
    def __get__(self, instance, owner):
        return instance.__hook__
    def __set__(self, instance, value):
        instance.__hook__ = value



class Schema(Schematics):
    hook = HookBank()
    def __init__(self, *args, **kwargs) -> None:
        self.hook = None
    def __call__(self, hook: LifetimeHook|Iterable[LifetimeHook], /, *args, **kwargs) -> Self:
        #TODO::
        self.hook = hook
        return self



class DimmSchema(Schema):
    __depth__: int = None
    __child__: Schematics = None
    __parent__: Schematics = None
    __kwargs__: dict = dict()

    @property
    def root(self) -> Schematics:
        return self.parent.root
    @root.setter
    def root(self,_) -> None: ...
    @property
    def child(self) -> Schematics:
        return self.__child__
    @child.setter
    def child(self,_) -> None: ...
    @property
    def parent(self) -> Schematics:
        return self.__parent__
    @parent.setter
    def parent(self, parent: Schematics) -> None:
        self.__parent__ = parent
        self.__parent__.__child__ = self
    @property
    def kwargs(self) -> dict:
        return self.__kwargs__
    @kwargs.setter
    def kwargs(self, _) -> None:...


    @overload
    def __init__(self, parent: Schematics, wlen: int, moniker: str = allonym, **kwargs) -> None:...
    def __init__(self, parent: Schematics, wlen: int, **kwargs) -> None:
        super(DimmSchema, self).__init__()
        self.parent, self.wlen, self.depth = parent, wlen, int(parent and parent.depth+1)
        self.__kwargs__ = idict(wlen=wlen, **kwargs)

    def __getattr__(self, item: str) -> Any:
        return self.kwargs.get(item, None) or super(DimmSchema, self).__getattribute__(item)

    def __call__(self, *args, **kwargs) -> LifetimeHook:
        return super(DimmSchema, self).__call__(*args, **kwargs)

    def __iter__(self) -> Schematics:
        schema = self.child
        while schema:
            yield schema
            schema = schema.child



class RootSchema(DimmSchema):
    @property
    def root(self) -> Schematics:
        return self
    @root.setter
    def root(self, parent: Schematics) -> None:...
    @property
    def parent(self) -> Schematics:
        return None
    @parent.setter
    def parent(self, parent: Schematics) -> None:
        self.__parent__ = parent

    @overload
    def __init__(self, wlen: int, feld: Sequence[str] = None, moniker: str = allonym, **kwargs) -> None:...
    def __init__(self, wlen: int, *args, **kwargs) -> None:
        super(RootSchema, self).__init__(None, *args, dict(wlen=wlen, **kwargs))



class DataDeck(metaclass=Singleton):
    __substance__: LifetimeSentinel[LifetimeHook,np.ndarray]
    __dimension__: LifetimeSentinel[LifetimeHook,np.ndarray]
    __optimized__: LifetimeSentinel[LifetimeHook,np.ndarray]

    @property
    def substance(self) -> LifetimeSentinel[LifetimeHook,np.ndarray]:
        return self.__substance__
    @property
    def dimension(self) -> LifetimeSentinel[LifetimeHook,np.ndarray]:
        return self.__dimension__
    @property
    def optimized(self) -> LifetimeSentinel[LifetimeHook,np.ndarray]:
        return self.__optimized__

    def __init__(self) -> None:
        self.__substance__ = LifetimeSentinel()
        self.__dimension__ = LifetimeSentinel()
        self.__optimized__ = LifetimeSentinel()


    @functools.singledispatchmethod
    def __call__(self, issue, schema) -> ...:...


    @__call__.register(np.ndarray)
    def _(self, issue: np.ndarray, schema: Schema, **kwargs) -> Schematics:
        #TODO:: Keep Schema
        #TODO:: Views Infrastructure
        #TODO:: Keep Hook for Leaf & Root Dependencies
        #TODO:: Placeement of Schema/Hooks
        #TODO:: Evaluate RootSchema construction by DataDeck
        schema(self.substance(issue))
        return

    @__call__.register(pd.DataFrame)
    def _(self, issue: pd.DataFrame, schema: Schema, **kwargs) -> Schematics:
        #TODO::
        schema(self.substance(issue.values))
        return


    def release(self, hook: LifetimeHook) -> ...:
        pass




if __name__ == "__main__":
    from fastdata import fastdata
    inar: np.ndarray = fastdata(dlen=1024, edat="2025-07-01", datyp=np.uint32, )["data"]
    indf: pd.DataFrame = pd.DataFrame(**fastdata(dlen=1024*1024, edat="2025-07-01", datyp=np.uint32, ))
    arf = wkRef(inar)
    drf = wkRef(indf)

    pass

    root = RootSchema(wlen=1024)
    leaf = DimmSchema(wlen=64)

    DataDeck()(root, inar)
    DataDeck()(root, indf)

    pass










