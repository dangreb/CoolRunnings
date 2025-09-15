import os

import numpy as np
import pandas as pd

from typing import Any, Iterable, final
from abc import ABCMeta, abstractmethod

from concurrent.futures import Future, ThreadPoolExecutor


__all__ = ["RunOper", "IterOps"]



class IterOpBase(metaclass=ABCMeta):

    @property
    @abstractmethod
    def columns(self) -> Iterable[str]:...
    @property
    @abstractmethod
    def index(self) -> Iterable[Any]|pd.Index:...
    @abstractmethod
    def __call__(self, batch: np.ndarray) -> np.ndarray:...

    def __init__(self, wdat: np.ndarray, blen: int = 1024, leap: int = 1):
        self.wdat, self.blen, self.leap = wdat, blen or self.data.shape(0), leap

    def __iter__(self) -> np.ndarray:
        blen = self.wdat.shape(0)//self.blen
        dlen = blen*self.blen
        asym = self.wdat.shape(0)-dlen
        yield from ([self.wdat[:asym]] if asym else [])+np.vsplit(self.wdat[-dlen:], blen)



class RunOpMeta(type):
    __inst__: dict[type ,object] = dict()
    def __call__(cls, *args, **kwargs):
        return cls.__inst__.get(cls, None) or cls.__inst__.setdefault(cls, super(ItropMeta, cls).__call__(*args, **kwargs))
    def __iter__(cls):
        yield from cls.__inst__.items()

class RunOper(metaclass=RunOpMeta):
    def __init__(self, maxw: int = round(os.cpu_count()*0.75)):
        self.maxw: int = maxw
    def __call__(self, iterop: IterOps, asdf: bool = False) -> tuple[np.ndarray,Iterable[str],pd.Index]|tuple[np.ndarray,Iterable[str],Iterable[Any]]|pd.DataFrame:
        with ThreadPoolExecutor(max_workers=self.maxw, thread_name_prefix=self.__class__.__name__) as executor:
            futures: list[Future] = [executor.submit(iterop, *(item,), **kwargs) for item in iterop]
            result = np.concat([f.result() for f in futures], axis=1), iterop.columns, iterop.index
            return result if not asdf else pd.DataFrame(result[0],**dict(columns=result[1], index=result[2]))


class IterOps(IterOpBase):

    @property
    @abstractmethod
    def columns(self) -> Iterable[str]:...

    @property
    @abstractmethod
    def index(self) -> Iterable[Any]|pd.Index:
        return range(self.wdat.shape[0])

    @abstractmethod
    def __call__(self, batch: np.ndarray) -> np.ndarray:...

    class _RunOper(RunOper):
        def __call__(self, *args, asdf: bool = False) -> tuple[np.ndarray, Iterable[str], pd.Index]|tuple[np.ndarray, Iterable[str], Iterable[Any]]|pd.DataFrame:
            return super(IterOps.RunOper, self).__call__(self, iterop=self, asdf=asdf)

    @final
    @property
    def RunOper(self) -> type[_RunOper]:
        return IterOpBase.RunOper

