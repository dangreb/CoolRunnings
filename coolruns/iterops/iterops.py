import os

import numpy as np
import pandas as pd

from typing import Any, Iterable, final
from abc import ABCMeta, abstractmethod

from concurrent.futures import Future, ThreadPoolExecutor

from coolruns.dimms import DimmBase


__all__ = ["IterOps", "RunOper"]



class IterOpBase(metaclass=ABCMeta):

    @property
    @abstractmethod
    def columns(self) -> Iterable[str]:...
    @property
    @abstractmethod
    def index(self) -> Iterable[Any]|pd.Index:...
    @abstractmethod
    def __call__(self, batch: np.ndarray) -> np.ndarray:...

    def __init__(self, dimm: DimmBase, blen: int = None, leap: int = 1):
        self.dimm, self.blen, self.leap = dimm, blen or dimm.wdat.shape[0], leap

    def __iter__(self) -> np.ndarray:

        blen = self.dimm.wdat.shape[0]//self.blen
        dlen = blen*self.blen
        asym = self.dimm.wdat.shape[0]-dlen
        yield from ([self.dimm.wdat[:asym]] if asym else [])+np.vsplit(self.dimm.wdat[-dlen:], blen)



class RunOpMeta(type):
    __inst__: dict[type ,object] = dict()
    def __call__(cls, *args, **kwargs):
        return cls.__inst__.get(cls, None) or cls.__inst__.setdefault(cls, super(RunOpMeta, cls).__call__(*args, **kwargs))
    def __iter__(cls):
        yield from cls.__inst__.items()

class RunOper(metaclass=RunOpMeta):
    def __init__(self, maxw: int = round(os.cpu_count()*0.75)):
        self.maxw: int = maxw
    def __call__(self, iterop: IterOpBase, **kwargs) -> tuple[np.ndarray,Iterable[str],pd.Index]|tuple[np.ndarray,Iterable[str],Iterable[Any]]|pd.DataFrame:
        with ThreadPoolExecutor(max_workers=self.maxw, thread_name_prefix=self.__class__.__name__) as executor:
            futures: list[Future] = [executor.submit(iterop, *(item,), **kwargs) for item in iterop]
            return np.concat([f.result() for f in futures], axis=0), iterop.columns, iterop.index


class IterOps(IterOpBase):

    @property
    def columns(self) -> Iterable[str]:
        return tuple(self.dimm.root.feld.keys())

    @property
    def index(self) -> Iterable[Any]|pd.Index:
        return range(self.dimm.wdat.shape[0])

    @abstractmethod
    def __call__(self, batch: np.ndarray) -> np.ndarray:...

    class _RunOper(RunOper):
        __iops__: IterOpBase = None
        def __call__(self, *args, **kwargs) -> tuple[np.ndarray, Iterable[str], pd.Index]|tuple[np.ndarray, Iterable[str], Iterable[Any]]|pd.DataFrame:
            return super(IterOps._RunOper, self).__call__(iterop=IterOps._RunOper.__iops__, **kwargs)

    @final
    @property
    def RunOper(self) -> type[_RunOper]:
        IterOps._RunOper.__iops__ = self
        return IterOps._RunOper

