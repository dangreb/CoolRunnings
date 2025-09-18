import os

import numpy as np
import pandas as pd

from abc import ABCMeta, abstractmethod
from typing import Any, Iterable, final, TypeAlias

from concurrent.futures import Future, ThreadPoolExecutor

from coolruns.dimms import DimmBase


__all__ = ["IterOps", "RunOper"]



OubdType: TypeAlias = tuple[np.ndarray, Iterable[str], pd.Index]|tuple[np.ndarray, Iterable[str], Iterable[Any]]|pd.DataFrame


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
    def __len__(self) -> int:
        return self.dimm.wdat.shape[0]//self.blen
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

    __moni__: dict[str,Any] = dict()
    @property
    def moni(self) -> dict[str,Any]:
        return self.__moni__
    @moni.setter
    def moni(self, open: int):
        self.__moni__ = dict(open=open, done=0)

    class ThreadExec(ThreadPoolExecutor):
        def __call__(self, cb, *args, **kwargs):
            self.callback = cb
            return self
        def submit(self, fn, /, *args, **kwargs):
            future = super(RunOper.ThreadExec, self).submit(fn, *args, **kwargs)
            future.add_done_callback(self.callback)
            return future

    def __init__(self, maxw: int = None):
        self.maxw: int = maxw or round(os.cpu_count()*0.75)
        self.moni, self.barr = 0, 1000
    def __call__(self, iterop: IterOpBase, **kwargs) -> OubdType:
        self.moni = len(iterop)
        #with ThreadPoolExecutor(max_workers=kwargs.pop("maxw", self.maxw), thread_name_prefix=self.__class__.__name__) as executor:
        with RunOper.ThreadExec(max_workers=kwargs.pop("maxw", self.maxw), thread_name_prefix=self.__class__.__name__)(self.callback) as executor:
            pass
            futures: list[Future] = [executor.submit(iterop, *(item,), **kwargs) for item in iterop]
            pass
            futures: np.ndarray = np.concat([f.result() for f in futures], axis=0)
            pass
            return futures, iterop.columns, iterop.index

    def callback(self, done: Future, *args, **kwargs):
        self.moni["done"] += 1
        self.barr -= 1
        if not self.barr:
            print(f'{self.moni["open"]} / {self.moni["done"]} / {self.moni["open"]-self.moni["done"]} / {round(self.moni["done"]/self.moni["open"]*100,2)}%')
            self.barr = 1000




class IterOps(IterOpBase):
    @property
    def columns(self) -> Iterable[str]:
        return tuple(self.dimm.root.feld.keys())
    @property
    def index(self) -> Iterable[Any]|pd.Index:
        return range(self.dimm.wdat.shape[0])
    @abstractmethod
    def __call__(self, batch: np.ndarray) -> np.ndarray:...

    def RunOper(self, maxw: int = None) -> OubdType:
        return RunOper(maxw)(self)
