
import os

import numpy as np
import pandas as pd

from string import ascii_lowercase
from threading import Thread, RLock

from abc import ABCMeta, abstractmethod
from typing import Self, Mapping, Sequence, Callable, MutableSequence

from concurrent.futures import ThreadPoolExecutor

__void__ = lambda *_,**__: None


__all__ = ["IterOps", "RunOper"]




class IterOps(metaclass=ABCMeta):
    __wrapper__: Callable = None

    def __init__(self, data: np.ndarray, blen: int = None, outcol: Sequence[str] = (), callback: Callable[[Self],None] = __void__):
        self.data, self.blen, self.dlen, self.callback = data, blen or data.shape[0], data.shape[0]//blen, callback
        sample = self(self.data[:8], slice(0,8))
        if isinstance(sample, np.ndarray):
            IterOps.__wrapper__ = self.__array_result__
        elif isinstance(sample, Sequence):
            IterOps.__wrapper__ = self.__list_result__
            sample = np.concat(sample, axis=-1)
        else:
            IterOps.__wrapper__ = self.__ignore_result__
            return
        nclmns = sample.shape[-1]
        self.result = np.empty(self.data.shape[:-1]+(nclmns,))
        self.outcol = (tuple(outcol)+tuple(f'{self.__class__.__name__.lower()}_{char}' for char in ascii_lowercase[:max(nclmns-len(outcol),0)]))[:nclmns]
        self.__class__.__call__ = self.__class__.__redecor__(self.__class__.__call__)

    def __len__(self) -> int:
        return self.dlen

    def __iter__(self) -> slice:
        yield from (slice(idex,idex+self.blen) for idex in range(0, self.data.shape[0], self.blen))

    @classmethod
    def make(cls, name: str, call: Callable[[np.ndarray,slice,...],np.ndarray|Sequence[np.ndarray]|Mapping[str,np.ndarray]|pd.DataFrame|pd.Series]):
        type(name or "IterOpLmbd", )

    def __ignore_result__(self, result: np.ndarray = None, slicer: slice = slice(None), /, **kwargs):...

    def __array_result__(self, result: np.ndarray = None, slicer: slice = slice(None), /, **kwargs):
        self.result[slicer] = result[..., :self.result.shape[-1]]
        return self.result[slicer]

    def __list_result__(self, result: Sequence[np.ndarray] = None, slicer: slice = slice(None), /, **kwargs):
        for idex in (idex for idex in range(self.result.shape[-1]) if idex < len(result)):
            self.result[slicer, ..., idex] = result[idex]
        return self.result[slicer]

    @staticmethod
    def __redecor__(func: Callable) -> Callable:
        def wrapper(self, slicer: slice, /, **kwargs):
            return IterOps.__wrapper__(func(self, data=self.data[slicer], slicer=slicer, **kwargs), slicer, **kwargs)
        return wrapper

    @__redecor__
    @abstractmethod
    def __call__(self, data: np.ndarray, slicer: slice, **kwargs) -> np.ndarray:...



class Tether(ABCMeta, type(Thread)):
    __instance__: Thread = None
    #__fetchlock__: Event = Event()
    __fetchlock__: RLock = RLock()
    __mxworkers__: int = os.cpu_count()*0.75
    __iteropers__: MutableSequence[IterOps] = list()
    def __call__(cls, *iterops: IterOps, workers: int = None, **kwargs):
        cls.__mxworkers__ = max(2,workers or cls.__mxworkers__)
        cls.__iteropers__.extend(list(iterops))
        cls.__instance__ = cls.__instance__ or super(Tether, cls).__call__(*iterops, **kwargs)
        return cls.__instance__.run()
        #return cls.__instance__.start()



class RunOper(Thread, metaclass=Tether):
    @property
    def fetchlock(self) -> RLock:
        return self.__class__.__fetchlock__
    @property
    def maxworkers(self) -> int:
        return self.__class__.__mxworkers__
    @property
    def iteropers(self) -> MutableSequence[IterOps]:
        self.fetchlock.acquire(timeout=2)
        self.fetchlock.release()
        return self.__class__.__iteropers__
    @iteropers.deleter
    def iteropers(self):
        self.__class__.__iteropers__ = list()
    def __init__(self, *args, **kwargs):
        super(RunOper, self).__init__(target=self, name=self.__class__.__name__, daemon=False)
    def __bool__(self) -> bool:
        return self.is_alive()
    def __iter__(self):
        while self.iteropers:
            with self.fetchlock:
                iteropers = tuple(self.iteropers)
                del self.iteropers
            yield from iteropers
    def __call__(self, *args, **kwargs):
        for iterop in self:
            with ThreadPoolExecutor(max_workers=self.maxworkers-1, thread_name_prefix=self.name) as runner:
                runner.map(iterop, iterop)
            iterop.callback(iterop=iterop)

        self.free()
    def start(self) -> Self:
        self._started.is_set() or super(RunOper, self).start()
        return self
    def free(self, *args, **kwargs):
        """ palestine """
        self.__class__.__instance__ = None





