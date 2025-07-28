
import time

import uuid
import queue
import threading

import numpy as np
import pandas as pd

from enum import Enum
from typing import Iterable
from dataclasses import dataclass, field

from numpy.lib.stride_tricks import as_strided

from coolruns.dimm.dimm import Dimm
from coolruns.typn import hstr, idict, typedictclass
from coolruns.tool import opt, Singleton, ObjectCatallog


__all__=["DimmIterator", "IterOps"]




class IterOpsRepo(type):
    __iops__: dict[int,threading.Thread] = dict()
    def __call__(cls, *args, **kwargs):
        inst = super(IterOpsCatalog, cls).__call__(*args, **kwargs)
        cls.__iops__[hash(inst)] = inst
        return inst
    def __class_getitem__(cls, hndl: int):
        return cls.__iops__.get(hndl, None)
    def __contains__(cls, hndl: int):
        return hndl in cls.__iops__


class IterOps(threading.Thread, metaclass=ObjectCatallog):
    @property
    def data(self):
        return self.__data__
    @data.setter
    def data(self, _):...

    def __init__(self, dimm: Dimm, data: np.ndarray, *args, **kwargs):
        super(IterOps, self).__init__(target=self, name=str(hash(self)), daemon=False, args=args, kwargs=kwargs)
        self._dimm = dimm
        self._data = data
        self.dimm = dimm
        self.__data__ = data

    def __bool__(self):
        return True
    def __hash__(self):
        return id(self)

    @staticmethod
    def wrapcall(func: callable) -> callable:
        def wrapper(self, *args, **kwargs):
            rdata, rcolm = func(self, *args, **kwargs)
            rdata = np.asarray(rdata)
            if rdata is None or not rdata.size:
                return None
            else:
                if rdata.ndim <= 1:
                    rdata = np.atleast_2d(rdata)
                if rdata.ndim > 2:
                    rdata = rdata.reshape((rdata.shape[0], -1))
                if 1 < rdata.shape[0] < self.dimm.wlen:
                    rdata = rdata.mean(axis=0)[...,None]
                if rdata.shape[0] == 1:
                    rdata = as_strided(rdata, shape=(self.dimm.wlen,rdata.shape[-1]), strides=(self.dimm.wlen,rdata.shape[0]))
                if len(rcolm) > rdata.shape[-1]:
                    rcolm = rcolm[:rdata.shape[-1]]
                if len(rcolm) < rdata.shape[-1]:
                    rcolm.extend([f'IterOp_{self.__class__.__name__}_{i}' for i in range(rdata.shape[-1]-len(rcolm))])
                #TODO:: Should be Better
                self.dimm(data=rdata, columns=rcolm)
                return rcolm, rdata
        return wrapper

    @wrapcall
    def __call__(self, *args, **kwargs) -> tuple[np.ndarray,list[str]|tuple[str]]:
        """ Meeant to be Subclassed
            :param args:
            :param kwargs:
            :return:
        """
        return self.data.mean(0), [f'mean_feld_{i:04}' for i in range(len(self.data.shape[-1]))]



class DimmIterator:
    __step__: int = 0
    @property
    def step(self):
        return self.__step__
    @step.setter
    def step(self, value: int):
        self.__step__ = value

    def __init__(self, dimm: Dimm, iops: type = IterOps, batch: int = 256, stride: int = 1, feld: Iterable[hstr] = Ellipsis):
        self.dimm, self.iops, self.batch, self.stride, self.columns, self.symetry = dimm, iops, batch or 1, stride, list(hstr(feld)), (len(dimm)//batch)*batch

    def __iter__(self) -> np.ndarray:
        pass
        for dlot in [self.dimm.wdat[0:len(self.dimm) - self.symetry]]+np.vsplit(self.dimm.wdat[-self.symetry:], len(self.dimm)//self.batch):
            yield hash(self.iops(dimm=self, data=dlot))

    def __hash__(self):
        return id(self)



class IterOpsBroker(threading.Thread):
    def __init__(self, hook: threading.Event, *args, **kwargs):
        super(IterOpsBroker, self).__init__(target=self, name="CoolRunnings.IterOpsBroker", daemon=False, *args, **kwargs)
        self.hook = hook

    def __call__(self, *args, **kwargs):
        while True:
            opid = MasterQueue.get()
            if self.hook.is_set() and opid is None:
                break
            if opid is not None and opid in IterOpsRepo:
                IterOpsRepo(opid).start()
                MasterQueue.task_done()



class QueueBroker(threading.Thread):
    __roster__: dict[int, DimmIterator] = dict()

    def __init__(self, *args, **kwargs):
        super(QueueBroker, self).__init__(target=self, name="CoolRunnings.QueueBroker", daemon=False, *args, **kwargs)
        self.quit = threading.Event()
        self.iobk: IterOpsBroker = IterOpsBroker(self.quit)

    def enrol(self, iterator: DimmIterator) -> threading.Lock:
        self.__roster__[hash(iterator)] = dict(inst=iterator, lock=threading.Lock())
        return self.__roster__[id(iterator)]["lock"]

    def ravel(self, iterator: DimmIterator):
        self.__roster__.pop(id(iterator))

    def __iter__(self):
        for dimm in [dimm for uniq,dimm in self.__roster__]:
            self.ravel(dimm)
            yield from dimm

    def __call__(self, *args, **kwargs):
        self.iobk.start()
        while self:
            [MasterQueue().put(opid) for opid in self if opid in IterOpsRepo and time.sleep(opt.threading_queue_loop_lag)]
        self.quit.set()

    def __bool__(self) -> bool:
        return bool(len(self.__roster__))



class MasterQueue(queue.Queue, metaclass=Singleton):
    def __init__(self):
        super().__init__(maxsize=opt.max_parallel_threads)



