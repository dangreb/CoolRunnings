import os

import numpy as np

from collections import deque
from typing import Self, Iterable, Callable, Any

from concurrent.futures import Future, ThreadPoolExecutor

from coolruns._typing import hstr, Singleton
from coolruns.accessor import Dimm


__all__ = ["CallableDeque", "DimmIterator", "IterOps"]


class CallableDeque(deque):
    def __init__(self, *args, **kwargs):
        super(CallableDeque, self).__init__(*args, **kwargs)
    def __call__(self, result: np.ndarray, columns: Iterable[hstr]):...



class DimmIterator(CallableDeque):
        def __init__(self, data: Dimm, batch: int = 1024, stride: int = 1):
            self.dimm: Dimm = data
            self.batch: int = len(self.dimm) if not batch or batch is Ellipsis else batch
            self.stride: int = stride,
            blen = len(self.dimm)//batch
            dlen = blen*batch
            asym = len(self.dimm)-dlen
            super(DimmIterator, self).__init__(([self.dimm.wnds[:asym]] if asym else [])+np.vsplit(self.dimm.wnds[-dlen:], blen))
        def __call__(self, result: np.ndarray, columns: Iterable[str]) -> Self:
            self.dimm(data=np.atleast_2d(np.concatenate(result, axis=0)), columns=columns)
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