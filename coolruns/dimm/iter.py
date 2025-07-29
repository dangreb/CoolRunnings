
import os
import sys
import numpy as np

from collections import deque
from typing import Any, Iterable

from coolruns.typn import hstr
from coolruns.tool import opt, Singleton
from concurrent.futures import Future, ThreadPoolExecutor



__all__=["IterOps", "CallableDeque"]


class CallableDeque(deque):
    def __init__(self, *args, **kwargs):
        super(CallableDeque, self).__init__(*args, **kwargs)
    def __call__(self, result: np.ndarray, columns: Iterable[hstr]):...



class IterOps(metaclass=Singleton):

    def __init__(self, maxw: int = round(os.cpu_count()*0.75)):
        self.maxw: int = maxw
        self.kept = list()
        self.keep = list()

    def __call__(self, pack: CallableDeque, oper: callable, columns: Iterable[hstr], **kwargs) -> Any:
        with ThreadPoolExecutor(max_workers=self.maxw, thread_name_prefix=self.__class__.__name__) as executor:
            futures: list[Future] = [executor.submit(oper, *(item,), **kwargs) for item in pack]
            pack([f.result() for f in futures], columns=columns)
        return
