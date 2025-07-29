
import os
import numpy as np

from coolruns.tool import opt, Singleton
from concurrent.futures import Future, ThreadPoolExecutor


__all__=["IterOps"]


class IterOps(metaclass=Singleton):

    def __init__(self, maxw: int = round(os.cpu_count()*0.75)):
        self.maxw: int = maxw

    def __call__(self, pack: deque[Iterables], oper: Callable[[Any], Any], name: str = str(), **kwargs) -> Any:
        with ThreadPoolExecutor(max_workers=self.maxw, thread_name_prefix=name,) as executor:
            futures = [executor.submit(oper, *tuple(item), **kwargs) for item in pack]
            pack([f.result() for f in futures])
        return

    def handler(self):
        pass
