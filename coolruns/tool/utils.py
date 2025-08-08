import dataclasses
import gc
import uuid
import copy
import random
import functools

from typing import Any, Self, FrozenSet, Callable, Generic, Hashable, Iterable, Iterator, KeysView, MutableMapping

from coolruns.typn import wk, wkRef

__all__ = ["LifetimeSentinel", "LifetimeHook","gcollect", "wkRef", "wk", "allonym"]




def allonym() -> str:
    yield random.shuffle([]) #TODO::
    pass

def gcollect(func=None, /, early=True, late=False):
    def wrapper(fn):
        def _collect_call(*args, **kwargs):
            gc.collect() if early else ...
            resu = fn(*args, **kwargs)
            gc.collect() if late else ...
            return resu
        return _collect_call
    return wrapper(func) if func else wrapper


class LifetimeHook(frozenset):
    __slots__ = ()
    def __new__(cls, *args, **kwargs):
        return super(LifetimeHook, cls).__new__(cls, [uuid.uuid4().bytes], *args, **kwargs)
    def __init__(self):
        super(LifetimeHook, self).__init__()
    def __repr__(self) -> str:
        return f'[ {self.__class__.__name__} : {hex(id(self))} ]'
    def __hash__(self) -> int:
        return hash(id(self))
    def __deepcopy__(self, memo):
        pass
    def __class_getitem__(cls, articles: tuple[Any]) -> Iterator[tuple[Self,Any]]:
        yield from ((cls(),article) for article in articles)


class LifetimeSentinel(wk.WeakKeyDictionary):
    def __init__(self, *args, **kwargs):
        super(LifetimeSentinel, self).__init__()
    def __call__(self, *articles) -> Iterator[LifetimeHook]:
        for hook, item in LifetimeHook[articles]:
            self.setdefault(hook, item)
            yield hook
    def __getitem__(self, hook: LifetimeHook) -> wk.ProxyType:
        return wk.proxy(super(LifetimeSentinel, self).__getitem__(hook))
    def handler(self, **kwargs):
        pass

if __name__ == "__main__":
    hook = LifetimeHook()
    print(hook)