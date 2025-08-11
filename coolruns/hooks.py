


import gc
import uuid
import random

from abc import ABCMeta, abstractmethod
from typing import Any, Self, Iterator

from coolruns._typing import wk, wkRef

__all__ = ["HookSentinel", "HookMart", "Hook", "gcollect", "wkRef", "wk", "allonym"]



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



class Hook(frozenset):
    __slots__ = ()
    def __new__(cls, *args, **kwargs):
        return super(Hook, cls).__new__(cls, [uuid.uuid4().bytes], *args, **kwargs)
    def __init__(self):
        super(Hook, self).__init__()
    def __repr__(self) -> str:
        return f'[ {self.__class__.__name__} : {hex(id(self))} ]'
    def __hash__(self) -> int:
        return hash(id(self))
    def __deepcopy__(self, memo):
        pass
    def __class_getitem__(cls, articles: tuple[Any]) -> Iterator[tuple[Self,Any]]:
        yield from ((cls(),article) for article in articles)


class HookMart:
    __hook__: dict[int, Hook]
    def __set_name__(self, owner, name):
        self.__hook__ = dict()
        self.owner = owner
        self.name = name
    def __get__(self, instance, owner) -> Hook:
        return self.__hook__.get(id(instance), None)
    def __set__(self, instance, hook: Hook) -> None:
        self.__hook__.setdefault(id(instance), hook)
    def __delete__(self, instance):
        del self.__hook__[id(instance)]


class HookSentinel(wk.WeakKeyDictionary, metaclass=ABCMeta):

    def __init__(self, *args, **kwargs):
        super(HookSentinel, self).__init__()
    def __call__(self, *articles) -> Iterator[Hook]:
        for hook, article in Hook[articles]:
            self.setdefault(hook, item)
            wk.finalize(hook, self.handler, article=article)
            yield hook
    def __getitem__(self, hook: Hook) -> wk.ProxyType:
        return wk.proxy(super(HookSentinel, self).__getitem__(hook))

    @abstractmethod
    def callback(self, *args, **kwargs):...



if __name__ == "__main__":
    hk = Hook()
    print(hk)