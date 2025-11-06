
import gc
import weakref as wk

from typing import Any, ClassVar
from abc import ABCMeta, abstractmethod

import pandas as pd
from pandas.core.base import PandasObject
from pandas.core.accessor import _register_accessor as register_accessor

from coolruns.hooks import HookSentinel, Hook


__all__ = ["PersistentAccessor", "gcollect"]



def gcollect(func=None, /, early=True, late=False):
    def wrapper(fn):
        def _collect_call(*args, **kwargs):
            gc.collect() if early else ...
            resu = fn(*args, **kwargs)
            gc.collect() if late else ...
            return resu
        return _collect_call
    return wrapper(func) if func else wrapper


class UID(Hook):
    __slots__ = ("cls",)
    def __init__(self, cls: type):
        self.cls = cls
    def __repr__(self) -> str:
        return f"{self.cls.target.__module__}.{self.cls.target.__name__}.{self.cls.__name__}.{self.cls.handle}"
    def __deepcopy__(self, memo):
        return next((self for d in memo.values() if isinstance(d, self.cls.target)), None)


class AccessorPersistor(ABCMeta):
    __uid__: UID

    __sentinel__: type[HookSentinel] = HookSentinel
    __catalogue__: dict[type,wk.WeakKeyDictionary[Hook,object]] = dict()

    @property
    def sentinel(cls) -> HookSentinel:
        return cls.__sentinel__()
    @property
    def catalogue(cls) -> wk.WeakKeyDictionary[Hook,object]:
        return cls.__catalogue__.get(cls, None) or cls.__catalogue__.setdefault(cls, wk.WeakKeyDictionary())

    __handle__: str
    __target__: type[PandasObject]

    @property
    def handle(self) -> str:
        return self.__handle__
    @property
    def target(self) -> type[PandasObject]:
        return self.__target__

    def __call__(cls, obj: PandasObject, /, *args, **kwargs):
        hook = obj.attrs.get(cls.__uid__) or obj.attrs.update({cls.__uid__:cls.sentinel(super(AccessorPersistor, cls).__call__(obj=obj, *args, **kwargs))}) or obj.attrs[cls.__uid__]
        inst = cls.catalogue.get(hook) or cls.catalogue.setdefault(hook, cls.sentinel[hook])
        inst.__obj__, inst.__hook__ = wk.ref(obj), wk.ref(hook)
        return inst


class PersistentAccessor(metaclass=AccessorPersistor):

    __obj__: wk.ReferenceType[PandasObject] = None
    __hook__: wk.ReferenceType[Hook] = None

    @property
    def obj(self) -> wk.ReferenceType[PandasObject]:
        return self.__obj__
    @property
    def hook(self) -> wk.ReferenceType[Hook]:
        return self.__hook__


    @classmethod
    def registar(cls, handle: str, target: type[PandasObject]):
        if isinstance(handle, str) and isinstance(handle, str) and issubclass(target, PandasObject):
            cls.__handle__, cls.__target__ = handle, target
            register_accessor(name=handle, cls=target)(cls)
        cls.registar = lambda *_,**__: None

    def __init_subclass__(cls, *, handle: str = None, target: type[PandasObject] = None, sentinel: type[HookSentinel] = HookSentinel, **kwargs):
        cls.__uid__ = UID(cls)
        cls.__sentinel__ = sentinel if issubclass(sentinel, HookSentinel) else HookSentinel
        handle and target and cls.registar(handle=handle, target=target)

    #def __call__(self, *args, **kwargs):...

    @gcollect
    def __deepcopy__(self, memo: dict[int, Any]|None):
        return next((self.__class__(dest).hook() for dest in memo.values() if isinstance(dest,self.__class__.target)))

    def __bool__(self) -> bool:
        return True

