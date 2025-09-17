
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
    __catalog__: dict[type,wk.WeakKeyDictionary[Hook,object]] = dict()

    @property
    def sentinel(cls) -> HookSentinel:
        return cls.__sentinel__()
    @property
    def catalog(cls) -> wk.WeakKeyDictionary[Hook,object]:
        return cls.__catalog__.get(cls, None) or cls.__catalog__.setdefault(cls, wk.WeakKeyDictionary())

    __handle__: str
    __target__: type[PandasObject]

    @property
    def handle(self) -> str:
        return self.__handle__
    @property
    def target(self) -> type[PandasObject]:
        return self.__target__

    def __call__(cls, obj: PandasObject, /, *args, **kwargs):
        obj.attrs.get(cls.__uid__, None) or obj.attrs.update({cls.__uid__:cls.sentinel(super(AccessorPersistor, cls).__call__(obj, *args, **kwargs)).pop()})
        cls.catalog.get(obj.attrs[cls.__uid__], None) or cls.catalog.setdefault(obj.attrs[cls.__uid__], cls.sentinel[obj.attrs[cls.__uid__]])
        cls.catalog[obj.attrs[cls.__uid__]].__obj__, cls.catalog[obj.attrs[cls.__uid__]].__hook__ = wk.ref(obj), wk.ref(obj.attrs[cls.__uid__])
        return cls.catalog[obj.attrs[cls.__uid__]]

class PersistentAccessor(metaclass=AccessorPersistor):

    __obj__: wk.ReferenceType[PandasObject] = None
    __hook__: wk.ReferenceType[Hook] = None

    @property
    def obj(self) -> wk.ReferenceType[PandasObject]:
        return self.__obj__
    @property
    def hook(self) -> wk.ReferenceType[Hook]:
        return self.__hook__

    def __init_subclass__(cls, *, handle: str = None, sentinel: type[HookSentinel] = HookSentinel, target: type[PandasObject] = PandasObject, **kwargs):
        cls.__uid__ = UID(cls)
        cls.__handle__ = handle
        cls.__target__ = target
        cls.__sentinel__ = sentinel
        handle and register_accessor(name=handle, cls=target)(cls)

    def __call__(self, *args, **kwargs):...

    def __deepcopy__(self, memo: dict[int, Any]|None):
        return getattr(next((self.__class__(d) for d in memo.values() if isinstance(d,self.__class__.target)), None), "hook", lambda: None)()

    def __bool__(self) -> bool:
        return True

