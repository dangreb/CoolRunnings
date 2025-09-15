
import gc
import weakref as wk

from typing import Any
from abc import ABCMeta

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


class AccessorPersistor(ABCMeta):

    __sentinel__: type[HookSentinel] = HookSentinel
    __catalog__: dict[type,wk.WeakKeyDictionary[Hook,object]] = dict()

    @property
    def sentinel(cls) -> HookSentinel:
        return cls.__sentinel__()
    @property
    def catalog(cls) -> wk.WeakKeyDictionary[Hook,object]:
        return cls.__catalog__.get(cls, None) or cls.__catalog__.setdefault(cls, wk.WeakKeyDictionary())

    def __call__(cls, obj: PandasObject, /, *args, **kwargs):
        obj.attrs.get(cls, None) or obj.attrs.update({cls:cls.sentinel(super(AccessorPersistor, cls).__call__(obj, *args, **kwargs)).pop()})
        cls.catalog.get(obj.attrs[cls], None) or cls.catalog.setdefault(obj.attrs[cls], cls.sentinel[obj.attrs[cls]])
        cls.catalog[obj.attrs[cls]].__obj__, cls.catalog[obj.attrs[cls]].__hook__ = wk.ref(obj), wk.ref(obj.attrs[cls])
        return cls.catalog[obj.attrs[cls]]

class PersistentAccessor(metaclass=AccessorPersistor):

    __obj__: PandasObject = None
    __hook__: Hook = None

    @property
    def obj(self) -> wk.ReferenceType[PandasObject]:
        return self.__obj__
    @property
    def hook(self) -> wk.ReferenceType[Hook]:
        return self.__hook__

    __handle__: str
    __target__: type[PandasObject]

    @property
    def handle(self) -> str:
        return self.__handle__
    @property
    def target(self) -> type[PandasObject]:
        return self.__target__

    def __init_subclass__(cls, *, handle: str, target: type[PandasObject], sentinel: type[HookSentinel] = HookSentinel, **kwargs):
        register_accessor(name=handle, cls=target)(cls)
        cls.__sentinel__ = sentinel
        cls.__handle__ = handle
        cls.__target__ = target

    def __deepcopy__(self, memo: dict[int, Any]|None):
        return getattr(next((self.__class__(d) for d in memo.values() if isinstance(d,type(self.obj()))), None), "hook", lambda: None)()

    def __bool__(self) -> bool:
        return True

