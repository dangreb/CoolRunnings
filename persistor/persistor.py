
import weakref as wk

from typing import Any

from pandas.core.base import PandasObject

from pandas.core.accessor import _register_accessor

from _persistor.hooks import HookSentinel, Hook

_void = lambda *_,**__: None
_tame = lambda *_,**__: True
_wild = lambda *_,**__: False

__all__ = ["AccessorPersistor", "PersistentAccessor"]


class AccessorPersistor(type):
    __obj__: PandasObject = None
    __hook__: Hook = None

    @property
    def obj(self) -> PandasObject:
        return self.__obj__()
    @property
    def hook(self) -> Hook:
        return self.__hook__()

    __sentinel__: type[HookSentinel] = HookSentinel
    __catalog__: dict[type,wk.WeakKeyDictionary[Hook,object]]

    @property
    def sentinel(cls) -> HookSentinel:
        return cls.__sentinel__()
    @property
    def catalog(cls) -> wk.WeakKeyDictionary[Hook,object]:
        return cls.__catalog__.get(cls, None) or cls.__catalog__.setdefault(cls, wk.WeakKeyDictionary)

    def __call__(cls, obj: PandasObject = None, /, *args, **kwargs):
        obj.attrs.get(cls, None) or obj.attrs.setdefault(cls, cls.sentinel(super(AccessorPersistor, cls).__call__(obj, *args, **kwargs)))
        cls.catalog.get(obj.attrs[cls], None) or cls.catalog.setdefault(obj.attrs[cls], cls.sentinel[obj.attrs[cls]])
        cls.catalog[obj.attrs[cls]].__obj__, cls.catalog[obj.attrs[cls]].__hook__ = obj, obj.attrs[cls]
        return cls.catalog[obj.attrs[cls]]


"""
class TargetAttrs:
    def __init__(self, accessor: type[Self], *args, **kwargs):
        self.accessor = accessor
        self.attrs = dict()
    def __get__(self) -> Mapping[Hashable, Any]:
        return self.attrs()
    def __set__(self, value: Mapping[Hashable, Any]) -> None:
        self.attrs = dict(value)
"""

class PersistentAccessor(metaclass=AccessorPersistor):
    """ """
    __handle__: str
    __target__: type[PandasObject]
    def handle(self) -> str:
        return __handle__
    def target(self) -> type[PandasObject]:
        return __target__

    def __init_subclass__(cls, *, handle: str, target: type[PandasObject], sentinel: type[HookSentinel] = HookSentinel, **kwargs):
        #hasattr(cls, "attrs") or stattr(cls, "attrs", TargetAttrs(cls))
        _register_accessor(name=handle, cls=target)(cls)
        cls.__sentinel__ = sentinel
        cls.__handle__ = handle
        cls.__target__ = target

    def __deepcopy__(self, memo: dict[int, Any]|None):
        return getattr(self.__class__(([None]+list(filter(lambda x: isinstance(x,PandasObject), memo.values()))).pop()), "hook", None)

    def __bool__(self) -> bool:
        return True

