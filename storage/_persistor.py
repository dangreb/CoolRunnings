
import gc
import uuid
import copy

import weakref as wk
from typing import ClassVar, Self, Callable

import pandas as pd
from pandas.core.generic import NDFrame, Index
from pandas.core.accessor import PandasDelegate, delegate_names

_void = lambda *_,**__: None

__all__ = ["HeldUUID", "AccessorPersistor", "AccessorPropagator"]


class HeldUUID:
    __hcls__: type = None
    __weak__: type = list()
    __uuid__: uuid.UUID = None
    def __init__(self, hcls: type, /) -> None:
        self.__hcls__ = hcls
        HeldUUID.__weak__.append(wk.ref(self))
        self.__uuid__ = uuid.uuid4()
    def __hash__(self) -> int:
        return hash(self.__uuid__)
    def __getattr__(self, item):
        return getattr(self.__uuid__, None) if hasattr(self.__uuid__, item) else self.__getattribute__(item)
    def __copy__(self):
        return self
    def __update__(self, huid: Self):
        self.__hcls__.held.update({huid:copy.deepcopy(self.__hcls__.held[self])})
        return huid
    def __deepcopy__(self, memo):
        gc.collect()
        return self.__update__(HeldUUID(self.__hcls__)) if self.__hcls__.held.get(self, None) else None


class AccessorPersistor(type):

    __held__: dict[type ,wk.WeakKeyDictionary[str ,object]] = dict()
    @property
    def held(cls) -> wk.WeakKeyDictionary[str ,object]:
        return cls.__held__.get(cls, None) or cls.__held__.setdefault(cls, wk.WeakKeyDictionary())
    @held.setter
    def held(cls, _) -> None: ...

    def __call__(cls, pobj: NDFrame|Index, *args, **kwargs):
        gc.collect()
        print(pobj)
        pobj.attrs.get(cls, None) or pobj.attrs.update({cls:HeldUUID(cls)})
        cls.held.get(pobj.attrs[cls], None) or cls.held.update({pobj.attrs[cls]:super(AccessorPersistor, cls).__call__(*args, **kwargs)})
        return cls.held[pobj.attrs[cls]].__complete__(pobj)

    @staticmethod
    def __complete__(func: callable):
        def wrapper(self, pobj: NDFrame|Index):
            if not self or self.pobj() is not pobj:
                print(f'{self}.{self.pobj}')
                print(f'{self}.{self.pobj}')
                self.pobj = wk.ref(pobj)
                func(self, pobj)
                wk.finalize(pobj.attrs[self.__class__], self.free)
            return self
        return wrapper

    def __new__(cls, name, bases, attributes):
        attributes["pobj"] = None
        attributes["free"] = cls.free(attributes.get("free", _void))
        attributes["__bool__"] = cls.__bool__(attributes.get("__bool__", id))
        attributes["__deepcopy__"] = cls.__copier__(attributes.get("__copy__", copy.copy))
        attributes["__complete__"] = cls.__complete__(attributes.get("__complete__", _void))
        pass
        return super(AccessorPersistor, cls).__new__(cls, name, bases, attributes)

    @staticmethod
    def free(func: callable):
        def wrapper(self):
            [setattr(self, "pobj", None), func(self)].pop()
        return wrapper
    @staticmethod
    def __bool__(func: callable):
        def wrapper(self):
            return self.pobj is not None and bool(func(self))
        return wrapper
    @staticmethod
    def __copier__(func: callable):
        def wrapper(self, memo):
            _out = [cp for _,cp in [(setattr(cp, "pobj", None), cp) for cp in [func(self)]]].pop()
            return _out
        return wrapper
