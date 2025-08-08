
import gc
import uuid
import copy

import functools

from abc import ABCMeta
from typing import ClassVar, Self, Callable

from pandas.core.generic import NDFrame, Index


from coolruns.typn import wk, wkRef
from coolruns.tool.utils import LifetimeHook, gcollect
from coolruns.tool._pydev import PyDBOps

_void = lambda *_,**__: None
_tame = lambda *_,**__: True
_wild = lambda *_,**__: False


__all__ = ["AccessorPersistor"]



class AccessorPersistor(ABCMeta):
    __held__: dict[type ,wk.WeakKeyDictionary[str ,object]] = dict()
    @property
    def held(cls) -> wk.WeakKeyDictionary[str ,object]:
        return cls.__held__.get(cls, None) or cls.__held__.setdefault(cls, wk.WeakKeyDictionary())

    @gcollect
    def __call__(cls, pobj: NDFrame|Index, *args, **kwargs):
        #PyDBOps()()
        pobj.attrs.get(cls, None) or pobj.attrs.update({cls:LifetimeHook()})
        cls.held.get(pobj.attrs[cls], None) or cls.held.update({pobj.attrs[cls]:super(AccessorPersistor, cls).__call__(*args, **kwargs)})
        return cls.held[pobj.attrs[cls]].__complete__(pobj)

    @staticmethod
    def __complete__(func: Callable):
        @functools.wraps(func)
        def wrapper(self, pobj: NDFrame|Index):
            if not self.pobj:
                func(self, pobj)
                self.pobj = wk.ref(pobj)
                wk.finalize(pobj.attrs[self.__class__], self.__free__)
            return self
        return wrapper

    def __new__(cls, name, bases, attributes, **kwargs):
        attributes["pobj"] = None
        attributes["__free__"] = cls.__free__(attributes.get("__free__", _void))
        attributes["__bool__"] = cls.__bool(attributes.get("__bool__", _tame))
        #attributes["__deepcopy__"] = cls.copy(attributes.get("__copy__", copy.copy))
        attributes["__complete__"] = cls.__complete__(attributes.get("__complete__", _void))
        return super(AccessorPersistor, cls).__new__(cls, name, bases, attributes)


    @staticmethod
    def __free__(func: Callable):
        """ palestine """
        @functools.wraps(func)
        def wrapper(self):
            [setattr(self, "pobj", None), func(self)].pop()
        return wrapper
    @staticmethod
    def copy(func: Callable):
        def wrapper(self, memo):
            #return [cp for _,_,cp in [(memo.update({list(memo)[-1]:cp}),setattr(cp, "pobj", None), cp) for cp in [func(self)]]].pop()
            return [cp for _,cp in [(setattr(cp, "pobj", None), cp) for cp in [func(self)]]].pop()
        return wrapper
    @staticmethod
    def __bool(func: Callable):
        def wrapper(self):
            return self.pobj is not None and bool(func(self))
        return wrapper