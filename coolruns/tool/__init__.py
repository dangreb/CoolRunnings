
import uuid
import warnings

import weakref as wk

from pandas.core.generic import NDFrame, Index
"""
from pandas.core.base import PandasObject
from pandas.core.accessor import Accessor, PandasDelegate, delegate_names
"""

from coolruns.tool._options import opt
from coolruns.tool.meta import MetaConstructor, MetaCaster, Singleton, ObjectDeque, ObjectCatallog

warnings.filterwarnings("ignore", r'\boverriding(.*)preexisting\b',  category=UserWarning, module="pandas.core.accessor")

__all__ = ["HeldUUID", "AcessorPersistor", "MetaConstructor", "MetaCaster", "Singleton", "ObjectDeque", "ObjectCatallog", "opt"]


class HeldUUID:
    __uuid__: uuid.UUID = None
    def __init__(self, _uid: uuid.UUID, /) -> None:
        self.__uuid__ = _uid
    def __copy__(self):
        return None
    def __deepcopy__(self, memo):
        return None
    def __getattr__(self, item):
        return getattr(self.__uuid__, item, None)
    def __setattr__(self, key, value) -> None:
        if key == "__uuid__":
            super().__setattr__(key, value)
        else:
            setattr(self.__uuid__, key, value)
    def __delattr__(self, item) -> None:
        delattr(self.__uuid__, item)
    def __dir__(self) -> list[str]:
        return dir(self.__uuid__)
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.__uuid__}>"
    def __str__(self) -> str:
        return str(self.__uuid__)
    def __hash__(self) -> int:
        return hash(self.__uuid__)
    def __eq__(self, other) -> bool:
        return self.__uuid__ == other
    def __ne__(self, other) -> bool:
        return self.__uuid__ != other
    def __lt__(self, other) -> bool:
        return self.__uuid__ < other
    def __le__(self, other) -> bool:
        return self.__uuid__ <= other
    def __gt__(self, other) -> bool:
        return self.__uuid__ > other
    def __ge__(self, other) -> bool:
        return self.__uuid__ >= other
    def __bool__(self) -> bool:
        return bool(self.__uuid__)
    def __call__(self, *args, **kwargs):
        return self.__uuid__.__call__(*args, **kwargs)
    def __getnewargs__(self):
        return (self.__uuid__,)
    def __getstate__(self):
        return self.__uuid__
    def __setstate__(self, state):
        self.__uuid__ = state


class AcessorPersistor(type):
    __held__: dict[type ,wk.WeakKeyDictionary[str ,object]] = dict()
    @property
    def held(cls) -> wk.WeakKeyDictionary[str ,object]:
        return cls.__held__.get(cls, None) or cls.__held__.setdefault(cls, wk.WeakKeyDictionary())
    @held.setter
    def held(cls, _) -> None: ...
    def __contains__(self, accessor):
        return accessor in self.__held__
    def __call__(cls, pobj: NDFrame|Index, *args, **kwargs):
        auid = pobj.attrs.get(cls, None) or pobj.attrs.setdefault(cls, HeldUUID(uuid.uuid4()))
        aobj = cls.held.get(auid, None) or cls.held.setdefault(auid, super(AcessorPersistor, cls).__call__(pobj, *args, **kwargs))
        wk.finalize(auid, aobj.free)
        return aobj


# TODO:: We need a solution for Dataframe copy propagation to ensure adequate usability!!
"""
@register_dataframe_accessor("attrs")
class AcessorPropagator(metaclass=MetaConstructor):
    def getter(self) -> dict[Hashable,Any]:
        return self._obj._attrs
    def setter(self, value: Mapping[Hashable, Any]) -> None:
        self._obj._attrs = dict(value)
    def __init__(self, pobj: NDFrame|Index, *args, **kwargs):
        self._obj = pobj
        self._obj._attrs.update((akey, self._obj) for akey, aval in self._obj._attrs.items() if aval is None and akey in AcessorPersistor)
    def __call__(self, *args, **kwargs):
        return property(self.getter, self.setter)
"""