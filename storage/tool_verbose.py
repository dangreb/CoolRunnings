
import gc
import uuid
import copy
import warnings

import weakref as wk
from abc import ABCMeta, abstractmethod
from typing import ClassVar

from pandas.core.generic import NDFrame, Index
"""
from pandas.core.base import PandasObject
from pandas.core.accessor import Accessor, PandasDelegate, delegate_names
"""

from coolruns.typn import idict
from coolruns.tool._options import opt
from coolruns.tool.meta import MetaConstructor, MetaCaster, Singleton, ObjectDeque, ObjectCatallog

warnings.filterwarnings("ignore", r'\boverriding(.*)preexisting\b',  category=UserWarning, module="pandas.core.accessor")

__all__ = ["HeldUUID", "AcessorPersistor", "MetaConstructor", "MetaCaster", "Singleton", "ObjectDeque", "ObjectCatallog", "opt"]

asdict = lambda _dbg:dict(
    (vnam, str(vval) if vnam not in {"py_db_command_thread"} else dict(
        (vn, str(vv)) for vn, vv in dict(vars(vval)).items() if len(str(vv)) < 512
    )) for vnam, vval in dict(vars(_dbg)).items() if vnam in {"py_db_command_thread", "value_resolve_thread_list"} or len(str(vval)) < 256)

class HeldUUID:
    __pdbg__: ClassVar[dict] = dict()

    __uuid__: uuid.UUID = None
    def __init__(self) -> None:
        self.__uuid__ = uuid.uuid4()
    def __copy__(self):
        return self
    def __deepcopy__(self, memo):
        _accs = ([None]+[(k,v[self]) for k, v in AcessorPersistor.__held__.items() if self in v and v[self] is not None]).pop()
        import json
        import inspect
        import hashlib
        _dbg = ([None]+list(filter(bool, [dict(s.frame.f_locals).get("debugger", None) for s in inspect.stack()]))).pop()
        if not _accs:
            return self
        """
        if _dbg is not None:
            _dbg = asdict(_dbg)
            HeldUUID.__pdbg__.update({hashlib.md5(str(_dbg).encode("utf-8")).digest().hex():_dbg})
        #import inspect
        #print(f'HeldUUID.{self}.{_accs}')
        #print("\n".join([f'{s.filename.split("\\")[-1]} : {s.lineno}' for s in inspect.stack() if True or s.filename.find("CoolRunnings") >= 0]))
        #print("\n")
        import os
        print("\n")
        print(os.environ["PYTHONPATH"].replace(";", "\n"))
        print("\n")
        """
        _uuid = HeldUUID()
        HeldUUID.__pdbg__.update({len(HeldUUID.__pdbg__):_dbg.value_resolve_thread_list})
        AcessorPersistor.__held__[_accs[0]].setdefault(_uuid, copy.copy(_accs[-1]))
        return _uuid
    def __getattr__(self, item):
        _uid =  getattr(self.__uuid__, item, None)
        return _uid
    def __setattr__(self, key, value) -> None:
        if key == "__uuid__":
            super().__setattr__(key, value)
        else:
            setattr(self.__uuid__, key, value)
        pass
    def __delattr__(self, item) -> None:
        delattr(self.__uuid__, item)
        pass

class AcessorPersistor(type):
    __held__: dict[type ,wk.WeakKeyDictionary[str ,object]] = dict()
    @property
    def held(cls) -> wk.WeakKeyDictionary[str ,object]:
        gc.collect()
        return cls.__held__.get(cls, None) or cls.__held__.setdefault(cls, wk.WeakKeyDictionary())
    @held.setter
    def held(cls, _) -> None: ...
    def __contains__(cls, accessor):
        return accessor in cls.held
    def __new__(cls, name, bases, attributes):
        if "__fix__" not in attributes:
            attributes["__fix__"] = lambda self, pobj: self
        return super(AcessorPersistor, cls).__new__(cls, name, bases, attributes)
    def __call__(cls, pobj: NDFrame|Index, *args, **kwargs):
        pass
        pobj.attrs.get(cls, None) or pobj.attrs.update({cls:HeldUUID()})

        aobj = (cls.held.get(pobj.attrs[cls], None) or cls.held.setdefault(pobj.attrs[cls], super(AcessorPersistor, cls).__call__(pobj, *args, **kwargs))).__fix__(pobj)
        wk.finalize(pobj.attrs[cls], aobj.free)
        return aobj



#TODO:: We need a solution for Dataframe copy propagation to ensure adequate usability!!
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