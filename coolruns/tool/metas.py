

from abc import ABCMeta
from collections import deque
from typing import Self, Any, Hashable

from coolruns.typn import stack

__all__ = ["ObjectCatallog", "ObjectDeque", "Singleton", "ObjectFILO", "MetaConstructor", "MetaCaster"]




class ObjectCatallog(type):
    __inst__: dict[type,dict[Hashable,object]] = dict()
    @property
    def inst(cls) -> dict[Hashable,object]:
        return cls.__inst__.get(cls, None) or cls.__inst__.setdefault(cls, dict())
    @inst.setter
    def inst(cls, _) -> None: ...
    def __call__(cls, hndl: Hashable = None, /, *args, **kwargs):
        return super(ObjectCatallog, cls).__call__(*args, **kwargs) if hndl is None else (cls.inst.get(hndl, None) or cls.inst.setdefault(hndl, super(ObjectCatallog, cls).__call__(hndl, *args, **kwargs)))
    def __iter__(cls):
        yield from cls.inst.items()
    def __class_getitem__(cls, hndl: Hashable) -> Self:
        return cls.inst.get(hndl, None)
    def __setitem__(cls, hndl: Hashable, value: Self) -> None:
        cls.inst[hndl] = value
    def __delitem__(cls, hndl: Hashable) -> None:
        del cls.inst[hndl]
    def keys(cls):
        return list(cls.__inst__.get(cls,{}).keys())
    def free(cls):
        cls.inst.clear()
        del cls.__inst__[cls]


class ObjectDeque(type):
    __roster__: dict[type, dict[Hashable, deque]] = dict()
    @property
    def roster(cls) -> dict[Hashable, deque]:
        return cls.__roster__.get(cls, None) or cls.__roster__.setdefault(cls, dict())
    @roster.setter
    def roster(cls, _) -> None: ...
    def __call__(cls, hndl: Hashable = None, /, *args, **kwargs):
        odeq = cls.roster.get(hndl, None) or cls.roster.setdefault(hndl, deque())
        odeq.appendleft(super(ObjectDeque, cls).__call__(hndl, *args, **kwargs))
        return odeq[0]
    def __getitem__(self, hndl: Hashable) -> deque:
        return self.roster.get(hndl, None)


class Singleton(ABCMeta):
    __inst__: dict[type,object] = dict()
    def __call__(cls, *args, **kwargs):
        return cls.__inst__.get(cls, None) or cls.__inst__.setdefault(cls, super(Singleton, cls).__call__(*args, **kwargs))


class ObjectFILO(type):
    __stack__: dict[type,stack[Any]] = dict()
    @property
    def __filo__(cls) -> stack[Any]:
        return cls.__stack__.get(cls, None) or cls.__stack__.setdefault(cls, stack())
    @__filo__.setter
    def __filo__(cls, _): ...

    @staticmethod
    def _wrap_enter(func):
        def wrapper(self):
            return func(self.__class__.__filo__())
        return wrapper
    @staticmethod
    def _wrap_exit(func):
        def wrapper(self, *args, **kwargs):
            func(self.__class__.__filo__.pop(), *args, **kwargs)
        return wrapper

    def __new__(cls, name: str, bases: tuple[type,...], attributes: dict[str, Any]):
        if attributes["__init__"].__code__.co_posonlyargcount > 1:
            raise TypeError("Positional-only arguments not allowed for ObjectFILO constructors.")
        attributes["__args__"]: dict[str,Any] = dict()
        attributes["__enter__"] = cls._wrap_enter(attributes.get("__enter__", None) or attributes.setdefault("__enter__", lambda self: None))
        attributes["__exit__"] = cls._wrap_exit(attributes.get("__exit__", None) or attributes.setdefault("__exit__", lambda self, exc_type, exc_val, exc_tb: None))
        return super(ObjectFILO, cls).__new__(cls, name, bases, attributes)

    def __call__(cls, *args, **kwargs):
        _arg = cls.__filo__().__args__ if cls.__filo__ else dict()
        _new = dict(zip(cls.__init__.__code__.co_varnames[:cls.__init__.__code__.co_argcount][1:], args))
        _new.update(kwargs)
        _arg.update(_new)
        inst = super(ObjectFILO, cls).__call__(**_arg)
        inst.__args__ = _arg
        return cls.__filo__.append(inst)()

    def pop(cls) -> Self:
        return cls.__filo__.pop()
    def get(cls) -> Self:
        return cls.__filo__()


class MetaConstructor(type):
    def __call__(cls, *args, **kwargs):
        return super(MetaConstructor, cls).__call__(*args, **kwargs)()


class MetaCaster(type):
    def __call__(cls, instance, /, **kwargs):
        instance.__class__ = cls
        instance.__call__(**kwargs) if hasattr(instance, "__call__") else ...
        return instance