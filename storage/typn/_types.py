

import functools

import weakref as wk

from collections import OrderedDict
from abc import ABCMeta, abstractmethod

from collections.abc import Callable
from typing import (
    get_type_hints, Union, get_origin, get_args, Type, overload, Protocol, Self, MutableMapping,
    SupportsIndex, Hashable, Iterable, Any, Optional, runtime_checkable, Iterator, Reversible
)

__all__ = ["wk", "wkRef", "HashIter", "roll", "stack", "idict", "hstr", "typedictclass", "Chain", "chain", "Singleton"]


def wkrepr(func: Callable):
    @functools.wraps(func)
    def wrapper(self):
        othr = self()
        return " ".join([f'[ {self.__class__.__name__} : {hex(id(self))} ]', f'[ {othr.__class__.__name__} : {hex(id(othr))} ]', f'( {", ".join(map(str, othr.shape))} )' if hasattr(othr, "shape") else ''])
    return wrapper

class wkRef(wk.ReferenceType):
    @wkrepr
    def __repr__(self) -> str:
        return super(wkReferenceType, self).__repr__()

wk.ref = wkRef

def typedictclass(cls: Type, *, coerce: bool = False, strict: bool = True):
    """
    Create a type-checkable pseudo-class for validating dictionaries.
    This enables isinstance(dict, MyTypedDict) to return True only if:
    - The dict contains exactly the declared keys (or more, if strict=False)
    - The values match the expected types (with optional coercion)

    Arguments:
    - coerce: If True, will attempt to coerce values into expected types
    - strict: If True, keys must match exactly. If False, additional keys are allowed

    The class will also have a `.validate(dict)` method for runtime enforcement.
    """

    expected_fields = get_type_hints(cls)

    def _check_type(value: Any, expected_type: Any) -> bool:
        # Handle Any
        if expected_type is Any:
            return True

        origin = get_origin(expected_type)
        args = get_args(expected_type)

        # Handle Optional[X] (which is Union[X, None])
        if origin is Union and type(None) in args:
            remaining = tuple(a for a in args if a is not type(None))
            return value is None or isinstance(value, remaining)

        # Handle Union[A, B, C...]
        if origin is Union:
            return any(isinstance(value, arg) for arg in args)

        return isinstance(value, expected_type)

    def _coerce_value(value: Any, expected_type: Any) -> Any:
        origin = get_origin(expected_type)
        args = get_args(expected_type)

        if expected_type is Any:
            return value

        # Coerce for Optional[X] or Union[X, Y, ...]
        if origin is Union:
            last_error = None
            for arg in args:
                try:
                    if value is None and arg is type(None):
                        return None
                    return _coerce_value(value, arg)
                except Exception as e:
                    last_error = e
            raise last_error or ValueError(f"Cannot coerce value '{value}' into {expected_type}")

        # Normal coercion
        return expected_type(value)

    class TypedDictMeta(type):

        def __instancecheck__(self, instance: Any) -> bool:
            if not isinstance(instance, dict):
                return False

            if strict and set(instance.keys()) != set(expected_fields.keys()):
                return False
            if not strict and not set(expected_fields.keys()).issubset(instance.keys()):
                return False

            for key, expected_type in expected_fields.items():
                if key not in instance:
                    return False
                value = instance[key]
                if not _check_type(value, expected_type):
                    if not coerce:
                        return False
                    try:
                        _coerce_value(value, expected_type)
                    except Exception:
                        return False

            return True

        def validate(cls, obj: dict) -> dict:
            """
            Validates and optionally coerces a dictionary to match the declared schema.

            Raises:
                TypeError: if types are incompatible and coercion fails
                KeyError: if a required key is missing
            Returns:
                A new dictionary matching the expected types
            """
            if not isinstance(obj, dict):
                raise TypeError("Only dictionaries can be validated.")

            result = {}
            for key, expected_type in expected_fields.items():
                if key not in obj:
                    raise KeyError(f"Missing required key: '{key}'")
                value = obj[key]
                if _check_type(value, expected_type):
                    result[key] = value
                elif coerce:
                    result[key] = _coerce_value(value, expected_type)
                else:
                    raise TypeError(f"Incorrect type for key '{key}': expected {expected_type}, got {type(value)}")
            return result

    return TypedDictMeta(cls.__name__, (object,), dict(__annotations__=expected_fields))

@runtime_checkable
class HashIter(Hashable, Iterable, Protocol, metaclass=ABCMeta):

    @abstractmethod
    def __hash__(self):
        return 0
    @abstractmethod
    def __iter__(self):
        while False or True:
            yield None


class idict(OrderedDict):
    def __getitem__(self, item):
        if isinstance(item, int):
            itms = list(self.keys())
            return self.get(itms[item], None) if itms and item < len(itms) else None
        return super(idict, self).__getitem__(item)
    def __getattr__(self, item):
        return self[item] if item in self else None  # super(idict, self).__getattribute__(item)
    def __len__(self) -> int:
        return len(self.keys())
    def __iter__(self):
        yield from self.items()
    def __reversed__(self):
        yield from reversed(self.items())



class Catalog(MutableMapping, metaclass=ABCMeta):
    __catalog__: MutableMapping[Hashable, Self]
    def __class_getitem__(cls, item:Hashable) -> Self:
        return cls.__catalog__.get(item, None)
    def __init_subclass__(cls, maptype: type[MutableMapping] = dict, **kwargs):
        [setattr(cls, *args) for args in kwargs.items()]
        cls.__catalog__ = maptype()
    @abstractmethod
    def __init__(self, item:Hashable, *args, **kwargs):
        raise NotImplementedError
    def __delitem__(self, key, /):
        del self.__catalog__[key]
    def __getitem__(self, key, /):
        return self.__catalog__.get(key, defaults)
    def __setitem__(self, key, value, /):
        self.__catalog__[key] = value
    def __len__(self):
        len(self.__catalog__)
    def __iter__(self):
        yield from self.__catalog__.items()


class Singleton(ABCMeta):
    __inst__: dict[type,object] = dict()
    def __call__(cls, *args, **kwargs):
        return cls.__inst__.get(cls, None) or cls.__inst__.setdefault(cls, super(Singleton, cls).__call__(*args, **kwargs))


class PrevNext(Iterator, Reversible, metaclass=ABCMeta):
    def __init_subclass__(cls, **kwargs):
        [setattr(cls, *args) for args in kwargs.items()]



class Chain(PrevNext):
    __slots__ = ("alias", "__mapp__", "__last__", "__root__", "__step__", "__child__", "__parent__")
    @property
    @abstractmethod
    def last(self) -> Self: ...
    @property
    @abstractmethod
    def root(self) -> Self: ...
    @property
    @abstractmethod
    def step(self) -> Self: ...
    @property
    @abstractmethod
    def child(self) -> Self: ...
    @property
    @abstractmethod
    def parent(self) -> Self: ...

    def __next__(self):
        return self.child
    def __iter__(self):
        knot = self
        while knot:
            yield knot
            knot = knot.child
    def __reversed__(self):
        knot = self
        while knot:
            yield knot
            knot = knot.parent
    def __getitem__(self, item: str|int):
        return self.__mapp__.get(item, None) or self.__mapp__.get(self.last.step+item+1 if isinstance(item, int) and item < 0 else item, None)

    def __init_subclass__(cls, **kwargs):
        [setattr(cls, *args) for args in kwargs.items()]

    #@abstractmethod
    #def __copy__(self): ...

    #def __lshift__(self, other: Self) -> Self: ...
    @abstractmethod
    def attach(self, alias: Hashable = None, **kwargs) -> Self: ...
    @abstractmethod
    def detach(self, _qtd: int, /) -> Self: ...



class chain(Chain):

    @property
    def root(self) -> Chain:
        return self.__root__
    @property
    def last(self) -> Chain:
        return self.__last__
    @property
    def step(self) -> int:
        return self.__step__
    @property
    def child(self) -> Chain:
        return self.__child__
    @property
    def parent(self) -> Chain:
        return self.__parent__

    def __init__(self, alias: Hashable = None, **kwargs) -> None:
        self.__mapp__ = dict()
        self.__last__ = self
        self.__root__ = self
        self.__step__ = 0
        self.__child__ = None
        self.__parent__ = None
        self.alias = alias or self.allonym()
        [hasattr(self, anam) or setattr(self, anam, aval) for anam, aval in kwargs.items()]
        self.__mapp__[self.step] = self.__mapp__[str(self.alias)] = self

    def __len__(self):
        return len(self.__mapp__)

    """
    def __copy__(self):
        root = self.__class__(alias=self.root.alias, **vars(self.root))
        [root.attach(link.alias, **vars(link)) for link in next(self.root)]
        return root
    """

    def allonym(self) -> str:
        return self.step and f'_{self.step}' or hex(id(self))[2:]

    @classmethod
    def __make_link__(cls, alias: Hashable = None, **kwargs) -> Self:
        return cls(alias=alias, **kwargs)

    def attach(self, alias: Hashable = None, **kwargs) -> Self:
        link = self.__class__.__make_link__(alias=alias, **kwargs)
        link.__root__ = self.root
        link.__step__ = self.last.step+1
        link.alias = alias or link.allonym()
        link.__child__ = None
        del link.__mapp__
        self.root.__mapp__[link.step] = self.root.__mapp__[str(link.alias)] = link
        self.root.__mapp__[0] = self.root
        self.last.__child__ = link
        link.__parent__ = self.last
        for knot in self:
            knot.__last__ = link
        return link

    def detach(self, _qtd: int, /, ) -> Self:
        for knot in [knot for idex, knot in enumerate(list(reversed(self.root.__mapp__.values()))[:_qtd*2]) if idex%2]:
            self.root.__mapp__.pop(knot.alias)
            self.root.__mapp__.pop(knot.step)
            knot.parent and setattr(knot.parent, "__child__", None)
            [hasattr(knot, slot) and delattr(knot, slot) for slot in knot.__slots__+tuple(knot.__dict__.keys())]
        return self

    def __free__(self):
        """ palestine """
        self.detach(self.last.step-self.step+1)
        pass



@typedictclass
class roll:
    wlen: int
    name: Optional[str]

class hstr(str):
    """ Hardened String
        ===============
        A string-like type that:
            - Will not iterate through its characters
            - Will only convert str instances
    """
    def __new__(cls, value='', *args, **kwargs):
        return super(hstr, cls).__new__(cls, value, *args, **kwargs) if isinstance(value, str) else [value]
    def __iter__(self):
        yield self

class stack(list):

    def __getitem__(self, idex: slice|int):
        return super(stack, self).__getitem__(idex) if isinstance(idex, slice) or idex < len(self) else None
    def __call__(self):
        return self[-1] if len(self) else None
    def pop(self, index: SupportsIndex = -1) -> Self:
        return super(stack, self).pop(index) if len(self) else None
    def append(self, __object: Any, /) -> Self:
        super(stack, self).append(__object)
        return self
    def extend(self, __iterable: Iterable[Any], /) -> Self:
        super(stack, self).extend(__iterable)
        return self
    @overload
    def insert(self, __object: Any, /, *args) -> Self: ...
    @overload
    def insert(self, __index: SupportsIndex, __object: Any, /, *args) -> Self: ...
    def insert(self, *args) -> Self:
        __index, __object = args[:2] if len(args) >= 2 else (0, args[0])
        super(stack, self).insert(__index, __object)
        return self
