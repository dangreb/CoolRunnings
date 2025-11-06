

import functools

from abc import ABCMeta, abstractmethod
from typing import Any, Self, ClassVar, Iterator, Sequence, Container, Hashable, Callable


__all__ = ["Catena", "Proper", "Singleton"]




class Catena(Iterator, Sequence, Hashable, metaclass=ABCMeta):

    __slots__ = ()

    @property
    @abstractmethod
    def root(self) -> Self:...
    @root.setter
    @abstractmethod
    def root(self, value: Self):...
    @property
    @abstractmethod
    def last(self) -> Self:...
    @last.setter
    @abstractmethod
    def last(self, value: Self):...
    @property
    @abstractmethod
    def prev(self) -> Self:...
    @prev.setter
    @abstractmethod
    def prev(self, value: Self):...
    @property
    @abstractmethod
    def next(self) -> Self:...
    @next.setter
    @abstractmethod
    def next(self, value: Self):...

    def __init__(self, prev: Self = None, /, name: str = None, **kwargs) -> Self:
        super(Catena, self).__init__()
        [hasattr(self, anam) or setattr(self, anam, aval) for anam, aval in kwargs.items()]

    @abstractmethod
    def attach(self, *args, name: str = None, **kwargs) -> Self:...
    @abstractmethod
    def detach(self, _qtd: int = 1, /,) -> Self:...

    def __hash__(self) -> int:
        return id(self)
    def __bool__(self) -> bool:
        return True
    def __len__(self) -> int:
        return self.last.slot

    @abstractmethod
    def __reversed__(self) -> Iterator[Self]:...
    @abstractmethod
    def __contains__(self, item) -> bool:
        return True
    @functools.singledispatchmethod
    def __getitem__(self, item: int|str) -> Self:...
    @abstractmethod
    @__getitem__.register
    def _(self, item: int) -> Self:...
    @abstractmethod
    @__getitem__.register
    def _(self, item: str) -> Self:...



#class Proper(Container, metaclass=ProperMeta):
class Proper(Container):
    owner: type = None
    static: bool = None
    initval: str = None
    propname: str = None
    internal: str = None
    surrogate: object = None
    echo: ClassVar[Callable[[Any], Any]] = lambda _:_
    source: Callable[[object], object] = echo
    __namespace__: ClassVar[dict[type, dict[str, Self]]] = dict()
    @property
    def namespace(self) -> dict[str, Self]:
        return self.__class__.__namespace__.get(self.owner) or self.__class__.__namespace__.setdefault(self.owner, dict())
    def __new__(cls, initval: Any = None, static: bool = False, surrogate: str = None):
        inst = super(Proper, cls).__new__(cls)
        inst.initval, inst.static, inst.source, inst.surrogate = initval, static, type if static else inst.__class__.echo, surrogate
        return inst
    def __set_name__(self, owner: type, name: str) -> None:
        self.owner, self.propname, self.internal = self.owner, name, f'__p_{name.lstrip("_").rstrip("_")}__'
        setattr(owner, self.internal, self.initval) #if not self.surrogate else None
        self.namespace.setdefault(self.propname, self)
        self.source = self.__sursource__ if self.surrogate and self.surrogate in dir(owner) else self.source
    def __set__(self, instance: object, value: object):
        setattr(self.__fixname__(self.source(instance)), self.internal, value)
    def __get__(self, instance: object, owner: type):
        return getattr(self.__fixname__(self.source(instance or owner)), self.internal, self.initval)
    def __delete__(self, instance: object) -> None:
        delattr(self.__fixname__(self.source(instance)), self.internal) if not self.surrogate else self.source(instance)
    def __bool__(self) -> bool:
        return True
    def __contains__(self, item):
        #return item in self.namespace
        return True
    @classmethod
    def free(cls, instance: object) -> None:
        [(delattr(prop.source(instance), prop.internal), setattr(owner, prop.internal, prop.initval)) for prop in cls.__namespace__.get(instance.__class__, dict()).values() if not prop.surrogate]
    def __sursource__(self, instance) -> Callable[[object], object]:
        outs =  getattr(type(instance) if self.static else instance, self.surrogate, instance)
        return outs
    def __fixname__(self, source: type|object) -> str:
        if source is None or isinstance(source, type):
            return source
        self.__fixname__ = self.__class__.echo
        if self.internal not in dir(source):
            self.internal = self.propname
        return source

