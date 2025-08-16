

from abc import ABCMeta, abstractmethod

from typing import Self, Hashable, Iterator, Reversible

__all__ = ["Chain", "Chain", "Singleton", "ObjectCatalog"]


def gcollect(func=None, /, early=True, late=False):
    def wrapper(fn):
        def _collect_call(*args, **kwargs):
            gc.collect() if early else ...
            resu = fn(*args, **kwargs)
            gc.collect() if late else ...
            return resu
        return _collect_call
    return wrapper(func) if func else wrapper


class Singleton(ABCMeta):
    __inst__: dict[type ,object] = dict()
    def __call__(cls, *args, **kwargs):
        return cls.__inst__.get(cls, None) or cls.__inst__.setdefault(cls, super(Singleton, cls).__call__(*args, **kwargs))
    def __iter__(cls):
        yield from cls.__inst__.items()


class Chain(Iterator, Reversible, metaclass=ABCMeta):
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
    @classmethod
    def __link__(cls, alias: Hashable = None, **kwargs) -> Self:
        return cls(alias=alias, **kwargs)
    def allonym(self) -> str:
        return self.step and f'_{self.step}' or hex(id(self))[2:]
    def attach(self, alias: Hashable = None, **kwargs) -> Self:
        link = self.__class__.__link__(alias=alias, **kwargs)
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
    def __next__(self):
        return self.child
    def __iter__(self):
        knot = self
        for _ in range(self.last.step+1):
            yield knot
            knot = knot.child
    def __reversed__(self):
        knot = self
        while knot:
            yield knot
            knot = knot.parent
    def __getitem__(self, item: str|int):
        return self.__mapp__.get(item, None) or self.__mapp__.get(self.last.step+item+1 if isinstance(item, int) and item < 0 else item, None)

