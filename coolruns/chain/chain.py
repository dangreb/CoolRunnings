
from typing import Self, Hashable, Iterator, Reversible, Container


__all__ = ["Chain"]


class Chain(Iterator, Reversible, Container):
    @property
    def root(self) -> Self:
        return self.__root__
    @property
    def last(self) -> Self:
        return self.__last__
    @property
    def step(self) -> int:
        return self.__step__
    @property
    def child(self) -> Self:
        return self.__child__
    @property
    def parent(self) -> Self:
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
    def __new__(cls, alias: Hashable = None, **kwargs) -> Self:
        return super(Chain, cls).__new__(cls)
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
        return int(len(self.__mapp__)//2)
    def __next__(self):
        return self.child
    def __iter__(self):
        knot = self
        for _ in range(int(len(self.root.__mapp__)//2)):
            yield knot
            knot = knot.child
    def __reversed__(self):
        knot = self
        while knot:
            yield knot
            knot = knot.parent
    def __contains__(self, item):
        return item in self.__mapp__
    def __getitem__(self, item: str|int):
        return self.__mapp__.get(item, None) or self.__mapp__.get(self.last.step+item+1 if isinstance(item, int) and item < 0 else item, None)

