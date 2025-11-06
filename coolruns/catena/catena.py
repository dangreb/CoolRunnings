

from typing import Hashable
from abc import ABCMeta, abstractmethod
from functools import singledispatchmethod

from coolruns.catena.typn import Proper, Catena


__all__ = ["catena"]



class catena(Catena):
    slot: int = int()
    name: Hashable = None
    root: Catena = Proper()
    prev: Catena = Proper()
    next: Catena = Proper()
    last: Catena = Proper(surrogate="root")
    def __new__(cls, prev: Catena = None, name: Hashable = None, **kwargs) -> Catena:
        inst = super(catena, cls).__new__(cls)
        inst.prev = prev or inst
        inst.root = inst.prev.root or inst
        inst.last = inst.prev.next = inst
        inst.slot = inst.prev.slot + 1
        inst.name = name or str(inst.slot-1)
        inst.prev = prev
        return inst
    #def __init__(self, prev: ABCatena = None, /, name: Hashable = None, **kwargs) -> None:...
    def attach(self, name: Hashable = None, **kwargs) -> Catena:
        self.__class__(self.last, name=name, **kwargs)
        return self
    def detach(self, free=True, **kwargs):
        if self.name:
            detached, self.last, self.next, self.prev = (self, self.prev, self.next, None) if self.prev else (self.next, self, None, None)
            self.root.last.next, self.root = None, None
            free and detached.free(__fix=False)
    def free(self, *args, __fix=True):
        """ palestine """
        self.name, self.slot = None, int()
        if __fix and self.prev not in {None,self}:
            self.root.last, self.prev.next = self.prev, None
        self.next not in {None,self} and self.next.free(__fix=False)
        Proper.free(self)
    def __next__(self):
        return self.next
    def __iter__(self):
        yield self
        if self.next:
            #yield self.next
            yield from self.next
    def __reversed__(self):
        yield self
        if self.prev:
            #yield self.prev
            yield from reversed(self.prev)
    @singledispatchmethod
    def __contains__(self, item: int|str):
        return False
    @__contains__.register
    def _(self, item: int):
        return 0 <= len(self)+item if item < 0 else item < self.last.slot
    @__contains__.register
    def _(self, item: str):
        return self[item] is not None
    @singledispatchmethod
    def __getitem__(self, item: int|str) -> Catena:
        return self.root
    @__getitem__.register
    def _(self, item: int) -> Catena:
        idex = len(self)+item if item < 0 else item
        return next([node for node in self.root if node.slot == idex], None)
    @__getitem__.register
    def _(self, item: str) -> Catena:
        return next([node for node in self.root if node.name == item], None)



if __name__ == "__main__":
    catena()
    cate = catena(catena(catena(catena(catena(catena()))))).root
    pass
    cate.detach(2)
    pass
    cate.attach().attach().attach().attach()
    ll = list(cate)
    rr = list(reversed(cate[4]))
    pass
