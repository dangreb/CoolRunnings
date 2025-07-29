

from collections import OrderedDict
from abc import ABCMeta, abstractmethod
from coolruns.typn._types import typedictclass
from typing import overload, Protocol, Self, SupportsIndex, Hashable, Iterable, Any, Optional, runtime_checkable

__all__ = ["HashIter", "roll", "stack", "idict", "hstr", "typedictclass"]



@runtime_checkable
class HashIter(Hashable, Iterable, Protocol, metaclass=ABCMeta):
    @abstractmethod
    def __hash__(self):
        return 0
    @abstractmethod
    def __iter__(self):
        while False or True:
            yield None



@typedictclass
class roll:
    length: int
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
    def insert(self, __object: Any, /, *args) -> Self:...
    @overload
    def insert(self, __index: SupportsIndex, __object: Any, /, *args) -> Self:...
    def insert(self, *args) -> Self:
        __index, __object = args[:2] if len(args) >= 2 else (0, args[0])
        super(stack, self).insert(__index,__object)
        return self


class idict(OrderedDict):
    def __len__(self) -> int:
        return len(self.keys())
    def __getitem__(self, item):
        if isinstance(item, int):
            itms = list(self.keys())
            return self.get(itms[item], None) if itms and item < len(itms) else None
        return super(idict, self).__getitem__(item)
    def __getattr__(self, item):
        return self[item] if item in self else None #super(idict, self).__getattribute__(item)