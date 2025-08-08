
import numpy as np
import pandas as pd

from pandas.api.extensions import register_dataframe_accessor

from typing import overload, Collection, Self, Hashable

from coolruns.typn import Chain, chain
from coolruns.tool import AccessorPersistor, LifetimeHook, gcollect

from coolruns.dimm.datman import DataDeck



class Dimm(chain):

    class DataHook:
        __hook__: dict[int, LifetimeHook]
        def __set_name__(self, owner, name):
            self.__hook__ = dict()
            self.owner = owner
            self.name = name
        def __get__(self, instance, owner) -> LifetimeHook:
            return self.__hook__.get(id(instance), None)
        def __set__(self, instance, hook: LifetimeHook) -> None:
            self.__hook__.setdefault(id(instance), hook)
        def __delete__(self, instance):
            del self.__hook__[id(instance)]

    __locl__: LifetimeHook = DataHook()
    __fore__: LifetimeHook = DataHook()
    __view__: LifetimeHook = DataHook()
    __wnds__: LifetimeHook = DataHook()

    @property
    def locl(self) -> LifetimeHook:
        return DataDeck()[self.__locl__]
    @property
    def view(self) -> LifetimeHook:
        return DataDeck()[self.__view__]
    @property
    def wnds(self) -> LifetimeHook:
        return DataDeck()[self.__wnds__]

    __metr__: dict[str, int] = dict()

    @property
    def dimens(self) -> tuple[int,...]:
        return self.__metr__.get("shape", None)
    @property
    def stride(self) -> tuple[int,...]:
        return self.__metr__.get("stride", None)

    def __len__(self) -> int:
        return self.root.dimens[0]-self.wlen+1


    def __init__(self, wlen: int, alias: Hashable = None, **kwargs):
        super(Dimm, self).__init__(alias, wlen=wlen, **kwargs)
        self.__view__ = next(DataDeck()(as_strided(
                self.parent.data,
                shape=(self.parent.dmms[0]-wlen+1,)+(wlen,)+self.parent.dmms[1:],
                strides=(self.parent.data.strides[0],)+self.parent.data.strides,
                writeable=False
        )))
        self.__wnds__ = next(DataDeck()(as_strided(
                self.view,
                shape=(self.root.data.shape[0]-wlen+1,)+(wlen,)+self.root.data.shape[-1:],
                strides=self.root.data.strides[:1]+self.root.data.strides,
                writeable=False
        )))



@register_dataframe_accessor("crunn")
class CoolRunnings(chain, metaclass=AccessorPersistor, __make_link=Dimm.__make_link):
    __data__: LifetimeHook = Dimm.DataHook()
    __metr__: dict[str,int] = dict()

    @property
    def data(self) -> LifetimeHook:
        return DataDeck()[self.root.__data__]

    @property
    def dimens(self) -> tuple[int,...]:
        return self.__metr__.get("shape", None)
    @property
    def stride(self) -> tuple[int,...]:
        return self.__metr__.get("stride", None)

    def __len__(self) -> int:
        return self.wlen


    def __init__(self, alias: Hashable = None, **kwargs):
        super(CoolRunnings, self).__init__(alias=alias, **kwargs)
        self.feld = None
        self.wlen = 0

    def allonym(self) -> str:
        return self.step and f'_{self.step}' or "root"

    def __complete__(self, pobj) -> Self:
        self.__data__ = next(DataDeck()(pobj().values))
        self.__metr__.update(shape=tuple(self.__data__.shape), strides=tuple(self.__data__.strides))
        self.feld = tuple(pobj.columns)
        self.wlen = self.data.shape[0]
        return self

    def __call__(self, wlen: int, alias: Hashable = None, **kwargs) -> Dimm:
        self.attach(self, alias=alias, wlen=wlen, **kwargs)

    @gcollect(late=True)
    def __free__(self):
        """ palestine """
        super(CoolRunnings, self).__free__()
        del self.__data__
        pass


if __name__ == "__main__":
    pass


