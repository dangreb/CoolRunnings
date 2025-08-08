
import functools

import numpy as np
import pandas as pd

from typing import overload, Collection, Self, Hashable

from coolruns.typn import Chain, chain
from coolruns.tool import AccessorPersistor, LifetimeHook

from coolruns.dimm.datman import DataDeck

class DataHook:
    __hook__: dict[int,LifetimeHook]
    def __set_name__(self, owner, name):
        self.__hook__ = dict()
        self.owner = owner
        self.name = name
    def __get__(self, instance, owner) -> LifetimeHook:
        return self.__hook__.get(id(instance), None)
    def __set__(self, instance, hook: LifetimeHook) -> None:
        self.__hook__.setdefault(id(instance), hook)


class Dimm(chain):
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

    def __len__(self) -> int:
        return self.__dlen__

    def __init__(self, wlen: int, alias: Hashable = None, **kwargs):
        super(Dimm, self).__init__(alias, wlen=wlen, **kwargs)
        self.__dlen__ = self.root.data.shape[0]-wlen+1
        self.__view__, self.__wnds__ = DataDeck()(
            as_strided(
                self.parent.data,
                shape=((self.parent.data.shape[0]-wlen)+1,)+(wlen,)+self.parent.data.shape[1:],
                strides=(self.parent.data.strides[0],)+self.parent.data.strides,
                writeable=False
            ),  # .swapaxes(1,-2),
            as_strided(
                self.view,
                shape=(self.root.data.shape[0]-wlen+1,)+(wlen,)+self.root.data.shape[-1:],
                strides=self.root.data.strides[:1]+self.root.data.strides,
                writeable=False
            )
        )




class Root(chain, metaclass=AccessorPersistor):
    __data__: LifetimeHook = DataHook()

    @property
    def data(self) -> LifetimeHook:
        return DataDeck()[self.root.__data__]

    def __len__(self) -> int:
        return self.__dlen__

    __new_link__ = Dimm.__new_link

    @functools.singledispatchmethod
    def __init__(self, data, alias: Hashable = None, **kwargs):
        None or super(Root, self).__init__(alias, wlen=wlen, **kwargs)
        raise TypeError(f'Source data must be a numpy.ndarray or a pandas.DataFrame, not {type(data)}.')

    @__init__.register(np.ndarray)
    def _(self, data: np.ndarray, feld: Collection[str] = None, alias: Hashable = None, **kwargs):
        super(Root, self).__init__(alias, wlen=data.shape[-1], feld=feld or tuple(f'column_{fidx:04}' for fidx in range(data.shape[-1])), **kwargs)
        self.__data__ = DataDeck()(data)
        self.__dlen__ = data.shape[0]


    @__init__.register(pd.DataFrame)
    def _(self, data: pd.DataFrame, alias: Hashable = None, **kwargs):
        super(Root, self).__init__(alias, wlen=data.values.shape[-1], feld=tuple(data.columns), **kwargs)
        self.__data__ = DataDeck()(data)
        self.__dlen__ = data.shape[0]





