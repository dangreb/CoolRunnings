
import os
import math

import functools
from importlib.metadata import metadata

import numpy as np
from numpy.lib.stride_tricks import as_strided

import pandas as pd
from pandas.core.frame import BlockManager
from pandas.core.generic import NDFrame
from pandas.core.accessor import _register_accessor
from pandas.api.extensions import register_dataframe_accessor

from collections import deque
from abc import ABCMeta, abstractmethod
from typing import Iterator

from coolruns.tool import Singleton
from coolruns.typn import wk, wkRef
from coolruns.tool import LifetimeHook



@_register_accessor("datman", BlockManager)
class ManagerAccess:
    __hook__: dict[int, LifetimeHook]
    def __set_name__(self, owner, name):
        self.__hook__ = dict()
        self.owner = owner
        self.name = name
    def __get__(self, instance, owner) -> LifetimeHook:
        return self.__hook__



class DataDeck(wk.WeakKeyDictionary, metaclass=Singleton):

    def __init__(self):
        super(DataDeck, self).__init__()

    def __call__(self, *articles) -> Iterator[LifetimeHook]:
        for hook, item in LifetimeHook[articles]:
            self.setdefault(hook, item)
            wk.finalize(hook, self.destructory, True, True, falso=False)
            yield hook

    def __getitem__(self, hook: LifetimeHook) -> wk.ProxyType:
        return wk.proxy(super(DataDeck, self).__getitem__(hook))

    def destructory(self, *args, **kwargs):
        for hook in self.keys():
            hook.handler()






if __name__ == "__main__":
    pass










