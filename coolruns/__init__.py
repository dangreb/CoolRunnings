
import pandas as pd

import coolruns.chain
import coolruns.hooks
import coolruns.iterops
import coolruns.persistor

from coolruns.coolruns import CoolRunningsBase

__all__ = ["CoolRunnings"]

class CoolRunnings(CoolRunningsBase, handle="coolruns", target=pd.DataFrame):
    def __init__(self, obj: pd.DataFrame, *args, **kwargs) -> None:
        super(CoolRunnings, self).__init__(obj, *args, **kwargs)
        self.__data__ = obj.values
        self.__feld__ = dict(zip(tuple(obj.columns), tuple(obj.dtypes.values)))