

import numpy as np
import pandas as pd

from typing import Hashable, Sequence, Mapping

from coolruns.dimms import Dimm
from coolruns.catena import Proper
from coolruns.persistor import PersistentAccessor


__all__ = ["CoolRunsBase"]



class CoolRunsBase(Dimm, PersistentAccessor):
    dimdata: np.ndarray = Proper()
    datypes: Mapping[str, np.dtype] = Proper(initval=dict())
    windata: np.ndarray = Proper(surrogate="last")
    locdata: np.ndarray = Proper(surrogate="last")
    columns: Sequence[str] = Proper(surrogate="last", initval=())
    def allonym(self, obj) -> str:
        return f'{{ {obj.__class__.__name__} : {hex(id(obj))} }}  ( {" , ".join(str(s) for s in obj.shape)} )  [ {"/".join([col for col in obj.columns])} ]'
    def __init__(self, obj: pd.DataFrame, *args, **kwargs) -> None:
        super(Dimm, self).__init__(wlen=obj.shape[0], name=self.allonym(obj), *args, **kwargs)
        self.dimdata, self.dataset, self.datypes = obj.values, obj.values, obj.dtypes.to_dict()
    def __call__(self, wlen: int, name: Hashable = None, **kwargs) -> Dimm:
        Dimm(wlen=wlen, prev=self.last, name=name, **kwargs)
        return self
