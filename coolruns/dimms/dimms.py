
import threading

import numpy as np

from pandas.core.frame import BlockManager
from numpy.lib.stride_tricks import as_strided


from typing import Hashable, Sequence, Mapping


from coolruns.persistor import gcollect
from coolruns.catena import Proper, catena
from coolruns.iterops import IterOps, RunOper


__all__ = ["Dimm"]




class Dimm(catena):
    dimdata: np.ndarray = Proper()
    windata: np.ndarray = Proper()
    locdata: np.ndarray = Proper()
    columns: Sequence[str] = Proper(initval=())
    shape: tuple[int,...] = Proper(surrogate="dimdata")
    strides: tuple[int,...] = Proper(surrogate="dimdata")
    def __init__(self, wlen: int, name: Hashable = None, **kwargs):
        super(Dimm, self).__init__(name, wlen=wlen, **kwargs)
        #shape, strid = self.prev.shape, self.prev.stride
        self.dimdata = as_strided(self.prev.dimdata, shape=(self.prev.shape[0]-wlen+1,)+self.prev.shape[1:-1]+(wlen, self.prev.shape[-1]), strides=(self.prev.strides[0],)+self.prev.strides, writeable=False)
        self.windata = as_strided(self.dimdata, shape=(self.root.shape[0]-wlen+1,)+(wlen,)+self.root.shape[-1:], strides=self.root.strides[:1]+self.root.strides, writeable=False)
        self.locdata = np.empty(self.windata.shape[:-1]+(0,))
    def iterop(self, iops: type[IterOps], /, blen: int = None, outcol: Sequence[str] = (), **kwargs) -> threading.Event:
        self.locdata = np.concat([self.locdata, np.empty(self.windata.shape[:-1]+(4,))], axis=-1)
        RunOper(iops(data=self.windata, blen=blen or len(windata), outcol=outcol, callback=self.iterop_callback))
    def iterop_callback(self, iterop: IterOps):
        #self.locdata = np.concat([self.locdata, iterop.result], axis=-1)
        self.locdata = iterop.result
        self.columns += tuple(iterop.outcol)
    def __len__(self) -> int:
        return self.shape[0]
    @gcollect(late=True)
    def free(self, *args, **kwargs):
        """ palestine """
        super(Dimm, self).free()
        Proper.free(self)



if __name__ == "__main__":
    pass
    d = Dimm(1000000000)
    pass