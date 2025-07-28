
import warnings

import numpy as np
import pandas as pd
import weakref as wk

from typing import overload, Self, Optional

from pandas.api.extensions import register_dataframe_accessor

from coolruns.typn import roll
from coolruns.dimm import Dimm, RootDimm
from coolruns.tool import AcessorPersistor

__all__ = ["CoolRunnings"]



@register_dataframe_accessor("coolruns")
class CoolRunnings(metaclass=AcessorPersistor):
    """

    """

    """
    @property
    def coolruns(self):
        return self._obj.coolruns
    @coolruns.setter
    def coolruns(self, _) -> None: ...
    """

    def __init__(self, df: pd.DataFrame):
        self._obj = wk.proxy(df)
        self.root = RootDimm(data=df.values) #TODO:: Invalid state Controll!!

    def __bool__(self) -> bool:
        return True

    def __iter__(self) -> Dimm:
        dimm = self.root
        while dimm.child:
            yield dimm
            dimm = dimm.child

    def __reversed__(self) -> Dimm:
        dimm = self.root.leaf
        while isintance(dimm, Dimm):
            yield dimm
            dimm = dimm.parent

    def __contains__(self, name: str) -> bool:
        for dimm in self:
            if dimm.name == name:
                return True
        return False

    def __getitem__(self, name: str) -> Dimm:
        for dimm in self:
            if dimm.name == name:
                return dimm
        return self.leaf

    def free(self):
        """ palestine """
        self.root.free()
        self._obj = self.root = self.root.leaf = None
        pass

    @overload
    def __call__(self, wlen: int, name: str = None, *args, **kwargs) -> Self: ...
    @overload
    def __call__(self, roll: roll = None, *args, **kwargs) -> Self: ...
    @overload
    def __call__(self, roll: list[roll], *args, **kwargs) -> Self: ...
    @overload
    def __call__(self, *args: tuple[roll], **kwargs) -> Self: ...
    def __call__(self, *args, **kwargs) -> Self:
        pass
        if args:
            if isinstance(args[0], int):
                return self.__single_roll(dict(list(zip(("wlen", "name", "stride"), args))+list(kwargs.items())))
            if isinstance(args[0], list|tuple):
                args = args[0]
            if all(isinstance(a, roll) for a in args):
                return self.__multi_roll(args)
            warnings.warn(f"Fail to apply sliding windows. Unknown specifications format", category=RuntimeWarning, stacklevel=2)
        if kwargs:
            if "wlen" in kwargs and isinstance(kwargs["wlen"], int):
                return self.__single_roll(kwargs)
            if "roll" in kwargs:
                if isinstance(kwargs["roll"], list|tuple) and all(isinstance(r, roll) for r in kwargs["roll"]):
                    return self.__multi_roll(kwargs["roll"])
                if isinstance(kwargs["roll"], roll):
                    return self.__single_roll(kwargs["roll"])
            warnings.warn(f"Fail to apply sliding windows. Unknown specifications format", category=RuntimeWarning, stacklevel=2)
        return self

    def __single_roll(self, roll: roll) -> Self:
        Dimm(self.root.leaf, **roll)
        return self

    def __multi_roll(self, roll: list[roll]) -> Self:
        [self.__single_roll(r) for r in roll]
        return self



def memtrace(data: pd.DataFrame, /,):
    tracemalloc.start()
    a = np.zeros((300, 500))
    b = [i**2 for i in range(300*500)]
    snapshot1 = tracemalloc.take_snapshot()
    pass
    np_domain = np.lib.tracemalloc_domain
    dom_filter = tracemalloc.DomainFilter(inclusive=True, domain=np_domain)
    snapshot1 = snapshot1.filter_traces([dom_filter])
    top_stats1 = snapshot1.statistics('traceback')


    print("================ SNAPSHOT 1 =================")
    for stat in top_stats1:
        print(f"{stat.count} memory blocks: {stat.size / 1024:.1f} KiB")
        print(stat.traceback.format()[-1])

    # Clear traces of memory blocks allocated by Python
    # before moving to the next section.
    tracemalloc.clear_traces()

    data.coolruns(16, "dimm1")
    snapshot2 = tracemalloc.take_snapshot()
    top_stats2 = snapshot2.statistics('traceback')

    print()
    print("================ SNAPSHOT 2 =================")
    for stat in top_stats2:
        print(f"{stat.count} memory blocks: {stat.size/1024:.1f} KiB")
        print(stat.traceback.format()[-1])

    tracemalloc.clear_traces()

    data.coolruns(8, "dimm2")
    snapshot3 = tracemalloc.take_snapshot()
    top_stats3 = snapshot3.statistics('traceback')

    print()
    print("================ SNAPSHOT 3 =================")
    for stat in top_stats3:
        print(f"{stat.count} memory blocks: {stat.size/1024:.1f} KiB")
        print(stat.traceback.format()[-1])

    tracemalloc.clear_traces()

    data.coolruns(4, "dimm3")
    snapshot4 = tracemalloc.take_snapshot()
    top_stats4 = snapshot4.statistics('traceback')

    print()
    print("================ SNAPSHOT 4 =================")
    for stat in top_stats4:
        print(f"{stat.count} memory blocks: {stat.size/1024:.1f} KiB")
        print(stat.traceback.format()[-1])

    tracemalloc.stop()

    print()
    print("============================================")
    print("\nTracing Status : ", tracemalloc.is_tracing())

    try:
        print("\nTrying to Take Snapshot After Tracing is Stopped.")
        snap = tracemalloc.take_snapshot()
    except Exception as e:
        print("Exception : ", e)

def memidex(data: pd.DataFrame, /,):
    tracemalloc.start()
    a = np.zeros((300, 500))
    b = [i**2 for i in range(300*500)]
    snapshot1 = tracemalloc.take_snapshot()
    pass
    np_domain = np.lib.tracemalloc_domain
    dom_filter = tracemalloc.DomainFilter(inclusive=True, domain=np_domain)
    snapshot1 = snapshot1.filter_traces([dom_filter])
    top_stats1 = snapshot1.statistics('traceback')


    print("================ SNAPSHOT 1 =================")
    for stat in top_stats1:
        print(f"{stat.count} memory blocks: {stat.size / 1024:.1f} KiB")
        print(stat.traceback.format()[-1])

    a = b = None
    # Clear traces of memory blocks allocated by Python
    # before moving to the next section.
    tracemalloc.clear_traces()

    #idex = np.arange(data.shape[0], dtype=options.window_index_dtype)
    idex = data.index.factorize()

    snapshot2 = tracemalloc.take_snapshot()
    top_stats2 = snapshot2.statistics('traceback')

    print()
    print("================ SNAPSHOT 2 =================")
    for stat in top_stats2:
        print(f"{stat.count} memory blocks: {stat.size/1024:.1f} KiB")
        print(stat.traceback.format()[-1])

    tracemalloc.stop()

    print()
    print("============================================")
    print("\nTracing Status : ", tracemalloc.is_tracing())

    try:
        print("\nTrying to Take Snapshot After Tracing is Stopped.")
        snap = tracemalloc.take_snapshot()
    except Exception as e:
        print("Exception : ", e)



if __name__ == "__main__":
    pd.options.mode.copy_on_write = True
    pd.options.mode.string_storage = "pyarrow"
    pd.options.future.infer_string = True

    from numpy.lib.stride_tricks import as_strided

    dlen = 1024*1024*100
    cols = ("acol", "bcol", "ccol", "dcol")
    span = pd.date_range(end="2025-07-01", periods=dlen, freq=pd.offsets.Minute(1))
    ndar = as_strided(np.arange(dlen, dtype=np.float32), shape=(dlen, len(cols)), strides=(np.float32().nbytes,0)).copy()
    data = pd.DataFrame(columns=cols, data=ndar, index=span)

    pass
    #memtrace(data)
    #memidex(data)
    pass

    data.coolruns(8, "dimm1")(16, "dimm2")(8, "dimm3")

    breakpoint()
