
import numpy as np
import pandas as pd

from numpy.lib.stride_tricks import as_strided
from pandas.api.extensions import register_dataframe_accessor

from typing import Self, Hashable

from coolruns._typing import chain, wk
from coolruns.hooks import HookSentinel, HookMart, Hook, gcollect
from coolruns.persistor import AccessorPersistor

from coolruns.datman import DataDeck


"""
@delegate_names(delegate=BlockManager, accessors=["attrs"], typ="property")
@_register_accessor("attrs", BlockManager)
class ManagerAccess(PandasDelegate, metaclass=AccessorPersistor):
    def __init__(self, obj: BlockManager) -> None:
        self.obj = obj
        print(obj.__class__.__name__)
    def __call__(self, *args, **kwargs) -> BlockManager:
        return self.obj
    def _delegate_property_get(self, name: str, *args, **kwargs):
        return getattr(self.obj, name, None)
    def _delegate_property_set(self, name: str, value, *args, **kwargs) -> None:
        return setattr(self.obj, name, value)
    def _delegate_method(self, name: str, *args, **kwargs):
        return delattr(self.obj, name, None)
"""



class DatMan(HookSentinel):

    def __init__(self, *args, **kwargs):
        super(DatMan, self).__init__()

    def callback(self, *args, **kwargs):
        pass



class Dimm(chain):
    __locl__: Hook = HookMart()
    __fore__: Hook = HookMart()
    __view__: Hook = HookMart()
    __wnds__: Hook = HookMart()

    @property
    def locl(self) -> Hook:
        return DataDeck()[self.__locl__]
    @property
    def view(self) -> Hook:
        return DataDeck()[self.__view__]
    @property
    def wnds(self) -> Hook:
        return DataDeck()[self.__wnds__]

    __metr__: dict[str, int] = dict()

    @property
    def shape(self) -> tuple[int,...]:
        return self.__metr__.get("shape", None)
    @property
    def stride(self) -> tuple[int,...]:
        return self.__metr__.get("stride", None)

    def __len__(self) -> int:
        return self.root.shape[0]-self.wlen+1



    def __init__(self, wlen: int, alias: Hashable = None, **kwargs):
        super(Dimm, self).__init__(alias, wlen=wlen, **kwargs)
        root = kwargs.get("root", None)
        self.__view__ = next(DataDeck()(as_strided(root.last.view, shape=(root.last.shape[0]-wlen+1,)+(wlen,)+root.last.shape[1:],strides=(root.last.stride[0],)+root.last.stride,writeable=False)))
        self.__wnds__ = next(DataDeck()(as_strided(self.view,shape=(root.shape[0]-wlen+1,)+(wlen,)+root.shape[-1:],strides=root.stride[:1]+root.stride,writeable=False)))
        self.__metr__.update(shape=tuple(self.view.shape), stride=tuple(self.view.strides))



@register_dataframe_accessor("coolruns")
class CoolRunnings(chain, metaclass=AccessorPersistor):
    __make_link__ = Dimm.__make_link__
    __data__: Hook = HookMart()
    __metr__: dict[str,int] = dict()


    @property
    def data(self) -> Hook:
        return DataDeck()[self.root.__data__]
    @property
    def shape(self) -> tuple[int,...]:
        return self.__metr__.get("shape", None)
    @property
    def stride(self) -> tuple[int,...]:
        return self.__metr__.get("stride", None)


    def __deepcopy__(self, memo):
        breakpoint()
        pass

    def __copy__(self):
        breakpoint()
        pass

    def allonym(self) -> str:
        return self.step and f'_{self.step}' or "root"


    def __len__(self) -> int:
        return self.wlen

    def __repr__(self) -> str:
        shape = tuple(schm.wlen for schm in self.root.schema) or (self.root.wlen,)
        return f'[ {self.__class__.__name__} : {hex(id(self))} ] [ Full Length : {math.prod(shape):,} | Windows : {math.prod(shape)//shape[-1]:,} | Depth : {len(shape)} ] ( {", ".join(map(str, shape))} )'


    def __init__(self, alias: Hashable = None, **kwargs):
        super(CoolRunnings, self).__init__(alias=alias, **kwargs)
        CoolRunnings.view = CoolRunnings.data
        self.feld = None
        self.wlen = 0


    def __complete__(self, pobj) -> Self:
        self.__data__ = next(DataDeck()(pobj.values))
        self.__metr__.update(shape=tuple(self.data.shape), stride=tuple(self.data.strides))
        self.feld = tuple(pobj.columns)
        self.wlen = self.data.shape[0]
        return self


    def __call__(self, wlen: int, alias: Hashable = None, **kwargs) -> Dimm:
        self.attach(alias=alias, wlen=wlen, root=self.root, **kwargs)
        return self


    @gcollect(late=True)
    def __free__(self):
        """ palestine """
        super(CoolRunnings, self).__free__()
        del self.__data__
        pass



if __name__ == "__main__":
    from fastdata import fastdata
    indf: pd.DataFrame = pd.DataFrame(**fastdata(dlen=1024*1024, edat="2025-07-01", datyp=np.uint32, ))
    indf.coolruns(64)(32)(16)

    #dats = CoolRunnings(indf)
    bret = indf.coolruns(64)(32)(16)


