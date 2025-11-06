
import numpy as np
import pandas as pd

from numpy.lib.stride_tricks import as_strided


__all__ = ["fastdata", "mixdata"]



def fastdata(dlen: int = 1024*1024, cols: tuple[str]|list[str] = ("acol", "bcol", "ccol", "dcol"), edat: str = "2025-07-01", datyp: np.dtype = np.float32):
    span = pd.date_range(end=edat, periods=dlen, freq=pd.offsets.Minute(1))
    return dict(columns=cols, data=as_strided(np.arange(dlen, dtype=datyp)+1, shape=(dlen, len(cols)), strides=(datyp().nbytes, 0)).copy(), index=span, dtype=datyp)


def mixdata(dlen: int = 1024*1024, cols: tuple[str]|list[str] = ("acol", "bcol", "ccol", "dcol"), edat: str = "2025-07-01", datyp: np.dtype = np.float32):
    span = pd.date_range(end=edat, periods=dlen, freq=pd.offsets.Minute(1))
    return dict(data=dict(zip(cols,[np.arange(dlen, dtype=datyp)+1 if not idex % 2 else np.array([f'{cols[idex]}_{idex}'], dtype=np.str_) for idex in range(len(cols))])), index=span)