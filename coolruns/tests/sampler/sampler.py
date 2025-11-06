from email.policy import default

import numpy as np
import pandas as pd

from datetime import datetime, date

from typing import Callable, Sequence

from string import ascii_lowercase
from dataclasses import dataclass, field

from numpy.lib.stride_tricks import as_strided


__all__ = ["abcdcol", "Sampler"]


def abcdcol(dtyp: Sequence[np.dtype] = _dtypdef, sufx: str = "col") -> dict[str, np.dtype]:
    return dict(zip((sufx+"\n").join(ascii_lowercase[:len(dtyp)]).split(), dtyp))

_defcols: Sequence[np.dtype] = abcdcol(field(default=(np.float32,)*4))


@dataclass
class Sampler:
    pattern: Callable = Sampler.random
    columns: dict[str, np.dtype] = field(default=_defcols)
    endtime: datetime|date = field(default=datetime.now().date())

    @staticmethod
    def abcdcol(dtyp: Sequence[np.dtype] = _dtypdef, sufx: str = "col") -> dict[str,np.dtype]:
        dict(zip((sufx+"\n").join(ascii_lowercase[:len(dtyp)]).split(),dtyp))

    @classmethod
    def random(cls, length: int, columns: dict[str,np.dtype] = _defcols):
        return np.random.rand(length,len(columns))

    def __call__(self, length: int = 1024*1024):
        iter()
        pass
