

import coolruns.hooks
import coolruns.catena
import coolruns.iterops
import coolruns.persistor

from pandas import DataFrame

from coolruns.hooks import HookSentinel
from coolruns.coolruns import CoolRunsBase


__all__ = ["CoolRunnings"]



class CoolRunnings(CoolRunsBase, handle="coolruns", target=DataFrame, sentinel=HookSentinel):...