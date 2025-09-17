
import numpy as np

from coolruns.iterops import IterOps


__all__ = ["WindowRatio"]



class WindowRatio(IterOps):
    # result = WindowRatio().RunOper(maxw=8)(asdf=True)
    @property
    def columns(self):
        return super(WindowRatio, self).columns
    @property
    def index(self):
        return super(WindowRatio, self).index
    def __call__(self, batch: np.ndarray) -> np.ndarray:
        return batch/np.mean(batch, axis=1, keepdims=True)