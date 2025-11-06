

import numpy as np

from coolruns.iterops import IterOps


__all__ = ["WindowRatio", "AutoPearson"]



class WindowRatio(IterOps):
    def __call__(self, data: np.ndarray, slicer: slice, *args, **kwargs) -> np.ndarray:
        return np.true_divide(data, np.mean(data, axis=1, keepdims=True, dtype=self.data.dtype))
        #return np.true_divide(np.multiply(data,1000000),self.mean[kwargs["slicer"]])


class AutoPearson(IterOps):
    def __call__(self, data: np.ndarray, slicer: slice, *args, **kwargs) -> np.ndarray:
        pass
        outs = np.corrcoef(data)
        return outs