
import inspect

#import pydevd as pedv
from coolruns.typn import Singleton

__all__ = ["PyDBOps"]

class PyDBOps(metaclass=Singleton):
    def __init__(self):
        super(PyDBOps, self).__init__()
        #self.pydb: pedv.PyDB = ([None]+list(filter(bool, [dict(s.frame.f_locals).get("debugger", None) for s in inspect.stack()]))).pop()
        self.pydb = ([None]+list(filter(bool, [dict(s.frame.f_locals).get("debugger", None) for s in inspect.stack()]))).pop()

    def __call__(self, *args, **kwargs):
        pass
        pass