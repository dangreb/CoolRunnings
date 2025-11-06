

import copy
import weakref as wk

from abc import ABCMeta
from typing import Any, Self, Iterator, Hashable


__all__ = ["HookSentinel", "Hook"]



# TODO::Provisory/Fix
def __byps__() -> str:
    """
    Bypass for PyDev Debugger's representation routines used by PyCharm Debugger console
    to generate lables to variable watches. These routines will trigger recursive copies
    of a pandas DataFrame cousing havoc during debug sessions.
    :return:
    """
    ## !!!! ========== !!!! #
    import inspect  # MacGyver
    from datetime import datetime, timedelta
    ## !!!! ========== !!!! #
    stk = "\n".join([f'{f.lineno:5} | {f.filename}' for f in inspect.stack()])
    if all([stk.find(fnm)+1 for fnm in [r"pydevd_repr_utils.py", r"reprlib.py"]]):
        #print(stk)
        #print(datetime.now())
        #print("\n\n")
        return True
    return False


class Hook(Hashable):
    __slots__ = ('__weakref__',)
    def __hash__(self) -> int:
        return id(self)
    def __deepcopy__(self, memo):
        if __byps__(): return None # TODO::Provisory/Fix
        return HookSentinel[self].get(hook, None) and copy.deepcopy(HookSentinel[self][hook], memo=memo)
    def __class_getitem__(cls, articles: tuple[Any]) -> Iterator[tuple[Self, Any]]:
        yield from ((cls(), article) for article in articles)



class MetaSentinel(ABCMeta):
    __instance__: Self = None
    def __call__(cls, *args, **kwargs):
        cls.__instance__ = cls.__instance__ or super(MetaSentinel, cls).__call__(*args, **kwargs)
        return cls.__instance__


class HookSentinel(wk.WeakKeyDictionary, metaclass=MetaSentinel):
    def __call__(self, article, hook=None, **kwargs) -> Hook:
        hook = hook or Hook()
        self.setdefault(hook, article)
        wk.finalize(hook, getattr(article, "free", lambda *_,**__: None), article=article, **kwargs)
        #super(HookSentinel, self).setdefault(hook, article)
        return hook
    def __class_getitem__(cls, hook: Hook) -> Self:
        return next((sentinel for _,sentinel in cls if hook in sentinel), None)
    def __getitem__(self, hook: Hook) -> wk.ProxyType:
        outs = super(HookSentinel, self).get(hook, None)
        return wk.proxy(outs) if outs else None
    def __bool__(self):
        return True


if __name__ == "__main__":
    pass
    hksn = HookSentinel()
    snhk = SentinelHook()
    pass