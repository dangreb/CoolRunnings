

import uuid
import copy
import random

import weakref as wk

from enum import Enum, auto

from typing import Any, Self, Iterable, Iterator


__all__ = ["HookSentinel", "Hook", "onhook"]


def allonym() -> str:
    yield random.shuffle([]) #TODO::
    pass


class onhook(Enum):
    copy = auto()
    delete = auto()


class Hook(frozenset):
    __slots__ = ()
    def __new__(cls, *args, **kwargs) -> Self:
        return super(Hook, cls).__new__(cls, [uuid.uuid4().bytes])
    def __class_getitem__(cls, articles: tuple[Any]) -> Iterator[tuple[Self, Any]]:
        yield from ((cls(), article) for article in articles)
    def __hash__(self) -> int:
        return hash(id(self))
    def __deepcopy__(self, memo):
        sentinel, article = HookSentinel[self]
        return sentinel().callback(onhook.copy, article, memo=memo) if article else None


class SentinelMeta(type):
    __inst__: dict[type ,object] = dict()
    def __call__(cls, *args, **kwargs):
        return cls.__inst__.get(cls, None) or cls.__inst__.setdefault(cls, super(SentinelMeta, cls).__call__(*args, **kwargs))
    def __iter__(cls):
        yield from cls.__inst__.items()


class HookSentinel(wk.WeakKeyDictionary, metaclass=SentinelMeta):
    __evnt__: onhook = None
    @property
    def evnt(self):
        return self.__evnt__
    @evnt.setter
    def evnt(self, value):
        self.__evnt__ = value

    def __bool__(self):
        return True

    def __call__(self, *articles, **kwargs) -> Iterable[Hook]:
        return [self.__enroll__(hook, article) for hook, article in Hook[articles]]

    def __enroll__(self, hook, article=None, **kwargs) -> Hook:
        wk.finalize(hook, self.callback, event=onhook.delete, article=article, **kwargs)
        super(HookSentinel, self).setdefault(hook, article)
        return hook

    def __class_getitem__(cls, hook: Hook) -> Self:
        for sentinel in cls:
            if hook in sentinel[-1]:
                return sentinel[0], sentinel[-1][hook]
        return HookSentinel(), None
    def __getitem__(self, hook: Hook) -> wk.ProxyType:
        outs = super(HookSentinel, self).get(hook, None)
        return wk.proxy(outs) if outs else None

    def __byps__(self) -> str:
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
        #if all([stk.find(fnm)+1 for fnm in [r"pandas\io\formats\format.py", r"pydevd_repr_utils.py", r"reprlib.py"]]):
        if all([stk.find(fnm)+1 for fnm in [r"pydevd_repr_utils.py", r"reprlib.py"]]):
            print(stk)
            print(datetime.now())
            print("\n\n")
            return True
        return False
    def callback(self, event: onhook, article: Any, memo: dict[int, Any]|None = None, **kwargs):
        """ Callback for Hook Events
        :param event: Event type. See :class:`coolruns.hooks.onhook`
        :param article: Object associated to the Hook
        :param memo: The deepcopy memo payload
        :param kwargs: Additional registered arguments
        """
        if self.__byps__(): return None # TODO::Provisory/Fix

        self.evnt = event
        match event:
            case onhook.copy:
                outs = copy.deepcopy(article, memo=memo)
            case onhook.delete:
                outs = article.__free__(article)
            case _: ...
        self.evnt = None
        return outs
