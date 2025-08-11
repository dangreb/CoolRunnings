from typing import Iterator

from coolruns._typing import wk, wkRef, Singleton
from coolruns.hooks import Hook


class DataDeck(wk.WeakKeyDictionary, metaclass=Singleton):

    def __init__(self):
        super(DataDeck, self).__init__()

    def __call__(self, *articles) -> Iterator[Hook]:
        for hook, item in Hook[articles]:
            self.setdefault(hook, item)
            wk.finalize(hook, self.destructory, article=item)
            yield hook

    def __getitem__(self, hook: Hook) -> wk.ProxyType:
        return wk.proxy(super(DataDeck, self).__getitem__(hook))

    def destructory(self, *args, **kwargs):...





class DatMan(wk.WeakKeyDictionary, metaclass=Singleton):

    def __init__(self):
        super(DatMan, self).__init__()

    def __call__(self, *articles) -> Iterator[Hook]:
        for hook, item in Hook[articles]:
            self.setdefault(hook, item)
            wk.finalize(hook, self.destructory, True, True, falso=False)
            yield hook

    def __getitem__(self, hook: Hook) -> wk.ProxyType:
        return wk.proxy(super(DatMan, self).__getitem__(hook))

    def destructory(self, *args, **kwargs): ...


if __name__ == "__main__":
    pass










