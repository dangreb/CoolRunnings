"""
def __dir__(self) -> list[str]:
    return dir(self.__uuid__)
def __repr__(self) -> str:
    return f"<{self.__class__.__name__}: {self.__uuid__}>"
def __str__(self) -> str:
    return str(self.__uuid__)
def __hash__(self) -> int:
    return hash(self.__uuid__)
def __eq__(self, other) -> bool:
    return self.__uuid__ == other
def __ne__(self, other) -> bool:
    return self.__uuid__ != other
def __lt__(self, other) -> bool:
    return self.__uuid__ < other
def __le__(self, other) -> bool:
    return self.__uuid__ <= other
def __gt__(self, other) -> bool:
    return self.__uuid__ > other
def __ge__(self, other) -> bool:
    return self.__uuid__ >= other
def __bool__(self) -> bool:
    return bool(self.__uuid__)
def __call__(self, *args, **kwargs):
    return self.__uuid__.__call__(*args, **kwargs)
def __getnewargs__(self):
    return (self.__uuid__,)
def __getstate__(self):
    return self.__uuid__
def __setstate__(self, state):
    self.__uuid__ = state
"""