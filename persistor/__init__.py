"""
    Persistent Accessors Suite
    --------------------------
    An abstraction layer to enable instance persistence
    behavior for high level accessor based pandas extensions.

    Currently available for pandas.Dataframe, pandas.Series
    and pandas.Index accessors.

    Usage
    -----
    Persistent Accessors must inherit :class:`PersistentAccessor`

"""

from persistor.hooks import Hook, HookSentinel
from persistor.persistor import PersistentAccessor

__all__ = ["Hook", "HookSentinel", "PersistentAccessor"]