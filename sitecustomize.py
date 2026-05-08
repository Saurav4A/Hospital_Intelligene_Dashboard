"""Process-wide startup tweaks for local Python launches.

Python imports this module automatically when the project folder is on
``sys.path``. Keep it quiet and best-effort: startup must never fail because a
console feature could not be changed.
"""

try:
    from modules.windows_console import disable_quick_edit_mode

    disable_quick_edit_mode()
except Exception:
    pass
