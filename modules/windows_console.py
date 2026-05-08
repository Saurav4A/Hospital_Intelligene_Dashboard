"""Windows console safeguards for long-running server processes."""

from __future__ import annotations

import ctypes
import os


def disable_quick_edit_mode() -> bool:
    """Disable CMD QuickEdit so selecting text cannot pause the server.

    In a Windows console, QuickEdit mode can suspend the attached process when
    text is selected in the window. That is dangerous for Waitress/Flask apps
    that continuously write logs to stdout/stderr. This function is a no-op on
    non-Windows platforms or when stdin is not a real console.
    """

    if os.name != "nt":
        return False

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    std_input_handle = -10
    invalid_handle_value = ctypes.c_void_p(-1).value
    enable_quick_edit_mode = 0x0040
    enable_extended_flags = 0x0080

    handle = kernel32.GetStdHandle(std_input_handle)
    if handle in (0, invalid_handle_value):
        return False

    mode = ctypes.c_uint()
    if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
        return False

    new_mode = (mode.value | enable_extended_flags) & ~enable_quick_edit_mode
    if new_mode == mode.value:
        return True

    return bool(kernel32.SetConsoleMode(handle, new_mode))
