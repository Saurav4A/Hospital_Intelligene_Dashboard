"""
WSGI entrypoint for production servers.

Usage examples:
- Gunicorn: gunicorn -w 4 -b 0.0.0.0:5001 wsgi:app
- Waitress (Windows-friendly): waitress-serve --listen=0.0.0.0:5001 --threads=8 wsgi:app
"""
try:
    from modules.windows_console import disable_quick_edit_mode

    disable_quick_edit_mode()
except Exception:
    pass

from app import app

# Expose as "application" for servers that look for that name
application = app
