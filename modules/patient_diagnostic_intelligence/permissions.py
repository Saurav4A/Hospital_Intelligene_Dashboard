from __future__ import annotations

from .utils import MODULE_KEY


def can_access_patient_diagnostic(session_obj) -> bool:
    role = (session_obj.get("role") or "").strip()
    if role == "IT":
        return True
    rights = [str(x).strip().lower() for x in (session_obj.get("section_rights") or []) if str(x).strip()]
    return "*" in rights or MODULE_KEY in rights
