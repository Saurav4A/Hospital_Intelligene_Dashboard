from __future__ import annotations


M1_CHECKLIST = [
    {
        "key": "credentials",
        "title": "Sandbox bridge credentials",
        "status": "ready_when_env_set",
        "detail": "Store client ID and secret in ABDM_CLIENT_ID / ABDM_CLIENT_SECRET.",
    },
    {
        "key": "public_https",
        "title": "Public HTTPS bridge URL",
        "status": "pending_infrastructure",
        "detail": "Expose this application or a small ABDM callback service on a valid HTTPS domain.",
    },
    {
        "key": "bridge_url",
        "title": "Update bridge URL",
        "status": "implemented",
        "detail": "Use /api/abdm/bridge/url after ABDM_BRIDGE_URL is configured.",
    },
    {
        "key": "services",
        "title": "Add HIP service",
        "status": "implemented",
        "detail": "Use /api/abdm/bridge/services after confirming service type from sandbox.",
    },
    {
        "key": "m1_tests",
        "title": "M1 test-case implementation",
        "status": "implemented",
        "detail": "ABHA V3 session, public certificate, encryption, OTP helper APIs, ABHA card download, and token-safe profile actions are implemented.",
    },
    {
        "key": "scan_share",
        "title": "Scan & Share callback linkage",
        "status": "implemented",
        "detail": "Profile share callbacks are stored in ACI, linked to patient/UHID references, and registered as local care contexts for M2 discovery.",
    },
]


def get_m1_checklist() -> list[dict]:
    return [dict(item) for item in M1_CHECKLIST]
