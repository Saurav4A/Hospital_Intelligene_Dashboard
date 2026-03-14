# ms_graph_it.py

import webbrowser
import os
import base64
import json
import msal

GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0"

# Separate token file so this login is bound to the noreply mailbox
TOKEN_PATH = "ms_graph_noreply_token.json"
NOREPLY_UPN = "noreply@asarfihospital.com"

# 🔹 Add this: your Directory (tenant) ID from Azure portal
TENANT_ID = "618946d2-c726-4906-9b60-820f56d7481d"   # e.g. "c0ffee00-1234-5678-9abc-0123456789ab"


def _decode_jwt_claims(token: str) -> dict:
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}
        payload = parts[1]
        padding = "=" * (-len(payload) % 4)
        decoded = base64.urlsafe_b64decode(payload + padding)
        return json.loads(decoded.decode("utf-8"))
    except Exception:
        return {}


def _extract_upn_from_result(result: dict) -> str:
    claims = result.get("id_token_claims") or {}
    upn = (
        claims.get("preferred_username")
        or claims.get("upn")
        or claims.get("unique_name")
        or claims.get("email")
    )
    if upn:
        return str(upn).strip().lower()
    token_claims = _decode_jwt_claims(result.get("access_token") or "")
    upn = (
        token_claims.get("preferred_username")
        or token_claims.get("upn")
        or token_claims.get("unique_name")
        or token_claims.get("email")
    )
    return str(upn).strip().lower() if upn else ""


def _purge_token_cache():
    try:
        if os.path.exists(TOKEN_PATH):
            os.remove(TOKEN_PATH)
    except Exception:
        pass


def _ensure_noreply_account(result: dict):
    upn = _extract_upn_from_result(result)
    if upn and upn != NOREPLY_UPN:
        _purge_token_cache()
        raise Exception("Logged into the wrong mailbox. Please sign in as noreply@asarfihospital.com.")


def generate_access_token(app_id, scopes):
    """
    Device-code auth for noreply@asarfihospital.com.
    First run will ask you to sign in as that user.
    After that it will silently reuse the cached token.
    """
    token_cache = msal.SerializableTokenCache()

    # Load cache if exists
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, "r") as f:
            token_cache.deserialize(f.read())

    # 🔹 IMPORTANT: pass authority with your tenant
    client = msal.PublicClientApplication(
        client_id=app_id,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        token_cache=token_cache,
    )

    # Try from cache
    accounts = client.get_accounts(username=NOREPLY_UPN) or []
    result = client.acquire_token_silent(scopes, account=accounts[0]) if accounts else None
    if result and "access_token" in result:
        _ensure_noreply_account(result)

    # If nothing in cache → device code flow
    if not result or "access_token" not in result:
        flow = client.initiate_device_flow(scopes=scopes)
        if "user_code" not in flow:
            raise Exception(f"Failed to create device flow: {flow}")

        print("\n=== LOGIN FOR noreply@asarfihospital.com ===")
        print("Use this code when browser opens:")
        print("user_code:", flow["user_code"])
        print("Make sure you sign in as: noreply@asarfihospital.com")
        print("===========================================\n")

        webbrowser.open(flow.get("verification_uri", "https://microsoft.com/devicelogin"))
        result = client.acquire_token_by_device_flow(flow)
        _ensure_noreply_account(result)

    if "access_token" not in result:
        raise Exception(f"Could not obtain access token: {result}")

    # Save updated cache
    with open(TOKEN_PATH, "w") as f:
        f.write(token_cache.serialize())

    return result


def get_graph_headers(app_id, scopes):
    token = generate_access_token(app_id=app_id, scopes=scopes)
    return {
        "Authorization": f"Bearer {token['access_token']}",
        "Content-Type": "application/json",
    }
