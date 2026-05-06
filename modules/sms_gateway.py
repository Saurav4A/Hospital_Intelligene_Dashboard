from __future__ import annotations

import json
import re
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import config


_SMS_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="sms-gateway")


def _config_text(name: str, default: str = "") -> str:
    return str(getattr(config, name, default) or "").strip()


def _config_bool(name: str, default: bool = False) -> bool:
    raw = getattr(config, name, default)
    if isinstance(raw, bool):
        return raw
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}


def _config_float(name: str, default: float = 8.0) -> float:
    try:
        return float(getattr(config, name, default) or default)
    except Exception:
        return float(default)


@dataclass
class SmsResult:
    status: str
    message: str
    mobile: str = ""
    provider: str = "prp_bulk_sms"
    http_status: int | None = None
    provider_message_id: str = ""
    response: Any = None

    @property
    def success(self) -> bool:
        return self.status == "success"

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "message": self.message,
            "mobile": self.mobile,
            "provider": self.provider,
            "http_status": self.http_status,
            "provider_message_id": self.provider_message_id,
            "response": self.response,
        }


def normalize_indian_mobile(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) > 10 and digits.startswith("91"):
        digits = digits[-10:]
    if len(digits) == 10 and digits[0] in {"6", "7", "8", "9"}:
        return digits
    return ""


def _money_text(value: Any) -> str:
    try:
        amount = Decimal(str(value or "0")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except Exception:
        amount = Decimal("0.00")
    return f"{amount:.2f}"


def _date_text(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%d-%b-%Y %I:%M %p")
    raw = str(value or "").strip()
    if not raw:
        return datetime.now().strftime("%d-%b-%Y %I:%M %p")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d-%b-%Y %I:%M %p", "%d-%m-%Y %H:%M:%S"):
        try:
            return datetime.strptime(raw[:19], fmt).strftime("%d-%b-%Y %I:%M %p")
        except Exception:
            continue
    return raw[:40]


class PrpBulkSmsClient:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        sender: str | None = None,
        template_url: str | None = None,
        timeout_seconds: float | None = None,
        user_agent: str | None = None,
    ):
        self.api_key = (api_key or _config_text("PRP_SMS_API_KEY")).strip()
        self.sender = (sender or _config_text("PRP_SMS_SENDER")).strip()
        self.template_url = (
            template_url
            or _config_text(
                "PRP_SMS_TEMPLATE_NAME_URL",
                "https://api.bulksmsadmin.com/BulkSMSapi/keyApiSendSMS/SendSmsTemplateName",
            )
        ).strip()
        self.timeout_seconds = float(timeout_seconds or _config_float("PRP_SMS_TIMEOUT_SECONDS", 8.0))
        self.user_agent = (
            user_agent
            or _config_text(
                "PRP_SMS_USER_AGENT",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36 HID/1.0",
            )
        ).strip()

    def send_template_sms(self, *, mobile: str, template_name: str, template_params: list[Any] | tuple[Any, ...]) -> SmsResult:
        normalized_mobile = normalize_indian_mobile(mobile)
        if not normalized_mobile:
            return SmsResult(status="skipped", message="No valid mobile number available.", mobile=str(mobile or "").strip())
        if not self.api_key:
            return SmsResult(status="skipped", message="SMS API key is not configured.", mobile=normalized_mobile)
        if not self.sender:
            return SmsResult(status="skipped", message="SMS sender/header is not configured.", mobile=normalized_mobile)
        template_name_text = str(template_name or "").strip()
        if not template_name_text:
            return SmsResult(status="skipped", message="SMS template name is not configured.", mobile=normalized_mobile)

        payload = {
            "sender": self.sender,
            "templateName": template_name_text,
            "smsReciever": [
                {
                    "mobileNo": normalized_mobile,
                    "templateParams": "~".join(str(part or "").strip() for part in template_params),
                }
            ],
        }
        request = Request(
            self.template_url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "apikey": self.api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read().decode("utf-8", errors="replace")
                http_status = int(getattr(response, "status", 0) or 0)
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            return SmsResult(
                status="error",
                message=f"SMS provider returned HTTP {exc.code}.",
                mobile=normalized_mobile,
                http_status=int(exc.code or 0),
                response=_parse_json_or_text(body),
            )
        except URLError as exc:
            return SmsResult(status="error", message=f"SMS provider connection failed: {exc}", mobile=normalized_mobile)
        except Exception as exc:
            return SmsResult(status="error", message=f"SMS send failed: {exc}", mobile=normalized_mobile)

        parsed = _parse_json_or_text(body)
        provider_status = ""
        provider_message = ""
        provider_message_id = ""
        provider_success = None
        if isinstance(parsed, dict):
            provider_status = str(parsed.get("status") or "").strip().lower()
            provider_message = str(parsed.get("message") or parsed.get("returnMessage") or "").strip()
            if "isSuccess" in parsed:
                provider_success = bool(parsed.get("isSuccess"))
            data = parsed.get("data") if isinstance(parsed.get("data"), dict) else {}
            provider_message_id = str(
                data.get("messageId")
                or data.get("msgId")
                or parsed.get("messageId")
                or parsed.get("token")
                or ""
            ).strip()
        if provider_success is not None:
            ok = 200 <= http_status < 300 and provider_success
        elif provider_status:
            ok = 200 <= http_status < 300 and provider_status == "success"
        else:
            ok = 200 <= http_status < 300
        return SmsResult(
            status="success" if ok else "error",
            message=provider_message or ("SMS sent successfully." if ok else "SMS provider did not confirm success."),
            mobile=normalized_mobile,
            http_status=http_status,
            provider_message_id=provider_message_id,
            response=parsed,
        )


def _parse_json_or_text(value: str) -> Any:
    try:
        return json.loads(value or "{}")
    except Exception:
        return value


def send_canteen_bill_sms(*, mobile: str, bill_no: str, amount: Any, bill_date: Any = None) -> SmsResult:
    if not _config_bool("ENABLE_CANTEEN_BILL_SMS", True):
        return SmsResult(status="skipped", message="Canteen bill SMS is disabled.", mobile=normalize_indian_mobile(mobile))
    template_name = _config_text("CANTEEN_BILL_SMS_TEMPLATE_NAME", "Canteen Bill Update")
    return PrpBulkSmsClient().send_template_sms(
        mobile=mobile,
        template_name=template_name,
        template_params=[str(bill_no or "").strip(), _money_text(amount), _date_text(bill_date)],
    )


def queue_canteen_bill_sms(*, mobile: str, bill_no: str, amount: Any, bill_date: Any = None) -> Future:
    return _SMS_EXECUTOR.submit(
        send_canteen_bill_sms,
        mobile=mobile,
        bill_no=bill_no,
        amount=amount,
        bill_date=bill_date,
    )
