import json
import re
from datetime import datetime

from modules import data_fetch


ASSET_STATUS_OPTIONS = [
    {"value": code, "label": label}
    for code, label in sorted(data_fetch.ASSET_STATUS_LABELS.items(), key=lambda item: item[0])
]

ASSIGNMENT_TYPE_OPTIONS = [
    {"value": "user", "label": "User"},
    {"value": "department", "label": "Department"},
    {"value": "location", "label": "Unit / Store"},
]

WARRANTY_BUCKET_OPTIONS = [
    {"value": "", "label": "All Warranty States"},
    {"value": "expired", "label": "Expired"},
    {"value": "due_30", "label": "Due in 30 Days"},
    {"value": "due_90", "label": "Due in 90 Days"},
    {"value": "covered", "label": "Covered"},
    {"value": "unknown", "label": "Unknown"},
]

COVERAGE_TYPE_OPTIONS = [
    {"value": "Warranty", "label": "Warranty"},
    {"value": "AMC", "label": "AMC"},
    {"value": "CMC", "label": "CMC"},
    {"value": "Not Covered", "label": "Not Covered"},
]

COVERAGE_ALERT_OPTIONS = [
    {"value": "", "label": "All Coverage Priorities"},
    {"value": "red", "label": "Red - Expired / Pending"},
    {"value": "orange", "label": "Orange - Due in 15 Days"},
    {"value": "yellow", "label": "Yellow - Due in 30 Days"},
    {"value": "blue", "label": "Blue - Due in 60/90 Days"},
    {"value": "green", "label": "Green - No Urgent Alert"},
]

SORT_OPTIONS = [
    {"value": "asset_code", "label": "Asset Code"},
    {"value": "equipment", "label": "Equipment"},
    {"value": "machine_type", "label": "Machine Type"},
    {"value": "manufacturer", "label": "Manufacturer"},
    {"value": "supplier", "label": "Supplier"},
    {"value": "invoice_date", "label": "Invoice Date"},
    {"value": "warranty_end", "label": "Warranty End"},
    {"value": "value", "label": "Asset Value"},
    {"value": "location", "label": "Location"},
    {"value": "status", "label": "Status"},
    {"value": "holder", "label": "Current Holder"},
    {"value": "updated", "label": "Recently Updated"},
    {"value": "created", "label": "Recently Added"},
]

REFERENCE_MASTER_OPTIONS = [
    {"value": "company", "label": "Company Master"},
    {"value": "manufacturer", "label": "Manufacturer Master"},
    {"value": "supplier", "label": "Supplier Master"},
    {"value": "equipment", "label": "Equipment Master"},
]

REFERENCE_MASTER_META = {
    "company": {
        "title": "Company Master",
        "subtitle": "Maintain company names from the shared asset master source used by asset entry.",
        "source": "manufacturer",
        "save_label": "Save Company",
        "empty_label": "No companies are available yet.",
        "success_label": "Company saved successfully.",
    },
    "manufacturer": {
        "title": "Manufacturer Master",
        "subtitle": "Maintain manufacturer records from the shared asset master source.",
        "source": "manufacturer",
        "save_label": "Save Manufacturer",
        "empty_label": "No manufacturers are available yet.",
        "success_label": "Manufacturer saved successfully.",
    },
    "supplier": {
        "title": "Supplier Master",
        "subtitle": "Maintain supplier records from the shared asset master source.",
        "source": "supplier",
        "save_label": "Save Supplier",
        "empty_label": "No suppliers are available yet.",
        "success_label": "Supplier saved successfully.",
    },
    "equipment": {
        "title": "Equipment Master",
        "subtitle": "Maintain equipment names from dbo.asset_equipment so asset entry stays complete and consistent.",
        "source": "equipment",
        "save_label": "Save Equipment",
        "empty_label": "No equipment records are available yet.",
        "success_label": "Equipment saved successfully.",
    },
}

DEFAULT_FILTERS = {
    "search": "",
    "machine_type": "",
    "equ_id": "",
    "brand_id": "",
    "supplier_id": "",
    "location_code": "",
    "status_code": "",
    "assignment_type": "",
    "warranty_bucket": "",
    "coverage_type": "",
    "coverage_alert": "",
    "sort": "asset_code",
    "direction": "asc",
    "page": 1,
    "page_size": 50,
}


def clean_text(value, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def to_int(value, default=None):
    if value in (None, ""):
        return default
    try:
        return int(float(str(value).strip()))
    except Exception:
        return default


def to_money(value, default=None):
    if value in (None, ""):
        return default
    try:
        text = str(value).strip().replace(",", "")
        if text.upper().startswith("RS."):
            text = text[3:].strip()
        amount = float(text)
        if amount < 0:
            return default
        return round(amount, 2)
    except Exception:
        return default


def normalize_warranty_years(value):
    text = clean_text(value)
    if not text:
        return ""
    match = re.search(r"-?\d+(\.\d+)?", text)
    if not match:
        return ""
    try:
        years = int(float(match.group(0)))
    except Exception:
        return ""
    if years < 0:
        return ""
    return str(years)


def normalize_date(value):
    text = clean_text(value)
    if not text:
        return ""
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    try:
        return datetime.fromisoformat(text).strftime("%Y-%m-%d")
    except Exception:
        return text


def has_explicit_unit_scope(units) -> bool:
    if units is None:
        return False
    if isinstance(units, str):
        return bool(units.strip())
    if isinstance(units, (list, tuple, set)):
        return bool(units)
    return True


def _unit_scope_items(units):
    if units is None:
        return []
    if isinstance(units, (list, tuple, set)):
        return list(units)
    if isinstance(units, str):
        text = units.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, (list, tuple, set)):
                return list(parsed)
            return [parsed]
        except Exception:
            return re.split(r"[,\|/]+", text)
    return [units]


def asset_scope_from_units(units):
    asset_order = ["AHL", "ACI", "BALLIA"]
    allowed = []
    seen = set()
    for unit in _unit_scope_items(units):
        code = clean_text(unit).upper()
        if not code or code in seen:
            continue
        if code == "*":
            return asset_order[:]
        if code not in data_fetch.ASSET_ALLOWED_LOCATION_CODES:
            continue
        seen.add(code)
        allowed.append(code)
    return allowed


def normalize_filters(source):
    source = source or {}
    filters = dict(DEFAULT_FILTERS)
    for key in (
        "search",
        "machine_type",
        "equ_id",
        "brand_id",
        "supplier_id",
        "location_code",
        "status_code",
        "assignment_type",
        "warranty_bucket",
        "coverage_type",
        "coverage_alert",
        "sort",
        "direction",
    ):
        filters[key] = clean_text(source.get(key), filters.get(key, ""))

    filters["location_code"] = filters["location_code"].upper()
    filters["coverage_alert"] = filters["coverage_alert"].lower()
    filters["direction"] = "desc" if filters["direction"].lower() == "desc" else "asc"
    filters["page"] = max(1, to_int(source.get("page"), 1) or 1)
    filters["page_size"] = min(max(to_int(source.get("page_size"), 50) or 50, 10), 5000)
    return filters


def build_asset_payload(source):
    source = source or {}
    return {
        "machine_type": to_int(source.get("machine_type")),
        "equ_id": to_int(source.get("equ_id")),
        "model_name": clean_text(source.get("model_name")),
        "brand_id": to_int(source.get("brand_id")),
        "serial_number": clean_text(source.get("serial_number")),
        "purchage_orderNo": clean_text(source.get("purchage_orderNo")),
        "purchage_orderDate": normalize_date(source.get("purchage_orderDate")),
        "supplier_id": to_int(source.get("supplier_id")),
        "invoice_number": clean_text(source.get("invoice_number")),
        "invoice_date": normalize_date(source.get("invoice_date")),
        "asset_value": to_money(source.get("asset_value")),
        "warranty": normalize_warranty_years(source.get("warranty")),
        "asset_status": to_int(source.get("asset_status"), 1) or 1,
        "locationId": to_int(source.get("locationId")),
        "status": to_int(source.get("status"), 1) or 1,
    }


def build_coverage_payload(source):
    source = source or {}
    return {
        "coverage_type": clean_text(source.get("coverage_type") or source.get("coverage_status")),
        "vendor": clean_text(source.get("coverage_vendor") or source.get("vendor")),
        "start_date": normalize_date(source.get("coverage_start_date") or source.get("start_date")),
        "expiry_date": normalize_date(source.get("coverage_expiry_date") or source.get("expiry_date")),
        "contract_no": clean_text(source.get("coverage_contract_no") or source.get("contract_no")),
        "remarks": clean_text(source.get("coverage_remarks") or source.get("remarks")),
        "document_file_id": to_int(source.get("coverage_document_file_id") or source.get("document_file_id")),
    }


def validate_coverage_payload(payload):
    payload = payload or {}
    coverage_type = clean_text(payload.get("coverage_type"))
    allowed = {item["value"] for item in COVERAGE_TYPE_OPTIONS}
    missing = []
    if coverage_type not in allowed:
        return ["Coverage Status"]
    if coverage_type == "Not Covered":
        if not clean_text(payload.get("remarks")):
            missing.append("Reason / Remarks")
        return missing
    for key, label in (
        ("vendor", "Vendor / Company"),
        ("start_date", "Coverage Start Date"),
        ("expiry_date", "Coverage Expiry Date"),
    ):
        if not clean_text(payload.get(key)):
            missing.append(label)
    return missing


def validate_asset_payload(payload, *, require_asset_value=True):
    payload = payload or {}
    missing = []
    required_fields = [
        ("machine_type", "Machine Type"),
        ("equ_id", "Equipment"),
        ("model_name", "Model Name"),
        ("brand_id", "Manufacturer"),
        ("supplier_id", "Supplier"),
        ("invoice_date", "Invoice Date"),
        ("warranty", "Warranty"),
        ("locationId", "Location"),
    ]
    if require_asset_value:
        required_fields.insert(6, ("asset_value", "Asset Value"))
    for key, label in required_fields:
        if payload.get(key) in (None, "", 0):
            missing.append(label)
    return missing


def build_maintenance_payload(source):
    source = source or {}
    return {
        "equ_id": to_int(source.get("equ_id")),
        "model_no": clean_text(source.get("model_no")),
        "serial_no": clean_text(source.get("serial_no")),
        "installation_date": normalize_date(source.get("installation_date")),
        "installation_location": clean_text(source.get("installation_location")),
        "warranty_start_date": normalize_date(source.get("warranty_start_date")),
        "warranty_end_date": normalize_date(source.get("warranty_end_date")),
        "installedby_name": clean_text(source.get("installedby_name")),
        "installedby_mobile": clean_text(source.get("installedby_mobile")),
        "amc_status": clean_text(source.get("amc_status")),
        "amc_date": normalize_date(source.get("amc_date")),
        "amc_value": clean_text(source.get("amc_value")),
        "amc_tax": clean_text(source.get("amc_tax")),
        "amc_amount": clean_text(source.get("amc_amount")),
        "service_eng_name": clean_text(source.get("service_eng_name")),
        "service_eng_mobile": clean_text(source.get("service_eng_mobile")),
        "service_eng_email": clean_text(source.get("service_eng_email")),
        "maint_status": to_int(source.get("maint_status"), 1) or 1,
    }


def build_lookup_maps(lookups):
    lookups = lookups or {}
    return {
        "users_by_key": {
            str(item.get("id")): item
            for item in (lookups.get("users") or [])
            if str(item.get("id") or "").strip()
        },
        "users_by_name": {
            clean_text(item.get("username")).lower(): item
            for item in (lookups.get("users") or [])
            if clean_text(item.get("username"))
        },
        "departments": {
            str(item.get("id")): item
            for item in (lookups.get("departments") or [])
            if str(item.get("id") or "").strip()
        },
        "locations_by_id": {
            str(item.get("id")): item
            for item in (lookups.get("locations") or [])
            if str(item.get("id") or "").strip()
        },
        "locations_by_code": {
            clean_text(item.get("code")).upper(): item
            for item in (lookups.get("locations") or [])
            if clean_text(item.get("code"))
        },
    }


def resolve_assignment(entity_type, entity_key, lookup_maps):
    entity_type = clean_text(entity_type).lower()
    entity_key_text = clean_text(entity_key)
    if not entity_type:
        raise ValueError("Please choose an assignment type.")
    if not entity_key_text:
        raise ValueError("Please choose a destination.")

    maps = lookup_maps or {}
    if entity_type == "user":
        user = maps.get("users_by_key", {}).get(entity_key_text) or maps.get("users_by_name", {}).get(entity_key_text.lower())
        if not user:
            raise ValueError("Selected user is not available for this scope.")
        return "user", clean_text(user.get("id") or user.get("username")), clean_text(user.get("username"))

    if entity_type == "department":
        dept = maps.get("departments", {}).get(entity_key_text)
        if not dept:
            raise ValueError("Selected department is not available.")
        return "department", clean_text(dept.get("id")), clean_text(dept.get("name"))

    if entity_type in {"location", "unit", "store"}:
        loc = maps.get("locations_by_id", {}).get(entity_key_text) or maps.get("locations_by_code", {}).get(entity_key_text.upper())
        if not loc:
            raise ValueError("Selected location is not available.")
        return "location", clean_text(loc.get("id")), clean_text(loc.get("name"))

    raise ValueError("Unsupported assignment type.")


def build_movement_payload(movement_type, source, current_asset, lookup_maps):
    movement_type_key = clean_text(movement_type).upper()
    if movement_type_key not in {"ISSUE", "RETURN", "TRANSFER", "STATUS"}:
        raise ValueError("Unsupported movement type.")

    current_asset = current_asset or {}
    status_after = to_int(source.get("status_after"), to_int(current_asset.get("asset_status_code"), 1) or 1) or 1
    location_id = to_int(source.get("location_id"), to_int(current_asset.get("locationId")))
    remarks = clean_text(source.get("remarks"))

    current_holder_type = clean_text(current_asset.get("current_holder_type"), "location").lower()
    current_holder_key = clean_text(current_asset.get("current_holder_key"), str(location_id or ""))
    current_holder_label = clean_text(current_asset.get("current_holder_label"), clean_text(current_asset.get("location_name")))

    payload = {
        "movement_type": movement_type_key,
        "status_after": status_after,
        "location_id": location_id,
        "remarks": remarks,
        "from_entity_type": current_holder_type,
        "from_entity_key": current_holder_key,
        "from_entity_label": current_holder_label,
    }

    if movement_type_key in {"ISSUE", "TRANSFER"}:
        entity_type, entity_key, entity_label = resolve_assignment(
            source.get("to_entity_type"),
            source.get("to_entity_key"),
            lookup_maps,
        )
        if entity_type == "location" and not location_id:
            location_id = to_int(entity_key)
            payload["location_id"] = location_id
        payload.update(
            {
                "to_entity_type": entity_type,
                "to_entity_key": entity_key,
                "to_entity_label": entity_label,
            }
        )
        if entity_type == "location":
            payload["location_id"] = to_int(entity_key, location_id)
        if movement_type_key == "ISSUE" and status_after == 1:
            payload["status_after"] = 2

    elif movement_type_key == "RETURN":
        entity_type, entity_key, entity_label = resolve_assignment(
            "location",
            source.get("to_entity_key") or source.get("location_id") or current_asset.get("locationId"),
            lookup_maps,
        )
        payload.update(
            {
                "to_entity_type": entity_type,
                "to_entity_key": entity_key,
                "to_entity_label": entity_label,
                "location_id": to_int(entity_key, location_id),
            }
        )
        if status_after == 2:
            payload["status_after"] = 3

    else:
        payload.update(
            {
                "to_entity_type": current_holder_type or "location",
                "to_entity_key": current_holder_key,
                "to_entity_label": current_holder_label,
            }
        )

    return payload


def filters_as_query(filters):
    filters = filters or {}
    out = {}
    for key in DEFAULT_FILTERS:
        value = filters.get(key)
        if value in (None, "", DEFAULT_FILTERS.get(key)):
            continue
        out[key] = value
    return out


def normalize_reference_master_kind(kind: str) -> str:
    key = clean_text(kind).lower()
    if key not in REFERENCE_MASTER_META:
        raise ValueError("Unsupported master type.")
    return key


def reference_master_meta(kind: str):
    return REFERENCE_MASTER_META[normalize_reference_master_kind(kind)]


def reference_master_source(kind: str) -> str:
    return reference_master_meta(kind).get("source") or "supplier"


def build_reference_master_payload(kind: str, source):
    key = normalize_reference_master_kind(kind)
    source = source or {}
    if reference_master_source(key) == "equipment":
        return {
            "id": to_int(source.get("id") or source.get("equipment_id"), 0) or 0,
            "code": clean_text(source.get("code")),
            "name": clean_text(source.get("name") or source.get("equ_name")),
            "machine_type_id": to_int(source.get("machine_type_id"), 0) or 0,
            "active": 1 if to_int(source.get("active"), 1) not in (0, None) else 0,
        }
    common = {
        "id": to_int(source.get("id") or source.get("manufacturer_id") or source.get("supplier_id"), 0) or 0,
        "code": clean_text(source.get("code")),
        "name": clean_text(source.get("name")),
        "address": clean_text(source.get("address")),
        "state_id": to_int(source.get("state_id"), 0) or 0,
        "city_id": to_int(source.get("city_id"), 0) or 0,
        "pin": clean_text(source.get("pin")),
        "contact_person": clean_text(source.get("contact_person")),
        "contact_designation": clean_text(source.get("contact_designation")),
        "phone1": clean_text(source.get("phone1")),
        "phone2": clean_text(source.get("phone2")),
        "cellphone": clean_text(source.get("cellphone")),
        "email": clean_text(source.get("email")),
        "web": clean_text(source.get("web")),
        "note": clean_text(source.get("note")),
        "society": clean_text(source.get("society")),
        "landmark": clean_text(source.get("landmark")),
    }
    if reference_master_source(key) == "manufacturer":
        common["active"] = 1 if to_int(source.get("active"), 1) not in (0, None) else 0
        return common
    common.update(
        {
            "credit_period": to_int(source.get("credit_period"), 0) or 0,
            "date_of_association": normalize_date(source.get("date_of_association")),
            "gst_cst": clean_text(source.get("gst_cst")),
            "tds": clean_text(source.get("tds")),
            "excise_code": clean_text(source.get("excise_code")),
        }
    )
    return common
