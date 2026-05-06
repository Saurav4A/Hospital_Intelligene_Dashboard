from flask import jsonify, render_template, request, session
from datetime import datetime

from modules import data_fetch


def register_purchase_master_routes(
    app,
    *,
    login_required,
    allowed_purchase_units_for_session,
    role_base,
    can_use_def_po_print_format,
    can_use_trust_po_print_format,
    get_purchase_unit,
    build_purchase_department_payload,
    clean_df_columns,
    normalize_purchase_unit_text,
    sanitize_json_payload,
    purchase_dept_master_permissions,
    safe_int,
    upsert_purchasing_department,
    build_purchase_department_master_rows,
    set_purchasing_department_active,
    delete_purchasing_department,
    purchase_unit_master_permissions,
    purchase_normalize_email,
    purchase_is_valid_email,
    build_master_list,
    row_value,
    build_supplier_params,
    build_manufacturer_params,
    load_item_master_lists,
    build_item_master_params,
    build_item_update_params,
    build_purchase_unit_master_rows,
    mirror_new_supplier_master_to_family,
    build_replication_message,
    unit_allows_zero_rate_mrp,
    ensure_purchase_unit_master_entry,
    mirror_new_item_master_to_family,
    item_master_descriptive_name_max,
    item_master_default_medicine_type_id,
    local_tz,
    safe_float,
    audit_log_event,
    invalidate_item_master_meta_cache,
    invalidate_item_name_cache,
):
    """Register the Purchase master/setup routes."""
    _allowed_purchase_units_for_session = allowed_purchase_units_for_session
    _role_base = role_base
    _can_use_def_po_print_format = can_use_def_po_print_format
    _can_use_trust_po_print_format = can_use_trust_po_print_format
    _get_purchase_unit = get_purchase_unit
    _build_purchase_department_payload = build_purchase_department_payload
    _clean_df_columns = clean_df_columns
    _normalize_purchase_unit_text = normalize_purchase_unit_text
    _sanitize_json_payload = sanitize_json_payload
    _purchase_dept_master_permissions = purchase_dept_master_permissions
    _safe_int = safe_int
    _upsert_purchasing_department = upsert_purchasing_department
    _build_purchase_department_master_rows = build_purchase_department_master_rows
    _set_purchasing_department_active = set_purchasing_department_active
    _delete_purchasing_department = delete_purchasing_department
    _purchase_unit_master_permissions = purchase_unit_master_permissions
    _purchase_normalize_email = purchase_normalize_email
    _purchase_is_valid_email = purchase_is_valid_email
    _build_master_list = build_master_list
    _row_value = row_value
    _build_supplier_params = build_supplier_params
    _build_manufacturer_params = build_manufacturer_params
    _load_item_master_lists = load_item_master_lists
    _build_item_master_params = build_item_master_params
    _build_item_update_params = build_item_update_params
    _build_purchase_unit_master_rows = build_purchase_unit_master_rows
    _mirror_new_supplier_master_to_family = mirror_new_supplier_master_to_family
    _build_replication_message = build_replication_message
    _unit_allows_zero_rate_mrp = unit_allows_zero_rate_mrp
    _ensure_purchase_unit_master_entry = ensure_purchase_unit_master_entry
    _mirror_new_item_master_to_family = mirror_new_item_master_to_family
    ITEM_MASTER_DESCRIPTIVE_NAME_MAX = item_master_descriptive_name_max
    ITEM_MASTER_DEFAULT_MEDICINE_TYPE_ID = _safe_int(item_master_default_medicine_type_id)
    LOCAL_TZ = local_tz
    _safe_float = safe_float
    _audit_log_event = audit_log_event
    _invalidate_item_master_meta_cache = invalidate_item_master_meta_cache
    _invalidate_item_name_cache = invalidate_item_name_cache
    CONTRACTED_RATE_UNITS = {"AHL", "ACI", "BALLIA"}

    def _supports_contracted_rate(unit: str) -> bool:
        return str(unit or "").strip().upper() in CONTRACTED_RATE_UNITS

    def _master_flag_is_active(raw_val, fallback: bool = True) -> bool:
        if raw_val in (None, "", []):
            return bool(fallback)
        if isinstance(raw_val, bool):
            return raw_val
        text = str(raw_val).strip().lower()
        if text in {"1", "true", "yes", "y"}:
            return True
        if text in {"0", "false", "no", "n"}:
            return False
        try:
            return int(raw_val) != 0
        except Exception:
            return bool(fallback)

    def _normalize_master_name(value) -> str:
        return " ".join(str(value or "").strip().lower().split())

    def _build_item_type_master_rows(unit: str, include_inactive: bool = True):
        df = data_fetch.fetch_item_type_master(unit, include_inactive=include_inactive)
        rows = _build_master_list(
            df,
            ["id"],
            ["name"],
            ["code"],
            extra_keys={"is_active": ["isactive"]},
        )
        return [
            {
                "id": _safe_int(row.get("id")),
                "code": str(row.get("code") or "").strip(),
                "name": str(row.get("name") or "").strip(),
                "is_active": 1 if _master_flag_is_active(row.get("is_active"), True) else 0,
            }
            for row in (rows or [])
            if _safe_int(row.get("id")) > 0 and str(row.get("name") or "").strip()
        ]

    def _build_item_group_master_rows(unit: str, include_inactive: bool = True, type_id: int | None = None):
        df = data_fetch.fetch_item_group_master(unit, include_inactive=include_inactive, type_id=type_id)
        rows = _build_master_list(
            df,
            ["id"],
            ["name"],
            ["code"],
            extra_keys={
                "type_id": ["typeid"],
                "type_name": ["typename"],
                "type_code": ["typecode"],
                "is_active": ["isactive"],
            },
        )
        return [
            {
                "id": _safe_int(row.get("id")),
                "code": str(row.get("code") or "").strip(),
                "name": str(row.get("name") or "").strip(),
                "type_id": _safe_int(row.get("type_id")),
                "type_name": str(row.get("type_name") or "").strip(),
                "type_code": str(row.get("type_code") or "").strip(),
                "is_active": 1 if _master_flag_is_active(row.get("is_active"), True) else 0,
            }
            for row in (rows or [])
            if _safe_int(row.get("id")) > 0 and str(row.get("name") or "").strip()
        ]

    def _build_item_subgroup_master_rows(
        unit: str,
        include_inactive: bool = True,
        group_id: int | None = None,
        type_id: int | None = None,
    ):
        df = data_fetch.fetch_item_subgroup_master(
            unit,
            include_inactive=include_inactive,
            group_id=group_id,
            type_id=type_id,
        )
        rows = _build_master_list(
            df,
            ["id"],
            ["name"],
            ["code"],
            extra_keys={
                "group_id": ["groupid"],
                "group_name": ["groupname"],
                "type_id": ["typeid"],
                "type_name": ["typename"],
                "is_active": ["isactive"],
            },
        )
        return [
            {
                "id": _safe_int(row.get("id")),
                "code": str(row.get("code") or "").strip(),
                "name": str(row.get("name") or "").strip(),
                "group_id": _safe_int(row.get("group_id")),
                "group_name": str(row.get("group_name") or "").strip(),
                "type_id": _safe_int(row.get("type_id")),
                "type_name": str(row.get("type_name") or "").strip(),
                "is_active": 1 if _master_flag_is_active(row.get("is_active"), True) else 0,
            }
            for row in (rows or [])
            if _safe_int(row.get("id")) > 0 and str(row.get("name") or "").strip()
        ]

    def _active_master_rows(rows):
        return [row for row in (rows or []) if _master_flag_is_active(row.get("is_active"), True)]

    def _resolve_verified_item_master_id(unit: str, item_id: int | None, item_name: str) -> int:
        resolved_item_id = _safe_int(item_id)
        normalized_name = _normalize_master_name(item_name)

        def _item_exists(candidate_id: int) -> bool:
            if candidate_id <= 0:
                return False
            detail_df = data_fetch.fetch_item_master_detail(unit, candidate_id)
            return detail_df is not None and not detail_df.empty

        if _item_exists(resolved_item_id):
            return resolved_item_id

        try:
            data_fetch.invalidate_item_master_query_cache(unit)
        except Exception:
            pass
        try:
            _invalidate_item_master_meta_cache(unit)
        except Exception:
            pass
        try:
            _invalidate_item_name_cache(unit)
        except Exception:
            pass

        if _item_exists(resolved_item_id):
            return resolved_item_id

        if not normalized_name:
            return 0

        detail_by_name_df = data_fetch.fetch_item_master_detail_by_exact_name(unit, item_name)
        if detail_by_name_df is not None and not detail_by_name_df.empty:
            detail_by_name_df = _clean_df_columns(detail_by_name_df)
            saved_row = detail_by_name_df.iloc[0]
            saved_name = str(saved_row.get("Name") or "").strip()
            saved_item_id = _safe_int(saved_row.get("ID") or saved_row.get("Id"))
            if saved_item_id > 0 and _normalize_master_name(saved_name) == normalized_name:
                return saved_item_id

        list_df = data_fetch.fetch_item_master_list(unit)
        if list_df is None or list_df.empty:
            return 0

        rows = _build_master_list(list_df, ["id"], ["name"], ["code"])
        for row in rows or []:
            if _normalize_master_name(row.get("name")) != normalized_name:
                continue
            candidate_id = _safe_int(row.get("id"))
            if _item_exists(candidate_id):
                return candidate_id
        return 0

    @app.route('/purchase')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def purchase_module():
        allowed_units = _allowed_purchase_units_for_session()
        role = (session.get("role") or "").strip()
        return (
            render_template(
                'purchase.html',
                allowed_units=allowed_units,
                is_executive=_role_base(role) == "Executive",
                can_use_po_def_format=_can_use_def_po_print_format("AHLSTORE"),
                can_use_po_trust_format=_can_use_trust_po_print_format("AHLSTORE"),
            ),
            200,
            {
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )

    @app.route('/api/purchase/init')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_init():
        unit, error = _get_purchase_unit()
        if error:
            return error

        try:
            data_fetch.ensure_iv_item_technical_specs_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_purchasing_dept_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_special_notes_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_cmc_amc_warranty_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_overall_discount_mode_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_overall_discount_value_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_print_format_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_senior_approval_authority_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_po_senior_approval_designation_column(unit)
        except Exception:
            pass
        try:
            data_fetch.ensure_iv_supplier_email_column(unit)
        except Exception:
            pass

        # Avoid reserving PO numbers on page load; assign on save instead.
        po_no = None
        against_df = data_fetch.fetch_purchase_against(unit)
        pack_df = data_fetch.fetch_purchase_pack_sizes(unit)
        unit_df = data_fetch.fetch_purchase_unit_master(unit, include_inactive=False)
        supplier_df = data_fetch.fetch_iv_suppliers(unit)
        indent_count = data_fetch.fetch_new_pharmacy_indent_count(unit)
        purchasing_departments_payload, default_purchasing_dept_id = _build_purchase_department_payload(unit)

        against = []
        if against_df is not None and not against_df.empty:
            against_df = _clean_df_columns(against_df)
            cols_map = {str(c).strip().lower(): c for c in against_df.columns}
            id_col = cols_map.get("id")
            name_col = cols_map.get("name")
            if id_col and name_col:
                for _, row in against_df.iterrows():
                    against.append({
                        "id": row.get(id_col),
                        "name": str(row.get(name_col) or "").strip()
                    })

        pack_sizes = []
        if pack_df is not None and not pack_df.empty:
            pack_df = _clean_df_columns(pack_df)
            cols_map = {str(c).strip().lower(): c for c in pack_df.columns}
            id_col = cols_map.get("id")
            name_col = cols_map.get("name")
            if id_col and name_col:
                for _, row in pack_df.iterrows():
                    pack_sizes.append({
                        "id": row.get(id_col),
                        "name": str(row.get(name_col) or "").strip()
                    })

        units = []
        if unit_df is not None and not unit_df.empty:
            unit_df = _clean_df_columns(unit_df)
            cols_map = {str(c).strip().lower(): c for c in unit_df.columns}
            id_col = cols_map.get("id")
            name_col = cols_map.get("name")
            code_col = cols_map.get("code")
            for _, row in unit_df.iterrows():
                name_val = _normalize_purchase_unit_text(row.get(name_col) if name_col else "")
                code_val = _normalize_purchase_unit_text(row.get(code_col) if code_col else "")
                if not name_val and not code_val:
                    continue
                if not name_val:
                    name_val = code_val
                units.append(
                    {
                        "id": row.get(id_col) if id_col else None,
                        "code": code_val,
                        "name": name_val,
                        "unit": unit,
                    }
                )

        suppliers = []
        if supplier_df is not None and not supplier_df.empty:
            supplier_df = _clean_df_columns(supplier_df)
            cols_map = {str(c).strip().lower(): c for c in supplier_df.columns}
            id_col = cols_map.get("supplierid") or cols_map.get("id")
            name_col = cols_map.get("suppliername") or cols_map.get("name")
            code_col = cols_map.get("suppliercode") or cols_map.get("code")
            email_col = (
                cols_map.get("email")
                or cols_map.get("emailid")
                or cols_map.get("email_id")
                or cols_map.get("e_mail")
                or cols_map.get("e_mail_id")
            )
            for _, row in supplier_df.iterrows():
                suppliers.append({
                    "id": row.get(id_col),
                    "name": str(row.get(name_col) or "").strip(),
                    "code": str(row.get(code_col) or "").strip(),
                    "email": str(row.get(email_col) or "").strip(),
                })

        return jsonify({
            "status": "success",
            "unit": unit,
            "po_no": po_no,
            "against": against,
            "pack_sizes": pack_sizes,
            "units": _sanitize_json_payload(units),
            "suppliers": suppliers,
            "indent_count": int(indent_count or 0),
            "purchasing_departments": _sanitize_json_payload(purchasing_departments_payload),
            "default_purchasing_dept_id": default_purchasing_dept_id,
        })


    @app.route('/api/purchase/po_valuation_filters')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_po_valuation_filters():
        unit, error = _get_purchase_unit()
        if error:
            return error

        purchasing_departments_payload, default_purchasing_dept_id = _build_purchase_department_payload(unit)
        return jsonify({
            "status": "success",
            "unit": unit,
            "purchasing_departments": _sanitize_json_payload(purchasing_departments_payload),
            "default_purchasing_dept_id": default_purchasing_dept_id,
        })


    @app.route('/api/purchase/purchasing_dept_master/list')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_purchasing_dept_master_list():
        unit, error = _get_purchase_unit()
        if error:
            return error
        role_base = _role_base(session.get("role") or "")
        departments, default_id = _build_purchase_department_payload(unit)
        master_rows = _build_purchase_department_master_rows()
        return jsonify({
            "status": "success",
            "unit": unit,
            "departments": _sanitize_json_payload(master_rows),
            "active_departments": _sanitize_json_payload(departments),
            "default_purchasing_dept_id": default_id,
            "permissions": _purchase_dept_master_permissions(role_base),
        })


    @app.route('/api/purchase/purchasing_dept_master', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_purchasing_dept_master_save():
        unit, error = _get_purchase_unit()
        if error:
            return error
        role_base = _role_base(session.get("role") or "")
        if role_base == "Executive":
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403
        payload = request.get_json(silent=True) or {}
        dept_payload = payload.get("purchasing_dept") or {}
        dept_id = _safe_int(dept_payload.get("id"))
        dept_name = str(dept_payload.get("name") or "").strip()
        if not dept_name:
            return jsonify({"status": "error", "message": "Purchasing department name is required."}), 400
        result = _upsert_purchasing_department(dept_name, dept_id if dept_id > 0 else None)
        if result.get("error"):
            code = str(result.get("code") or "").strip().lower()
            if code == "duplicate":
                return jsonify({
                    "status": "error",
                    "message": result.get("error") or "Purchasing department already exists.",
                    "existing_id": result.get("existing_id"),
                }), 409
            if code == "not_found":
                return jsonify({"status": "error", "message": result.get("error") or "Purchasing department not found."}), 404
            return jsonify({"status": "error", "message": result.get("error") or "Failed to save purchasing department."}), 500
        departments, default_id = _build_purchase_department_payload(unit)
        master_rows = _build_purchase_department_master_rows()
        return jsonify({
            "status": "success",
            "mode": result.get("mode") or ("update" if dept_id > 0 else "add"),
            "department_id": result.get("dept_id"),
            "department_name": result.get("name"),
            "departments": _sanitize_json_payload(master_rows),
            "active_departments": _sanitize_json_payload(departments),
            "default_purchasing_dept_id": default_id,
            "permissions": _purchase_dept_master_permissions(role_base),
            "unit": unit,
        })


    @app.route('/api/purchase/purchasing_dept_master/action', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_purchasing_dept_master_action():
        unit, error = _get_purchase_unit()
        if error:
            return error
        role_base = _role_base(session.get("role") or "")
        if role_base == "Executive":
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403

        payload = request.get_json(silent=True) or {}
        action = str(payload.get("action") or "").strip().lower()
        dept_id = _safe_int(payload.get("department_id"))
        if dept_id <= 0:
            return jsonify({"status": "error", "message": "Department id is required."}), 400
        if action not in {"activate", "deactivate", "delete"}:
            return jsonify({"status": "error", "message": "Invalid action."}), 400

        if action == "delete" and role_base != "IT":
            return jsonify({"status": "error", "message": "Only IT can delete purchasing departments."}), 403
        if action in {"activate", "deactivate"} and role_base not in {"IT", "Management", "Departmental Head"}:
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403

        if action == "delete":
            result = _delete_purchasing_department(dept_id)
        else:
            result = _set_purchasing_department_active(dept_id, is_active=(action == "activate"))

        if result.get("error"):
            code = str(result.get("code") or "").strip().lower()
            if code == "not_found":
                return jsonify({"status": "error", "message": result.get("error") or "Purchasing department not found."}), 404
            if code == "invalid_id":
                return jsonify({"status": "error", "message": result.get("error") or "Invalid purchasing department id."}), 400
            if code == "has_dependency":
                incharge_count = int(result.get("incharge_count") or 0)
                po_count = int(result.get("po_count") or 0)
                parts = []
                if incharge_count > 0:
                    parts.append(f"mapped in {incharge_count} incharge record(s)")
                if po_count > 0:
                    parts.append(f"used in {po_count} PO record(s)")
                detail = " and ".join(parts) if parts else "already mapped"
                return jsonify({
                    "status": "error",
                    "message": f"Cannot delete this department because it is {detail}.",
                    "incharge_count": incharge_count,
                    "po_count": po_count,
                    "po_usage": result.get("po_usage") or [],
                }), 409
            if code == "dependency_check_failed":
                failed_units = ", ".join(result.get("failed_units") or [])
                msg = "Dependency check failed for one or more units."
                if failed_units:
                    msg = f"Dependency check failed for units: {failed_units}."
                return jsonify({
                    "status": "error",
                    "message": msg,
                    "failed_units": result.get("failed_units") or [],
                }), 503
            return jsonify({"status": "error", "message": result.get("error") or "Action failed."}), 500

        departments, default_id = _build_purchase_department_payload(unit)
        master_rows = _build_purchase_department_master_rows()
        return jsonify({
            "status": "success",
            "action": action,
            "department_id": dept_id,
            "departments": _sanitize_json_payload(master_rows),
            "active_departments": _sanitize_json_payload(departments),
            "default_purchasing_dept_id": default_id,
            "permissions": _purchase_dept_master_permissions(role_base),
            "unit": unit,
        })


    @app.route('/api/purchase/unit_master/list')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_unit_master_list():
        unit, error = _get_purchase_unit()
        if error:
            return error
        role_base = _role_base(session.get("role") or "")
        master_rows = _build_purchase_unit_master_rows(unit)
        active_rows = [row for row in master_rows if _safe_int(row.get("is_active"), 1) == 1]
        return jsonify(
            {
                "status": "success",
                "unit": unit,
                "units": _sanitize_json_payload(master_rows),
                "active_units": _sanitize_json_payload(active_rows),
                "permissions": _purchase_unit_master_permissions(role_base),
            }
        )


    @app.route('/api/purchase/unit_master', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_unit_master_save():
        unit, error = _get_purchase_unit()
        if error:
            return error
        role_base = _role_base(session.get("role") or "")
        if role_base == "Executive":
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403

        payload = request.get_json(silent=True) or {}
        unit_payload = payload.get("unit_master") or {}
        unit_id = _safe_int(unit_payload.get("id"))
        unit_name = _normalize_purchase_unit_text(unit_payload.get("name"))
        unit_code = _normalize_purchase_unit_text(unit_payload.get("code")).upper()
        if not unit_name:
            return jsonify({"status": "error", "message": "Unit name is required."}), 400

        result = data_fetch.upsert_purchase_unit_master(
            unit,
            unit_name=unit_name,
            unit_code=unit_code,
            unit_id=unit_id if unit_id > 0 else None,
            reactivate=True,
        )
        if result.get("error"):
            code = str(result.get("code") or "").strip().lower()
            if code == "duplicate":
                return jsonify(
                    {
                        "status": "error",
                        "message": result.get("error") or "Unit already exists.",
                        "existing_id": result.get("existing_id"),
                    }
                ), 409
            if code == "not_found":
                return jsonify({"status": "error", "message": result.get("error") or "Unit not found."}), 404
            if code == "table_not_found":
                return jsonify({"status": "error", "message": "Unit master table not found in selected unit database."}), 500
            return jsonify({"status": "error", "message": result.get("error") or "Failed to save unit."}), 500

        master_rows = _build_purchase_unit_master_rows(unit)
        active_rows = [row for row in master_rows if _safe_int(row.get("is_active"), 1) == 1]
        return jsonify(
            {
                "status": "success",
                "mode": result.get("mode") or ("update" if unit_id > 0 else "add"),
                "unit_id": result.get("unit_id"),
                "unit_name": result.get("name") or unit_name,
                "unit_code": result.get("code") or unit_code,
                "units": _sanitize_json_payload(master_rows),
                "active_units": _sanitize_json_payload(active_rows),
                "permissions": _purchase_unit_master_permissions(role_base),
                "unit": unit,
            }
        )


    @app.route('/api/purchase/unit_master/action', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_unit_master_action():
        unit, error = _get_purchase_unit()
        if error:
            return error
        role_base = _role_base(session.get("role") or "")
        if role_base == "Executive":
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403

        payload = request.get_json(silent=True) or {}
        action = str(payload.get("action") or "").strip().lower()
        unit_id = _safe_int(payload.get("unit_id"))
        if unit_id <= 0:
            return jsonify({"status": "error", "message": "Unit id is required."}), 400
        if action not in {"activate", "deactivate"}:
            return jsonify({"status": "error", "message": "Invalid action."}), 400
        if role_base not in {"IT", "Management", "Departmental Head"}:
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403

        result = data_fetch.set_purchase_unit_master_active(unit, unit_id, is_active=(action == "activate"))
        if result.get("error"):
            code = str(result.get("code") or "").strip().lower()
            if code == "not_found":
                return jsonify({"status": "error", "message": result.get("error") or "Unit not found."}), 404
            if code == "invalid_id":
                return jsonify({"status": "error", "message": result.get("error") or "Invalid unit id."}), 400
            if code == "unsupported":
                return jsonify({"status": "error", "message": "Selected unit database does not support activate/deactivate."}), 501
            return jsonify({"status": "error", "message": result.get("error") or "Action failed."}), 500

        master_rows = _build_purchase_unit_master_rows(unit)
        active_rows = [row for row in master_rows if _safe_int(row.get("is_active"), 1) == 1]
        return jsonify(
            {
                "status": "success",
                "action": action,
                "unit_id": unit_id,
                "units": _sanitize_json_payload(master_rows),
                "active_units": _sanitize_json_payload(active_rows),
                "permissions": _purchase_unit_master_permissions(role_base),
                "unit": unit,
            }
        )


    @app.route('/api/purchase/item_type_master/list')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_type_master_list():
        unit, error = _get_purchase_unit()
        if error:
            return error
        role_base = _role_base(session.get("role") or "")
        master_rows = _build_item_type_master_rows(unit, include_inactive=True)
        active_rows = _active_master_rows(master_rows)
        return jsonify(
            {
                "status": "success",
                "unit": unit,
                "item_types": _sanitize_json_payload(master_rows),
                "active_item_types": _sanitize_json_payload(active_rows),
                "permissions": _purchase_unit_master_permissions(role_base),
            }
        )


    @app.route('/api/purchase/item_type_master', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_type_master_save():
        unit, error = _get_purchase_unit()
        if error:
            return error
        role_base = _role_base(session.get("role") or "")
        if role_base == "Executive":
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403

        payload = request.get_json(silent=True) or {}
        item_type_payload = payload.get("item_type_master") or {}
        item_type_id = _safe_int(item_type_payload.get("id"))
        item_type_name = str(item_type_payload.get("name") or "").strip()
        item_type_code = _normalize_purchase_unit_text(item_type_payload.get("code") or "").upper()
        if not item_type_name:
            return jsonify({"status": "error", "message": "Item type name is required."}), 400

        result = data_fetch.upsert_item_type_master(
            unit,
            item_type_name=item_type_name,
            item_type_code=item_type_code,
            item_type_id=item_type_id if item_type_id > 0 else None,
            reactivate=True,
        )
        if result.get("error"):
            code = str(result.get("code") or "").strip().lower()
            if code == "duplicate":
                return jsonify(
                    {
                        "status": "error",
                        "message": result.get("error") or "Item type already exists.",
                        "existing_id": result.get("existing_id"),
                    }
                ), 409
            if code == "not_found":
                return jsonify({"status": "error", "message": result.get("error") or "Item type not found."}), 404
            if code == "table_not_found":
                return jsonify({"status": "error", "message": "Item type master table not found in selected unit database."}), 500
            return jsonify({"status": "error", "message": result.get("error") or "Failed to save item type."}), 500

        master_rows = _build_item_type_master_rows(unit, include_inactive=True)
        active_rows = _active_master_rows(master_rows)
        return jsonify(
            {
                "status": "success",
                "mode": result.get("mode") or ("update" if item_type_id > 0 else "add"),
                "item_type_id": result.get("record_id"),
                "item_type_name": result.get("name") or item_type_name,
                "item_type_code": result.get("code") or item_type_code,
                "item_types": _sanitize_json_payload(master_rows),
                "active_item_types": _sanitize_json_payload(active_rows),
                "permissions": _purchase_unit_master_permissions(role_base),
                "unit": unit,
            }
        )


    @app.route('/api/purchase/item_type_master/action', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_type_master_action():
        unit, error = _get_purchase_unit()
        if error:
            return error
        role_base = _role_base(session.get("role") or "")
        if role_base == "Executive":
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403

        payload = request.get_json(silent=True) or {}
        action = str(payload.get("action") or "").strip().lower()
        item_type_id = _safe_int(payload.get("item_type_id"))
        if item_type_id <= 0:
            return jsonify({"status": "error", "message": "Item type id is required."}), 400
        if action not in {"activate", "deactivate"}:
            return jsonify({"status": "error", "message": "Invalid action."}), 400

        result = data_fetch.set_item_type_master_active(unit, item_type_id, is_active=(action == "activate"))
        if result.get("error"):
            code = str(result.get("code") or "").strip().lower()
            if code == "not_found":
                return jsonify({"status": "error", "message": result.get("error") or "Item type not found."}), 404
            if code == "invalid_id":
                return jsonify({"status": "error", "message": result.get("error") or "Invalid item type id."}), 400
            return jsonify({"status": "error", "message": result.get("error") or "Action failed."}), 500

        master_rows = _build_item_type_master_rows(unit, include_inactive=True)
        active_rows = _active_master_rows(master_rows)
        return jsonify(
            {
                "status": "success",
                "action": action,
                "item_type_id": item_type_id,
                "item_types": _sanitize_json_payload(master_rows),
                "active_item_types": _sanitize_json_payload(active_rows),
                "permissions": _purchase_unit_master_permissions(role_base),
                "unit": unit,
            }
        )


    @app.route('/api/purchase/item_group_master/list')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_group_master_list():
        unit, error = _get_purchase_unit()
        if error:
            return error
        role_base = _role_base(session.get("role") or "")
        master_rows = _build_item_group_master_rows(unit, include_inactive=True)
        active_rows = _active_master_rows(master_rows)
        active_item_types = _build_item_type_master_rows(unit, include_inactive=False)
        return jsonify(
            {
                "status": "success",
                "unit": unit,
                "item_groups": _sanitize_json_payload(master_rows),
                "active_item_groups": _sanitize_json_payload(active_rows),
                "item_types": _sanitize_json_payload(active_item_types),
                "permissions": _purchase_unit_master_permissions(role_base),
            }
        )


    @app.route('/api/purchase/item_group_master', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_group_master_save():
        unit, error = _get_purchase_unit()
        if error:
            return error
        role_base = _role_base(session.get("role") or "")
        if role_base == "Executive":
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403

        payload = request.get_json(silent=True) or {}
        group_payload = payload.get("item_group_master") or {}
        group_id = _safe_int(group_payload.get("id"))
        group_name = str(group_payload.get("name") or "").strip()
        group_code = _normalize_purchase_unit_text(group_payload.get("code") or "").upper()
        type_id = _safe_int(group_payload.get("type_id"))
        if not group_name:
            return jsonify({"status": "error", "message": "Item group name is required."}), 400
        if type_id <= 0:
            return jsonify({"status": "error", "message": "Item type selection is required."}), 400

        active_item_types = _build_item_type_master_rows(unit, include_inactive=False)
        if not any(_safe_int(row.get("id")) == type_id for row in active_item_types):
            return jsonify({"status": "error", "message": "Selected item type was not found."}), 400

        result = data_fetch.upsert_item_group_master(
            unit,
            group_name=group_name,
            group_code=group_code,
            parent_type_id=type_id,
            group_id=group_id if group_id > 0 else None,
            reactivate=True,
        )
        if result.get("error"):
            code = str(result.get("code") or "").strip().lower()
            if code == "duplicate":
                return jsonify(
                    {
                        "status": "error",
                        "message": result.get("error") or "Item group already exists.",
                        "existing_id": result.get("existing_id"),
                    }
                ), 409
            if code == "not_found":
                return jsonify({"status": "error", "message": result.get("error") or "Item group not found."}), 404
            if code == "required_parent":
                return jsonify({"status": "error", "message": "Item type selection is required."}), 400
            return jsonify({"status": "error", "message": result.get("error") or "Failed to save item group."}), 500

        master_rows = _build_item_group_master_rows(unit, include_inactive=True)
        active_rows = _active_master_rows(master_rows)
        active_item_types = _build_item_type_master_rows(unit, include_inactive=False)
        return jsonify(
            {
                "status": "success",
                "mode": result.get("mode") or ("update" if group_id > 0 else "add"),
                "item_group_id": result.get("record_id"),
                "item_group_name": result.get("name") or group_name,
                "item_group_code": result.get("code") or group_code,
                "type_id": type_id,
                "item_groups": _sanitize_json_payload(master_rows),
                "active_item_groups": _sanitize_json_payload(active_rows),
                "item_types": _sanitize_json_payload(active_item_types),
                "permissions": _purchase_unit_master_permissions(role_base),
                "unit": unit,
            }
        )


    @app.route('/api/purchase/item_group_master/action', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_group_master_action():
        unit, error = _get_purchase_unit()
        if error:
            return error
        role_base = _role_base(session.get("role") or "")
        if role_base == "Executive":
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403

        payload = request.get_json(silent=True) or {}
        action = str(payload.get("action") or "").strip().lower()
        group_id = _safe_int(payload.get("item_group_id"))
        if group_id <= 0:
            return jsonify({"status": "error", "message": "Item group id is required."}), 400
        if action not in {"activate", "deactivate"}:
            return jsonify({"status": "error", "message": "Invalid action."}), 400

        result = data_fetch.set_item_group_master_active(unit, group_id, is_active=(action == "activate"))
        if result.get("error"):
            code = str(result.get("code") or "").strip().lower()
            if code == "not_found":
                return jsonify({"status": "error", "message": result.get("error") or "Item group not found."}), 404
            if code == "invalid_id":
                return jsonify({"status": "error", "message": result.get("error") or "Invalid item group id."}), 400
            return jsonify({"status": "error", "message": result.get("error") or "Action failed."}), 500

        master_rows = _build_item_group_master_rows(unit, include_inactive=True)
        active_rows = _active_master_rows(master_rows)
        active_item_types = _build_item_type_master_rows(unit, include_inactive=False)
        return jsonify(
            {
                "status": "success",
                "action": action,
                "item_group_id": group_id,
                "item_groups": _sanitize_json_payload(master_rows),
                "active_item_groups": _sanitize_json_payload(active_rows),
                "item_types": _sanitize_json_payload(active_item_types),
                "permissions": _purchase_unit_master_permissions(role_base),
                "unit": unit,
            }
        )


    @app.route('/api/purchase/item_subgroup_master/list')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_subgroup_master_list():
        unit, error = _get_purchase_unit()
        if error:
            return error
        role_base = _role_base(session.get("role") or "")
        master_rows = _build_item_subgroup_master_rows(unit, include_inactive=True)
        active_rows = _active_master_rows(master_rows)
        active_item_types = _build_item_type_master_rows(unit, include_inactive=False)
        active_item_groups = _build_item_group_master_rows(unit, include_inactive=False)
        return jsonify(
            {
                "status": "success",
                "unit": unit,
                "item_subgroups": _sanitize_json_payload(master_rows),
                "active_item_subgroups": _sanitize_json_payload(active_rows),
                "item_types": _sanitize_json_payload(active_item_types),
                "item_groups": _sanitize_json_payload(active_item_groups),
                "permissions": _purchase_unit_master_permissions(role_base),
            }
        )


    @app.route('/api/purchase/item_subgroup_master', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_subgroup_master_save():
        unit, error = _get_purchase_unit()
        if error:
            return error
        role_base = _role_base(session.get("role") or "")
        if role_base == "Executive":
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403

        payload = request.get_json(silent=True) or {}
        subgroup_payload = payload.get("item_subgroup_master") or {}
        subgroup_id = _safe_int(subgroup_payload.get("id"))
        subgroup_name = str(subgroup_payload.get("name") or "").strip()
        subgroup_code = _normalize_purchase_unit_text(subgroup_payload.get("code") or "").upper()
        type_id = _safe_int(subgroup_payload.get("type_id"))
        group_id = _safe_int(subgroup_payload.get("group_id"))
        if not subgroup_name:
            return jsonify({"status": "error", "message": "Item sub group name is required."}), 400
        if type_id <= 0:
            return jsonify({"status": "error", "message": "Item type selection is required."}), 400
        if group_id <= 0:
            return jsonify({"status": "error", "message": "Item group selection is required."}), 400

        active_item_types = _build_item_type_master_rows(unit, include_inactive=False)
        if not any(_safe_int(row.get("id")) == type_id for row in active_item_types):
            return jsonify({"status": "error", "message": "Selected item type was not found."}), 400

        type_groups = _build_item_group_master_rows(unit, include_inactive=True, type_id=type_id)
        if not any(_safe_int(row.get("id")) == group_id for row in type_groups):
            return jsonify({"status": "error", "message": "Selected item group does not belong to the chosen item type."}), 400

        result = data_fetch.upsert_item_subgroup_master(
            unit,
            subgroup_name=subgroup_name,
            subgroup_code=subgroup_code,
            group_id=group_id,
            subgroup_id=subgroup_id if subgroup_id > 0 else None,
            reactivate=True,
        )
        if result.get("error"):
            code = str(result.get("code") or "").strip().lower()
            if code == "duplicate":
                return jsonify(
                    {
                        "status": "error",
                        "message": result.get("error") or "Item sub group already exists.",
                        "existing_id": result.get("existing_id"),
                    }
                ), 409
            if code == "not_found":
                return jsonify({"status": "error", "message": result.get("error") or "Item sub group not found."}), 404
            if code == "required_parent":
                return jsonify({"status": "error", "message": "Item group selection is required."}), 400
            return jsonify({"status": "error", "message": result.get("error") or "Failed to save item sub group."}), 500

        master_rows = _build_item_subgroup_master_rows(unit, include_inactive=True)
        active_rows = _active_master_rows(master_rows)
        active_item_types = _build_item_type_master_rows(unit, include_inactive=False)
        active_item_groups = _build_item_group_master_rows(unit, include_inactive=False)
        return jsonify(
            {
                "status": "success",
                "mode": result.get("mode") or ("update" if subgroup_id > 0 else "add"),
                "item_subgroup_id": result.get("record_id"),
                "item_subgroup_name": result.get("name") or subgroup_name,
                "item_subgroup_code": result.get("code") or subgroup_code,
                "type_id": type_id,
                "group_id": group_id,
                "item_subgroups": _sanitize_json_payload(master_rows),
                "active_item_subgroups": _sanitize_json_payload(active_rows),
                "item_types": _sanitize_json_payload(active_item_types),
                "item_groups": _sanitize_json_payload(active_item_groups),
                "permissions": _purchase_unit_master_permissions(role_base),
                "unit": unit,
            }
        )


    @app.route('/api/purchase/item_subgroup_master/action', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_subgroup_master_action():
        unit, error = _get_purchase_unit()
        if error:
            return error
        role_base = _role_base(session.get("role") or "")
        if role_base == "Executive":
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403

        payload = request.get_json(silent=True) or {}
        action = str(payload.get("action") or "").strip().lower()
        subgroup_id = _safe_int(payload.get("item_subgroup_id"))
        if subgroup_id <= 0:
            return jsonify({"status": "error", "message": "Item sub group id is required."}), 400
        if action not in {"activate", "deactivate"}:
            return jsonify({"status": "error", "message": "Invalid action."}), 400

        result = data_fetch.set_item_subgroup_master_active(unit, subgroup_id, is_active=(action == "activate"))
        if result.get("error"):
            code = str(result.get("code") or "").strip().lower()
            if code == "not_found":
                return jsonify({"status": "error", "message": result.get("error") or "Item sub group not found."}), 404
            if code == "invalid_id":
                return jsonify({"status": "error", "message": result.get("error") or "Invalid item sub group id."}), 400
            return jsonify({"status": "error", "message": result.get("error") or "Action failed."}), 500

        master_rows = _build_item_subgroup_master_rows(unit, include_inactive=True)
        active_rows = _active_master_rows(master_rows)
        active_item_types = _build_item_type_master_rows(unit, include_inactive=False)
        active_item_groups = _build_item_group_master_rows(unit, include_inactive=False)
        return jsonify(
            {
                "status": "success",
                "action": action,
                "item_subgroup_id": subgroup_id,
                "item_subgroups": _sanitize_json_payload(master_rows),
                "active_item_subgroups": _sanitize_json_payload(active_rows),
                "item_types": _sanitize_json_payload(active_item_types),
                "item_groups": _sanitize_json_payload(active_item_groups),
                "permissions": _purchase_unit_master_permissions(role_base),
                "unit": unit,
            }
        )


    @app.route('/api/purchase/suppliers', methods=['GET', 'POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_suppliers():
        unit, error = _get_purchase_unit()
        if error:
            return error
        if request.method != "GET" and _role_base(session.get("role") or "") == "Executive":
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403

        if request.method == 'GET':
            supplier_df = data_fetch.fetch_iv_suppliers(unit)
            if supplier_df is None:
                return jsonify({"status": "error", "message": "Failed to fetch suppliers"}), 500
            if supplier_df.empty:
                return jsonify({"status": "success", "suppliers": [], "unit": unit})

            suppliers = []
            supplier_df = _clean_df_columns(supplier_df)
            cols_map = {str(c).strip().lower(): c for c in supplier_df.columns}
            id_col = cols_map.get("supplierid") or cols_map.get("id")
            name_col = cols_map.get("suppliername") or cols_map.get("name")
            code_col = cols_map.get("suppliercode") or cols_map.get("code")
            email_col = (
                cols_map.get("email")
                or cols_map.get("emailid")
                or cols_map.get("email_id")
                or cols_map.get("e_mail")
                or cols_map.get("e_mail_id")
            )
            for _, row in supplier_df.iterrows():
                suppliers.append({
                    "id": row.get(id_col),
                    "name": str(row.get(name_col) or "").strip(),
                    "code": str(row.get(code_col) or "").strip(),
                    "email": str(row.get(email_col) or "").strip(),
                })

            return jsonify({"status": "success", "suppliers": suppliers, "unit": unit})

        payload = request.get_json(silent=True) or {}
        supplier = payload.get("supplier") or {}

        name = str(supplier.get("name") or "").strip()
        if not name:
            return jsonify({"status": "error", "message": "Supplier name is required"}), 400

        code = str(supplier.get("code") or "").strip()
        if not code:
            code = data_fetch.fetch_iv_supplier_number(unit, after_add=False)
        if not code:
            return jsonify({"status": "error", "message": "Failed to generate supplier code"}), 500

        def _safe_int(val, default=0):
            try:
                return int(val)
            except Exception:
                return default

        city_id = _safe_int(supplier.get("city_id"))
        state_id = _safe_int(supplier.get("state_id"))
        if not city_id or not state_id:
            default_df = data_fetch.fetch_default_city_state(unit)
            if default_df is not None and not default_df.empty:
                default_df = _clean_df_columns(default_df)
                cols_map = {str(c).strip().lower(): c for c in default_df.columns}
                city_col = cols_map.get("city_id") or cols_map.get("cityid")
                state_col = cols_map.get("state_id") or cols_map.get("stateid")
                row = default_df.iloc[0].to_dict()
                city_id = _safe_int(row.get(city_col), city_id)
                state_id = _safe_int(row.get(state_col), state_id)

        if not city_id or not state_id:
            return jsonify({"status": "error", "message": "Default city/state not available"}), 400

        now = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        updated_by = session.get("user_id") or session.get("user") or session.get("username")
        updated_by = _safe_int(updated_by, 1)

        supplier_params = {
            "pId": 0,
            "pCode": code,
            "pName": name,
            "pAddress": supplier.get("address") or "",
            "pCity": city_id,
            "pState": state_id,
            "pPin": supplier.get("pin") or "",
            "pContactperson": supplier.get("contact_person") or "",
            "pContactdesignation": supplier.get("contact_designation") or "",
            "pGroupid": _safe_int(supplier.get("group_id")),
            "pCreditperiod": _safe_int(supplier.get("credit_period")),
            "pDateofassociation": now,
            "pFax": supplier.get("fax") or "",
            "pPhone1": supplier.get("phone1") or "",
            "pPhone2": supplier.get("phone2") or "",
            "pCellphone": supplier.get("cellphone") or "",
            "pEmail": supplier.get("email") or "",
            "pWeb": supplier.get("web") or "",
            "pCst": supplier.get("cst") or "",
            "pMst": supplier.get("mst") or "",
            "pTds": supplier.get("tds") or "",
            "pExcisecode": supplier.get("excise_code") or "",
            "pExportcode": supplier.get("export_code") or "",
            "pLedgerid": _safe_int(supplier.get("ledger_id")),
            "pEligableforadv": _safe_int(supplier.get("eligible_for_adv")),
            "pBankname": supplier.get("bank_name") or "",
            "pBankbranch": supplier.get("bank_branch") or "",
            "pBankacno": supplier.get("bank_account") or "",
            "pMcirno": supplier.get("mcir_no") or "",
            "pNote": supplier.get("note") or "",
            "pProposed": supplier.get("proposed") or "",
            "pUpdatedby": updated_by,
            "pUpdatedon": now,
            "pSupptype": supplier.get("supplier_type") or "",
            "pSociety": supplier.get("society") or "",
            "pLandmark": supplier.get("landmark") or "",
            "pIncomeTaxNo": supplier.get("income_tax_no") or "0",
            "pVillage": _safe_int(supplier.get("village_id")),
            "pPaytermsid": _safe_int(supplier.get("pay_terms_id")),
        }

        result = data_fetch.add_iv_supplier(unit, supplier_params)
        if result.get("error"):
            return jsonify({"status": "error", "message": result["error"]}), 500

        return jsonify({
            "status": "success",
            "supplier_id": result.get("supplier_id"),
            "supplier_code": code,
            "supplier_name": name,
            "unit": unit,
        })


    @app.route('/api/purchase/supplier_email', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_supplier_email():
        unit, error = _get_purchase_unit()
        if error:
            return error

        payload = request.get_json(silent=True) or {}
        supplier_id = _safe_int(payload.get("supplier_id") or payload.get("id"))
        email = _purchase_normalize_email(payload.get("email"))
        if supplier_id <= 0:
            _audit_log_event(
                "purchase",
                "supplier_email_update",
                status="error",
                entity_type="supplier",
                unit=unit,
                summary="Supplier ID is required",
                details={"supplier_id": supplier_id},
            )
            return jsonify({"status": "error", "message": "Supplier ID is required"}), 400
        if not _purchase_is_valid_email(email):
            _audit_log_event(
                "purchase",
                "supplier_email_update",
                status="error",
                entity_type="supplier",
                entity_id=str(supplier_id),
                unit=unit,
                summary="Invalid supplier email",
                details={"email": email},
            )
            return jsonify({"status": "error", "message": "Enter a valid supplier email address"}), 400

        result = data_fetch.update_iv_supplier_email(unit, supplier_id, email)
        if result.get("error"):
            err_msg = str(result.get("error") or "Failed to update supplier email")
            status_code = 404 if "not found" in err_msg.lower() else 500
            _audit_log_event(
                "purchase",
                "supplier_email_update",
                status="error",
                entity_type="supplier",
                entity_id=str(supplier_id),
                unit=unit,
                summary="Supplier email update failed",
                details={"email": email, "error": err_msg},
            )
            return jsonify({"status": "error", "message": err_msg}), status_code

        _audit_log_event(
            "purchase",
            "supplier_email_update",
            status="success",
            entity_type="supplier",
            entity_id=str(supplier_id),
            unit=unit,
            summary="Supplier email updated",
            details={"email": email},
        )
        return jsonify({
            "status": "success",
            "supplier_id": supplier_id,
            "email": email,
            "unit": unit,
        })


    @app.route('/api/purchase/suppliers/new_code')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_supplier_code():
        unit, error = _get_purchase_unit()
        if error:
            return error
        code = data_fetch.fetch_iv_supplier_number(unit, after_add=False)
        if not code:
            return jsonify({"status": "error", "message": "Failed to fetch supplier code"}), 500
        return jsonify({"status": "success", "code": code, "unit": unit})


    @app.route('/api/purchase/suppliers/default_location')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_supplier_defaults():
        unit, error = _get_purchase_unit()
        if error:
            return error
        df = data_fetch.fetch_default_city_state(unit)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch default location"}), 500
        if df.empty:
            return jsonify({"status": "success", "location": None, "unit": unit})

        df = _clean_df_columns(df)
        cols_map = {str(c).strip().lower(): c for c in df.columns}
        city_id_col = cols_map.get("city_id") or cols_map.get("cityid")
        city_name_col = cols_map.get("city_name") or cols_map.get("cityname")
        state_id_col = cols_map.get("state_id") or cols_map.get("stateid")
        state_name_col = cols_map.get("state_name") or cols_map.get("statename")
        country_id_col = cols_map.get("country_id") or cols_map.get("countryid")
        country_name_col = cols_map.get("country_name") or cols_map.get("countryname")
        row = df.iloc[0]

        location = {
            "city_id": row.get(city_id_col),
            "city_name": str(row.get(city_name_col) or "").strip(),
            "state_id": row.get(state_id_col),
            "state_name": str(row.get(state_name_col) or "").strip(),
            "country_id": row.get(country_id_col),
            "country_name": str(row.get(country_name_col) or "").strip(),
        }

        location = _sanitize_json_payload(location)
        return jsonify({"status": "success", "location": location, "unit": unit})


    @app.route('/api/purchase/suppliers/states')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_supplier_states():
        unit, error = _get_purchase_unit()
        if error:
            return error
        df = data_fetch.fetch_state_list(unit)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch states"}), 500
        if df.empty:
            return jsonify({"status": "success", "states": [], "unit": unit})
        states = _build_master_list(
            df,
            ["state_id", "stateid", "id"],
            ["state_name", "statename", "name"],
            ["state_code", "statecode", "code"],
        )
        return jsonify({"status": "success", "states": _sanitize_json_payload(states), "unit": unit})


    @app.route('/api/purchase/suppliers/cities')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_supplier_cities():
        unit, error = _get_purchase_unit()
        if error:
            return error
        state_id_raw = request.args.get("state_id")
        if state_id_raw and not str(state_id_raw).isdigit():
            return jsonify({"status": "error", "message": "Invalid state id"}), 400
        state_id = int(state_id_raw) if state_id_raw else 0
        df = data_fetch.fetch_city_list(unit, state_id)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch cities"}), 500
        if df.empty:
            return jsonify({"status": "success", "cities": [], "unit": unit})
        cities = _build_master_list(
            df,
            ["city_id", "cityid", "id"],
            ["city_name", "cityname", "name"],
            ["city_code", "citycode", "code"],
        )
        return jsonify({"status": "success", "cities": _sanitize_json_payload(cities), "unit": unit})


    @app.route('/api/purchase/manufacturers/new_code')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_manufacturer_code():
        unit, error = _get_purchase_unit()
        if error:
            return error
        code = data_fetch.fetch_iv_manufacturer_number(unit)
        if not code:
            return jsonify({"status": "error", "message": "Failed to fetch manufacturer code"}), 500
        return jsonify({"status": "success", "code": code, "unit": unit})


    @app.route('/api/purchase/manufacturers/default_location')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_manufacturer_defaults():
        unit, error = _get_purchase_unit()
        if error:
            return error
        df = data_fetch.fetch_default_city_state(unit)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch default location"}), 500
        if df.empty:
            return jsonify({"status": "success", "location": None, "unit": unit})

        df = _clean_df_columns(df)
        cols_map = {str(c).strip().lower(): c for c in df.columns}
        city_id_col = cols_map.get("city_id") or cols_map.get("cityid")
        city_name_col = cols_map.get("city_name") or cols_map.get("cityname")
        state_id_col = cols_map.get("state_id") or cols_map.get("stateid")
        state_name_col = cols_map.get("state_name") or cols_map.get("statename")
        country_id_col = cols_map.get("country_id") or cols_map.get("countryid")
        country_name_col = cols_map.get("country_name") or cols_map.get("countryname")
        row = df.iloc[0]

        location = {
            "city_id": row.get(city_id_col),
            "city_name": str(row.get(city_name_col) or "").strip(),
            "state_id": row.get(state_id_col),
            "state_name": str(row.get(state_name_col) or "").strip(),
            "country_id": row.get(country_id_col),
            "country_name": str(row.get(country_name_col) or "").strip(),
        }

        location = _sanitize_json_payload(location)
        return jsonify({"status": "success", "location": location, "unit": unit})


    @app.route('/api/purchase/manufacturers/states')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_manufacturer_states():
        unit, error = _get_purchase_unit()
        if error:
            return error
        df = data_fetch.fetch_state_list(unit)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch states"}), 500
        if df.empty:
            return jsonify({"status": "success", "states": [], "unit": unit})
        states = _build_master_list(df, ["state_id", "stateid", "id"], ["state_name", "statename", "name"], ["state_code", "statecode", "code"])
        return jsonify({"status": "success", "states": _sanitize_json_payload(states), "unit": unit})


    @app.route('/api/purchase/manufacturers/cities')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_manufacturer_cities():
        unit, error = _get_purchase_unit()
        if error:
            return error
        state_id_raw = request.args.get("state_id")
        if state_id_raw and not str(state_id_raw).isdigit():
            return jsonify({"status": "error", "message": "Invalid state id"}), 400
        state_id = int(state_id_raw) if state_id_raw else 0
        df = data_fetch.fetch_city_list(unit, state_id)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch cities"}), 500
        if df.empty:
            return jsonify({"status": "success", "cities": [], "unit": unit})
        cities = _build_master_list(df, ["city_id", "cityid", "id"], ["city_name", "cityname", "name"], ["city_code", "citycode", "code"])
        return jsonify({"status": "success", "cities": _sanitize_json_payload(cities), "unit": unit})


    @app.route('/api/purchase/supplier_master/list')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_supplier_master_list():
        unit, error = _get_purchase_unit()
        if error:
            return error
        df = data_fetch.fetch_iv_supplier_master_list(unit)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch supplier list"}), 500
        if df.empty:
            return jsonify({"status": "success", "suppliers": [], "unit": unit})
        suppliers = _build_master_list(df, ["id"], ["name"], ["code"])
        return jsonify({"status": "success", "suppliers": _sanitize_json_payload(suppliers), "unit": unit})


    @app.route('/api/purchase/supplier_master/details')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_supplier_master_details():
        unit, error = _get_purchase_unit()
        if error:
            return error
        supplier_id_raw = request.args.get("supplier_id")
        if not supplier_id_raw or not str(supplier_id_raw).isdigit():
            return jsonify({"status": "error", "message": "Supplier ID is required"}), 400
        df = data_fetch.fetch_iv_supplier_master_detail(unit, int(supplier_id_raw))
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch supplier details"}), 500
        if df.empty:
            return jsonify({"status": "error", "message": "Supplier not found"}), 404
        df = _clean_df_columns(df)
        row = df.iloc[0].to_dict()
        supplier = {
            "id": _row_value(row, ["ID", "Id"]),
            "code": _row_value(row, ["Code"]),
            "name": _row_value(row, ["Name"]),
            "address": _row_value(row, ["Address"]),
            "city_id": _row_value(row, ["City", "CityID", "City_Id", "City_ID"]),
            "city_name": _row_value(row, ["City_Name", "CityName"]),
            "state_id": _row_value(row, ["State", "StateID", "State_Id", "State_ID"]),
            "state_name": _row_value(row, ["State_Name", "StateName"]),
            "pin": _row_value(row, ["Pin"]),
            "credit_period": _row_value(row, ["CreditPeriod"]),
            "contact_person": _row_value(row, ["ContactPerson"]),
            "contact_designation": _row_value(row, ["ContactDesignation"]),
            "phone1": _row_value(row, ["Phone1"]),
            "phone2": _row_value(row, ["Phone2"]),
            "cellphone": _row_value(row, ["CellPhone", "Cellphone"]),
            "email": _row_value(row, ["Email"]),
            "excise_code": _row_value(row, ["ExciseCode", "Excise_Code"]),
            "note": _row_value(row, ["Note"]),
        }
        return jsonify({"status": "success", "supplier": _sanitize_json_payload(supplier), "unit": unit})


    @app.route('/api/purchase/supplier_master', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_supplier_master():
        unit, error = _get_purchase_unit()
        if error:
            return error
        if _role_base(session.get("role") or "") == "Executive":
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403
        payload = request.get_json(silent=True) or {}
        supplier = payload.get("supplier") or {}

        supplier_id = _safe_int(supplier.get("supplier_id") or supplier.get("id"))
        existing = {}
        if supplier_id:
            existing_df = data_fetch.fetch_iv_supplier_master_detail(unit, supplier_id)
            if existing_df is None:
                return jsonify({"status": "error", "message": "Failed to load existing supplier"}), 500
            if existing_df.empty:
                return jsonify({"status": "error", "message": "Supplier not found"}), 404
            existing_df = _clean_df_columns(existing_df)
            existing = existing_df.iloc[0].to_dict()

        name = str(supplier.get("name") or "").strip()
        if not name and existing:
            name = str(_row_value(existing, ["Name"]) or "").strip()
        if not name:
            return jsonify({"status": "error", "message": "Supplier name is required"}), 400

        code = str(supplier.get("code") or "").strip()
        if not code and existing:
            code = str(_row_value(existing, ["Code"]) or "").strip()
        if not code:
            code = data_fetch.fetch_iv_supplier_number(unit, after_add=False)
        if not code:
            return jsonify({"status": "error", "message": "Failed to generate supplier code"}), 500

        city_id = _safe_int(supplier.get("city_id")) if "city_id" in supplier else 0
        state_id = _safe_int(supplier.get("state_id")) if "state_id" in supplier else 0
        if not city_id and existing:
            city_id = _safe_int(_row_value(existing, ["City", "CityID", "City_Id", "City_ID"]))
        if not state_id and existing:
            state_id = _safe_int(_row_value(existing, ["State", "StateID", "State_Id", "State_ID"]))
        if not city_id or not state_id:
            default_df = data_fetch.fetch_default_city_state(unit)
            if default_df is not None and not default_df.empty:
                default_df = _clean_df_columns(default_df)
                cols_map = {str(c).strip().lower(): c for c in default_df.columns}
                city_col = cols_map.get("city_id") or cols_map.get("cityid")
                state_col = cols_map.get("state_id") or cols_map.get("stateid")
                row = default_df.iloc[0].to_dict()
                if not city_id:
                    city_id = _safe_int(row.get(city_col))
                if not state_id:
                    state_id = _safe_int(row.get(state_col))
        if not city_id or not state_id:
            return jsonify({"status": "error", "message": "Default city/state not available"}), 400

        now = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        updated_by = session.get("user_id") or session.get("user") or session.get("username")
        updated_by = _safe_int(updated_by, 1)

        params = _build_supplier_params(
            supplier_id=supplier_id,
            supplier=supplier,
            code=code,
            name=name,
            city_id=city_id,
            state_id=state_id,
            updated_by=updated_by,
            now_str=now,
            existing=existing,
        )

        if supplier_id:
            result = data_fetch.update_iv_supplier(unit, params)
        else:
            result = data_fetch.add_iv_supplier(unit, params)

        if result.get("error"):
            return jsonify({"status": "error", "message": result["error"]}), 500

        mirror_outcomes = []
        if not supplier_id:
            mirror_supplier = dict(supplier or {})
            mirror_supplier["name"] = name
            mirror_supplier["code"] = code
            mirror_outcomes = _mirror_new_supplier_master_to_family(unit, mirror_supplier, updated_by, now)
        message, mirror_status = _build_replication_message(
            "Supplier",
            "updated" if supplier_id else "saved",
            result.get("supplier_id") or supplier_id,
            mirror_outcomes,
        )

        return jsonify({
            "status": "success",
            "supplier_id": result.get("supplier_id") or supplier_id,
            "supplier_code": code,
            "supplier_name": name,
            "unit": unit,
            "mode": "update" if supplier_id else "add",
            "mirrored_units": [row.get("unit") for row in mirror_outcomes if row.get("status") == "mirrored"],
            "mirror_failures": [row for row in mirror_outcomes if row.get("status") == "failed"],
            "mirror_status": mirror_status,
            "message": message,
        })


    @app.route('/api/purchase/manufacturer_master/list')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_manufacturer_master_list():
        unit, error = _get_purchase_unit()
        if error:
            return error
        df = data_fetch.fetch_iv_manufacturer_master_list(unit)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch manufacturer list"}), 500
        if df.empty:
            return jsonify({"status": "success", "manufacturers": [], "unit": unit})
        manufacturers = _build_master_list(df, ["id"], ["name"], ["code"])
        return jsonify({"status": "success", "manufacturers": _sanitize_json_payload(manufacturers), "unit": unit})


    @app.route('/api/purchase/manufacturer_master/details')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_manufacturer_master_details():
        unit, error = _get_purchase_unit()
        if error:
            return error
        manufacturer_id_raw = request.args.get("manufacturer_id")
        if not manufacturer_id_raw or not str(manufacturer_id_raw).isdigit():
            return jsonify({"status": "error", "message": "Manufacturer ID is required"}), 400
        df = data_fetch.fetch_iv_manufacturer_master_detail(unit, int(manufacturer_id_raw))
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch manufacturer details"}), 500
        if df.empty:
            return jsonify({"status": "error", "message": "Manufacturer not found"}), 404
        df = _clean_df_columns(df)
        row = df.iloc[0].to_dict()
        manufacturer = {
            "id": _row_value(row, ["ID", "Id"]),
            "code": _row_value(row, ["Code"]),
            "name": _row_value(row, ["Name"]),
            "address": _row_value(row, ["Address"]),
            "city_id": _row_value(row, ["City", "CityID", "City_Id", "City_ID"]),
            "city_name": _row_value(row, ["City_Name", "CityName"]),
            "city_code": _row_value(row, ["City_Code", "CityCode"]),
            "state_id": _row_value(row, ["State", "StateID", "State_Id", "State_ID"]),
            "state_name": _row_value(row, ["State_Name", "StateName"]),
            "state_code": _row_value(row, ["State_Code", "StateCode"]),
            "pin": _row_value(row, ["Pin"]),
            "contact_person": _row_value(row, ["ContactPerson"]),
            "contact_designation": _row_value(row, ["ContactDesignation"]),
            "phone1": _row_value(row, ["Phone1"]),
            "phone2": _row_value(row, ["Phone2"]),
            "cellphone": _row_value(row, ["CellPhone", "Cellphone"]),
            "web": _row_value(row, ["Web"]),
            "email": _row_value(row, ["Email"]),
            "bank_name": _row_value(row, ["BankName"]),
            "bank_account": _row_value(row, ["BankAcNo", "BankAcNO"]),
            "bank_branch": _row_value(row, ["BankBranch"]),
            "note": _row_value(row, ["Note"]),
            "society": _row_value(row, ["Society"]),
            "landmark": _row_value(row, ["Landmark", "landmark"]),
            "village_id": _row_value(row, ["VillageID", "VillageId"]),
            "village": _row_value(row, ["Village"]),
        }
        return jsonify({"status": "success", "manufacturer": _sanitize_json_payload(manufacturer), "unit": unit})


    @app.route('/api/purchase/manufacturer_master', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_manufacturer_master():
        unit, error = _get_purchase_unit()
        if error:
            return error
        if _role_base(session.get("role") or "") == "Executive":
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403
        payload = request.get_json(silent=True) or {}
        manufacturer = payload.get("manufacturer") or {}

        manufacturer_id = _safe_int(manufacturer.get("manufacturer_id") or manufacturer.get("id"))
        existing = {}
        if manufacturer_id:
            existing_df = data_fetch.fetch_iv_manufacturer_master_detail(unit, manufacturer_id)
            if existing_df is None:
                return jsonify({"status": "error", "message": "Failed to load existing manufacturer"}), 500
            if existing_df.empty:
                return jsonify({"status": "error", "message": "Manufacturer not found"}), 404
            existing_df = _clean_df_columns(existing_df)
            existing = existing_df.iloc[0].to_dict()

        name = str(manufacturer.get("name") or "").strip()
        if not name and existing:
            name = str(_row_value(existing, ["Name"]) or "").strip()
        if not name:
            return jsonify({"status": "error", "message": "Manufacturer name is required"}), 400

        code = str(manufacturer.get("code") or "").strip()
        if not code and existing:
            code = str(_row_value(existing, ["Code"]) or "").strip()
        if not code:
            code = data_fetch.fetch_iv_manufacturer_number(unit)
        if not code:
            return jsonify({"status": "error", "message": "Failed to generate manufacturer code"}), 500

        manufacturer_list_df = data_fetch.fetch_iv_manufacturer_master_list(unit)
        if manufacturer_list_df is None:
            return jsonify({"status": "error", "message": "Failed to validate manufacturer name"}), 500
        duplicate_name_key = _normalize_master_name(name)
        if duplicate_name_key:
            duplicate_rows = _build_master_list(manufacturer_list_df, ["id"], ["name"], ["code"])
            duplicate_match = next(
                (
                    row for row in (duplicate_rows or [])
                    if _safe_int(row.get("id")) != manufacturer_id
                    and _normalize_master_name(row.get("name")) == duplicate_name_key
                ),
                None,
            )
            if duplicate_match:
                return jsonify({
                    "status": "error",
                    "message": f'Manufacturer "{duplicate_match.get("name")}" already exists.',
                    "code": "duplicate",
                    "manufacturer_id": _safe_int(duplicate_match.get("id")),
                }), 409

        city_id = _safe_int(manufacturer.get("city_id")) if "city_id" in manufacturer else 0
        state_id = _safe_int(manufacturer.get("state_id")) if "state_id" in manufacturer else 0
        if not city_id and existing:
            city_id = _safe_int(_row_value(existing, ["City", "CityID", "City_Id", "City_ID"]))
        if not state_id and existing:
            state_id = _safe_int(_row_value(existing, ["State", "StateID", "State_Id", "State_ID"]))
        if not city_id or not state_id:
            default_df = data_fetch.fetch_default_city_state(unit)
            if default_df is not None and not default_df.empty:
                default_df = _clean_df_columns(default_df)
                cols_map = {str(c).strip().lower(): c for c in default_df.columns}
                city_col = cols_map.get("city_id") or cols_map.get("cityid")
                state_col = cols_map.get("state_id") or cols_map.get("stateid")
                row = default_df.iloc[0].to_dict()
                if not city_id:
                    city_id = _safe_int(row.get(city_col))
                if not state_id:
                    state_id = _safe_int(row.get(state_col))
        if not city_id or not state_id:
            return jsonify({"status": "error", "message": "Default city/state not available"}), 400

        now = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        updated_by = session.get("user_id") or session.get("user") or session.get("username")
        updated_by = _safe_int(updated_by, 1)

        params = _build_manufacturer_params(
            manufacturer_id=manufacturer_id,
            manufacturer=manufacturer,
            code=code,
            name=name,
            city_id=city_id,
            state_id=state_id,
            updated_by=updated_by,
            now_str=now,
            existing=existing,
        )

        if manufacturer_id:
            result = data_fetch.update_iv_manufacturer(unit, params)
        else:
            result = data_fetch.add_iv_manufacturer(unit, params)

        if result.get("error"):
            return jsonify({"status": "error", "message": result["error"]}), 500

        return jsonify({
            "status": "success",
            "manufacturer_id": result.get("manufacturer_id") or manufacturer_id,
            "manufacturer_code": code,
            "manufacturer_name": name,
            "unit": unit,
            "mode": "update" if manufacturer_id else "add",
        })


    @app.route('/api/purchase/item_master/init')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_master_init():
        unit, error = _get_purchase_unit()
        if error:
            return error
        if _supports_contracted_rate(unit):
            try:
                data_fetch.ensure_iv_item_contracted_rate_column(unit)
            except Exception:
                pass
        meta = _load_item_master_lists(unit)
        return jsonify({
            "status": "success",
            "unit": unit,
            "pack_sizes": meta.get("pack_sizes") or [],
            "categories": meta.get("categories") or [],
            "groups": meta.get("groups") or [],
            "subgroups": meta.get("subgroups") or [],
            "units": meta.get("units") or [],
            "locations": meta.get("locations") or [],
            "defaults": meta.get("defaults") or {},
        })


    @app.route('/api/purchase/item_master/groups')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_master_groups():
        unit, error = _get_purchase_unit()
        if error:
            return error
        type_id_raw = request.args.get("type_id")
        if not type_id_raw or not str(type_id_raw).isdigit():
            return jsonify({"status": "error", "message": "Type ID is required"}), 400
        df = data_fetch.fetch_item_groups_by_type(unit, int(type_id_raw), include_inactive=False)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch groups"}), 500
        if df.empty:
            return jsonify({"status": "success", "groups": [], "unit": unit})
        df = _clean_df_columns(df)
        cols = {str(c).strip().lower(): c for c in df.columns}
        id_col = cols.get("id")
        name_col = cols.get("name")
        code_col = cols.get("code")
        type_col = cols.get("typeid")
        type_name_col = cols.get("typename")
        rows = []
        if id_col and name_col:
            for _, row in df.iterrows():
                rows.append({
                    "id": row.get(id_col),
                    "name": str(row.get(name_col) or "").strip(),
                    "code": str(row.get(code_col) or "").strip() if code_col else "",
                    "type_id": row.get(type_col) if type_col else "",
                    "type_name": str(row.get(type_name_col) or "").strip() if type_name_col else "",
                })
        return jsonify({"status": "success", "groups": _sanitize_json_payload(rows), "unit": unit})


    @app.route('/api/purchase/item_master/subgroups')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_master_subgroups():
        unit, error = _get_purchase_unit()
        if error:
            return error
        group_id_raw = request.args.get("group_id")
        if not group_id_raw or not str(group_id_raw).isdigit():
            return jsonify({"status": "error", "message": "Group ID is required"}), 400
        df = data_fetch.fetch_item_subgroups_by_group(unit, int(group_id_raw), include_inactive=False)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch subgroups"}), 500
        if df.empty:
            return jsonify({"status": "success", "subgroups": [], "unit": unit})
        df = _clean_df_columns(df)
        cols = {str(c).strip().lower(): c for c in df.columns}
        id_col = cols.get("id")
        name_col = cols.get("name")
        code_col = cols.get("code")
        group_col = cols.get("groupid")
        group_name_col = cols.get("groupname")
        type_col = cols.get("typeid")
        type_name_col = cols.get("typename")
        rows = []
        if id_col and name_col:
            for _, row in df.iterrows():
                rows.append({
                    "id": row.get(id_col),
                    "name": str(row.get(name_col) or "").strip(),
                    "code": str(row.get(code_col) or "").strip() if code_col else "",
                    "group_id": row.get(group_col) if group_col else "",
                    "group_name": str(row.get(group_name_col) or "").strip() if group_name_col else "",
                    "type_id": row.get(type_col) if type_col else "",
                    "type_name": str(row.get(type_name_col) or "").strip() if type_name_col else "",
                })
        return jsonify({"status": "success", "subgroups": _sanitize_json_payload(rows), "unit": unit})


    @app.route('/api/purchase/item_master/list')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_master_list():
        unit, error = _get_purchase_unit()
        if error:
            return error
        df = data_fetch.fetch_item_master_list(unit)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch items"}), 500
        if df.empty:
            return jsonify({"status": "success", "items": [], "unit": unit})
        df = _clean_df_columns(df)
        cols = {str(c).strip().lower(): c for c in df.columns}
        id_col = cols.get("id")
        code_col = cols.get("code")
        name_col = cols.get("name")
        product_col = cols.get("productcode") or cols.get("product_code")
        rows = []
        for _, row in df.iterrows():
            rows.append({
                "id": row.get(id_col),
                "code": str(row.get(code_col) or "").strip(),
                "name": str(row.get(name_col) or "").strip(),
                "product_code": str(row.get(product_col) or "").strip() if product_col else "",
            })
        return jsonify({"status": "success", "items": _sanitize_json_payload(rows), "unit": unit})


    @app.route('/api/purchase/item_master/details')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_master_details():
        unit, error = _get_purchase_unit()
        if error:
            return error
        item_id_raw = request.args.get("item_id")
        if not item_id_raw or not str(item_id_raw).isdigit():
            return jsonify({"status": "error", "message": "Item ID is required"}), 400
        df = data_fetch.fetch_item_master_detail(unit, int(item_id_raw))
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch item details"}), 500
        if df.empty:
            return jsonify({"status": "error", "message": "Item not found"}), 404
        df = _clean_df_columns(df)
        row = df.iloc[0].to_dict()
        payload = _sanitize_json_payload({
            "id": _row_value(row, ["ID", "Id"]),
            "code": _row_value(row, ["Code"]),
            "name": _row_value(row, ["Name"]),
            "descriptive_name": _row_value(row, ["DescriptiveName"]),
            "technical_specs": _row_value(row, ["TechnicalSpecs", "TechnicalSpec", "TechSpecs", "TechSpec"]),
            "product_code": _row_value(row, ["ProductCode"]),
            "item_type_id": _row_value(row, ["ItemTypeID", "ItemTypeId"]),
            "item_type_name": _row_value(row, ["ItemTypeName", "TypeName"]),
            "item_group_id": _row_value(row, ["ItemGroupID", "ItemGroupId"]),
            "item_group_name": _row_value(row, ["ItemGroupName", "GroupName"]),
            "sub_group_id": _row_value(row, ["SubGroupID", "SubGroupId"]),
            "sub_group_name": _row_value(row, ["SubGroupName", "SubGroup"]),
            "unit_id": _row_value(row, ["UnitID", "UnitId"]),
            "location_id": _row_value(row, ["LocationID", "LocationId"]),
            "pack_size_id": _row_value(row, ["PackSizeID", "PackSizeId"]),
            "standard_rate": _row_value(row, ["StandardRate"]),
            "contracted_rate": _row_value(row, ["Contracted_Rate", "ContractedRate"]),
            "sales_price": _row_value(row, ["SalesPrice", "Salesprice"]),
            "sales_tax": _row_value(row, ["SalesTax", "Salestax"]),
            "current_qty": _row_value(row, ["CurrentQty"]),
            "max_level": _row_value(row, ["MaxLevel"]),
            "min_level": _row_value(row, ["MinLevel"]),
            "reorder_level": _row_value(row, ["ReOrderLevel", "ReorderLevel"]),
            "batch_required": _row_value(row, ["BatchRequired"]),
            "expiry_required": _row_value(row, ["ExpiryDtRequired"]),
            "active": _row_value(row, ["Active"]),
            "loose_selling": _row_value(row, ["ChkLooseSelling"]),
            "quality_control": _row_value(row, ["chkQualityCtrl", "ChkQualityCtrl"]),
            "vat_on": _row_value(row, ["VatOn"]),
            "medicine_type_id": _row_value(row, ["MedicineTypeId", "MedicineTypeID"]),
        })
        return jsonify({"status": "success", "item": payload, "unit": unit})


    @app.route('/api/purchase/item_master/manufacturers', methods=['GET', 'POST', 'DELETE'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_master_manufacturers():
        unit, error = _get_purchase_unit()
        if error:
            return error

        if request.method == 'GET':
            item_id_raw = request.args.get("item_id")
            if not item_id_raw or not str(item_id_raw).isdigit():
                return jsonify({"status": "error", "message": "Item ID is required"}), 400
            df = data_fetch.fetch_item_manufacturer_links(unit, int(item_id_raw))
            if df is None:
                return jsonify({"status": "error", "message": "Failed to fetch manufacturer links"}), 500
            if df.empty:
                return jsonify({
                    "status": "success",
                    "links": [],
                    "unit": unit,
                    "can_delink": _role_base(session.get("role") or "") in {"IT", "Management", "Departmental Head"},
                })
            df = _clean_df_columns(df)
            cols = {str(c).strip().lower(): c for c in df.columns}
            link_col = cols.get("linkid") or cols.get("link_id")
            item_col = cols.get("itemid") or cols.get("item_id")
            manu_col = cols.get("manufacturerid") or cols.get("id")
            name_col = cols.get("manufacturername") or cols.get("name")
            code_col = cols.get("manufacturercode") or cols.get("code")
            rows = []
            for _, row in df.iterrows():
                rows.append({
                    "link_id": row.get(link_col),
                    "item_id": row.get(item_col),
                    "manufacturer_id": row.get(manu_col),
                    "name": str(row.get(name_col) or "").strip() if name_col else "",
                    "code": str(row.get(code_col) or "").strip() if code_col else "",
                    "mapping_count": _safe_int(row.get(cols.get("mappingcount") or cols.get("mapping_count")), 1),
                })
            return jsonify({
                "status": "success",
                "links": _sanitize_json_payload(rows),
                "unit": unit,
                "can_delink": _role_base(session.get("role") or "") in {"IT", "Management", "Departmental Head"},
            })

        if _role_base(session.get("role") or "") == "Executive":
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403
        payload = request.get_json(silent=True) or {}
        item_id = _safe_int(payload.get("item_id") or payload.get("itemId"))
        manufacturer_id = _safe_int(payload.get("manufacturer_id") or payload.get("manufacturerId") or payload.get("id"))
        link_id = _safe_int(payload.get("link_id") or payload.get("linkId"))
        if request.method == 'DELETE':
            if not item_id or (not manufacturer_id and not link_id):
                return jsonify({"status": "error", "message": "Item ID and manufacturer selection are required"}), 400
            now_str = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
            user_token = str(session.get("user_id") or session.get("user") or session.get("username") or "1")
            result = data_fetch.delete_item_manufacturer_link(
                unit,
                int(item_id),
                int(manufacturer_id) if manufacturer_id else None,
                user_token,
                now_str,
                request.remote_addr,
                link_id=int(link_id) if link_id else None,
            )
            if result.get("error"):
                return jsonify({"status": "error", "message": result["error"]}), 500
            deleted_count = _safe_int(result.get("deleted_count"))
            removed = deleted_count > 0
            message = "Manufacturer link was already removed." if not removed else "Manufacturer delinked successfully."
            if deleted_count > 1:
                message = "Manufacturer delinked successfully. Duplicate mappings were cleared too."
            return jsonify({
                "status": "success",
                "deleted_count": deleted_count,
                "removed": removed,
                "manufacturer_id": result.get("manufacturer_id"),
                "message": message,
                "unit": unit,
            })
        if not item_id or not manufacturer_id:
            return jsonify({"status": "error", "message": "Item ID and manufacturer ID are required"}), 400
        now_str = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        user_token = str(session.get("user_id") or session.get("user") or session.get("username") or "1")
        result = data_fetch.add_item_manufacturer_link(
            unit,
            int(item_id),
            int(manufacturer_id),
            user_token,
            now_str,
            request.remote_addr,
        )
        if result.get("error"):
            return jsonify({"status": "error", "message": result["error"]}), 500
        return jsonify({
            "status": "success",
            "link_id": result.get("link_id"),
            "created": result.get("created", True),
            "unit": unit,
        })


    @app.route('/api/purchase/item_master', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_item_master_create():
        unit, error = _get_purchase_unit()
        if error:
            return error
        if _role_base(session.get("role") or "") == "Executive":
            return jsonify({"status": "error", "message": "Access denied for your role."}), 403
        try:
            data_fetch.ensure_iv_item_technical_specs_column(unit)
        except Exception:
            pass
        payload = request.get_json(silent=True) or {}
        item = payload.get("item") or {}
        manufacturer_id = _safe_int(
            payload.get("manufacturer_id")
            or payload.get("manufacturerId")
            or item.get("manufacturer_id")
            or item.get("manufacturerId")
        )

        item_id = _safe_int(item.get("item_id"))
        name = str(item.get("name") or "").strip()
        if not name:
            return jsonify({"status": "error", "message": "Item name is required"}), 400
        descriptive_name_raw = str(item.get("descriptive_name") or "").strip()
        descriptive_name = descriptive_name_raw or name
        if len(descriptive_name) > ITEM_MASTER_DESCRIPTIVE_NAME_MAX:
            return jsonify({
                "status": "error",
                "message": (
                    f"Descriptive Name supports maximum {ITEM_MASTER_DESCRIPTIVE_NAME_MAX} characters. "
                    "Please use Technical Specs / Description for long narration."
                ),
            }), 400

        store_name = str(item.get("store_name") or item.get("location_name") or "").strip()
        location_id = _safe_int(item.get("location_id"))
        pack_size_id = _safe_int(item.get("pack_size_id"))
        unit_id = _safe_int(item.get("unit_id"))
        unit_name = str(item.get("unit_name") or item.get("unit") or "").strip()
        rate = _safe_float(item.get("rate"))
        mrp = _safe_float(item.get("mrp"))
        contracted_rate_raw = item.get("contracted_rate")
        contracted_rate = _safe_float(contracted_rate_raw, None) if _supports_contracted_rate(unit) else None
        gst_raw = item.get("gst_pct")
        gst = _safe_float(gst_raw)
        gst_provided = gst_raw is not None and str(gst_raw).strip() != ""
        allow_zero_rate_mrp = _unit_allows_zero_rate_mrp(unit)

        missing = []
        if not location_id and not store_name:
            missing.append("store")
        if not pack_size_id:
            missing.append("pack size")
        if not unit_id and not unit_name:
            missing.append("unit")
        if rate < 0 or (rate <= 0 and not allow_zero_rate_mrp):
            missing.append("rate")
        if mrp < 0 or (mrp <= 0 and not allow_zero_rate_mrp):
            missing.append("mrp")
        if not gst_provided or gst < 0:
            missing.append("gst")
        if missing:
            return jsonify({"status": "error", "message": f"Missing required fields: {', '.join(missing)}"}), 400
        if _supports_contracted_rate(unit):
            if contracted_rate is not None and contracted_rate < 0:
                return jsonify({"status": "error", "message": "Contracted Rate cannot be negative"}), 400
            if contracted_rate is not None and contracted_rate <= 0:
                contracted_rate = None

        if not unit_id and unit_name:
            unit_id_resolved, unit_err = _ensure_purchase_unit_master_entry(unit, unit_name)
            if unit_err:
                return jsonify({"status": "error", "message": f"Failed to save unit '{unit_name}': {unit_err}"}), 500
            unit_id = unit_id_resolved

        if manufacturer_id:
            manufacturer_df = data_fetch.fetch_iv_manufacturer_master_detail(unit, manufacturer_id)
            if manufacturer_df is None:
                return jsonify({"status": "error", "message": "Failed to validate manufacturer selection"}), 500
            if manufacturer_df.empty:
                return jsonify({"status": "error", "message": "Selected manufacturer was not found. Please refresh and try again."}), 400

        defaults = {
            "item_type_id": _safe_int(item.get("item_type_id")),
            "item_group_id": _safe_int(item.get("item_group_id")),
            "sub_group_id": _safe_int(item.get("sub_group_id")),
            "unit_id": _safe_int(unit_id),
            "location_id": _safe_int(location_id),
            "pack_size_id": _safe_int(pack_size_id),
            "medicine_type_id": _safe_int(item.get("medicine_type_id"), ITEM_MASTER_DEFAULT_MEDICINE_TYPE_ID),
        }
        lookups = {}
        needs_master_meta = (
            defaults["item_type_id"] <= 0
            or defaults["item_group_id"] <= 0
            or defaults["sub_group_id"] <= 0
            or defaults["unit_id"] <= 0
            or defaults["location_id"] <= 0
            or defaults["pack_size_id"] <= 0
        )
        if needs_master_meta:
            meta = _load_item_master_lists(
                unit,
                selected_type_id=defaults["item_type_id"] or None,
                selected_group_id=defaults["item_group_id"] or None,
            )
            defaults.update(meta.get("defaults", {}))
            lookups = meta.get("lookups", {})
        now_str = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        user_token = str(session.get("user_id") or session.get("user") or session.get("username") or "1")

        item_payload = {
            "name": name,
            "descriptive_name": descriptive_name,
            "technical_specs": item.get("technical_specs") or "",
            "product_code": item.get("product_code") or "",
            "item_type_id": _safe_int(item.get("item_type_id"), defaults.get("item_type_id")),
            "item_group_id": _safe_int(item.get("item_group_id"), defaults.get("item_group_id")),
            "sub_group_id": _safe_int(item.get("sub_group_id"), defaults.get("sub_group_id")),
            "unit_id": unit_id,
            "unit_name": unit_name,
            "location_id": location_id,
            "location_name": store_name,
            "pack_size_id": pack_size_id or defaults.get("pack_size_id"),
            "standard_rate": rate,
            "sales_price": mrp,
            "gst_pct": gst,
            "batch_required": bool(item.get("batch_required")),
            "expiry_required": bool(item.get("expiry_required", True)),
            "loose_selling": bool(item.get("loose_selling")),
            "quality_control": bool(item.get("quality_control")),
            "active": bool(item.get("active", True)),
            "vat_on": item.get("vat_on") or "P",
            "medicine_type_id": _safe_int(item.get("medicine_type_id"), defaults.get("medicine_type_id")),
        }

        if item_id:
            detail_df = data_fetch.fetch_item_master_detail(unit, item_id)
            if detail_df is None:
                return jsonify({"status": "error", "message": "Failed to fetch existing item"}), 500
            if detail_df.empty:
                return jsonify({"status": "error", "message": "Item not found"}), 404
            detail_df = _clean_df_columns(detail_df)
            existing = detail_df.iloc[0].to_dict()
            params = _build_item_update_params(
                item_id,
                item_payload,
                defaults,
                lookups,
                user_token,
                now_str,
                request.remote_addr,
                existing,
            )
        else:
            params = _build_item_master_params(item_payload, defaults, lookups, user_token, now_str, request.remote_addr)
        if _supports_contracted_rate(unit):
            params["ContractedRate"] = contracted_rate
        result = data_fetch.save_iv_item_with_manufacturer_link(
            unit,
            params,
            is_update=bool(item_id),
            manufacturer_id=manufacturer_id,
            user_token=user_token,
            now_str=now_str,
            remote_addr=request.remote_addr,
        )
        if result.get("error"):
            return jsonify({"status": "error", "message": result["error"]}), 500
        saved_item_id = _resolve_verified_item_master_id(unit, item_id or result.get("item_id"), name)
        if saved_item_id <= 0:
            return jsonify({
                "status": "error",
                "message": "Item save could not be verified in Item Master. Please refresh and try again.",
            }), 500
        try:
            _invalidate_item_master_meta_cache(unit)
        except Exception:
            pass
        try:
            _invalidate_item_name_cache(unit)
        except Exception:
            pass

        mirror_outcomes = []
        if not item_id:
            mirror_outcomes = _mirror_new_item_master_to_family(unit, item_payload, user_token, now_str, request.remote_addr)
        message, mirror_status = _build_replication_message(
            "Item",
            "updated" if item_id else "saved",
            saved_item_id,
            mirror_outcomes,
        )
        if manufacturer_id:
            if result.get("manufacturer_link_created") is False:
                message = f"{message} Manufacturer was already linked."
            else:
                message = f"{message} Manufacturer linked in the same save."
        return jsonify({
            "status": "success",
            "item_id": saved_item_id,
            "item_name": name,
            "unit": unit,
            "mirrored_units": [row.get("unit") for row in mirror_outcomes if row.get("status") == "mirrored"],
            "mirror_failures": [row for row in mirror_outcomes if row.get("status") == "failed"],
            "mirror_status": mirror_status,
            "manufacturer_id": manufacturer_id or None,
            "manufacturer_linked": bool(manufacturer_id),
            "manufacturer_link_created": result.get("manufacturer_link_created"),
            "message": message,
        })
