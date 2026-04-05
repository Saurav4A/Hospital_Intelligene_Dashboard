from flask import jsonify, request, session
from datetime import datetime
import math

from modules import data_fetch


def register_purchase_pm_indent_routes(
    app,
    *,
    login_required,
    get_purchase_unit,
    clean_df_columns,
    sanitize_json_payload,
    safe_float,
    safe_int,
    row_value,
    audit_log_event,
    local_tz,
):
    """Register Purchase PM indent routes."""
    _get_purchase_unit = get_purchase_unit
    _clean_df_columns = clean_df_columns
    _sanitize_json_payload = sanitize_json_payload
    _safe_float = safe_float
    _safe_int = safe_int
    _row_value = row_value
    _audit_log_event = audit_log_event
    LOCAL_TZ = local_tz
    def _first_numeric_value(row, cols):
        for col in cols:
            if not col:
                continue
            try:
                num = float(row.get(col))
            except Exception:
                continue
            if math.isfinite(num):
                return num
        return None

    def _get_item_master_rate_mrp_map(unit: str, item_ids: list[int]) -> dict[int, dict[str, float | None]]:
        if not item_ids:
            return {}
        df = data_fetch.fetch_item_master_rate_mrp(unit, item_ids)
        if df is None or df.empty:
            return {}
        df = _clean_df_columns(df)
        cols = {str(c).strip().lower(): c for c in df.columns}
        id_col = cols.get("itemid") or cols.get("id")
        rate_col = cols.get("standardrate") or cols.get("standard_rate") or cols.get("rate")
        mrp_col = cols.get("salesprice") or cols.get("sales_price") or cols.get("mrp")
        tax_col = cols.get("salestax") or cols.get("sales_tax") or cols.get("tax") or cols.get("vat")
        if not id_col:
            return {}
        out = {}
        for _, row in df.iterrows():
            item_id = row.get(id_col)
            try:
                item_id = int(item_id)
            except Exception:
                continue
            rate_val = _safe_float(row.get(rate_col), None) if rate_col else None
            mrp_val = _safe_float(row.get(mrp_col), None) if mrp_col else None
            tax_val = _safe_float(row.get(tax_col), None) if tax_col else None
            out[item_id] = {"rate": rate_val, "mrp": mrp_val, "tax": tax_val}
        return out

    @app.route('/api/purchase/pm_indent/init')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_pm_indent_init():
        unit, error = _get_purchase_unit()
        if error:
            return error

        # Avoid reserving indent numbers on page load; assign on save instead.
        indent_no = None
        dept_df = data_fetch.fetch_departments_for_indent(unit)
        pack_df = data_fetch.fetch_purchase_pack_sizes(unit)
        category_df = data_fetch.fetch_item_categories(unit)

        departments = []
        if dept_df is not None and not dept_df.empty:
            dept_df = _clean_df_columns(dept_df)
            cols = {str(c).strip().lower(): c for c in dept_df.columns}
            id_col = cols.get("department_id") or cols.get("departmentid") or cols.get("id")
            name_col = cols.get("department_name") or cols.get("name")
            code_col = cols.get("department_code") or cols.get("code")
            for _, row in dept_df.iterrows():
                departments.append({
                    "id": row.get(id_col),
                    "name": str(row.get(name_col) or "").strip(),
                    "code": str(row.get(code_col) or "").strip(),
                })

        pack_sizes = []
        if pack_df is not None and not pack_df.empty:
            pack_df = _clean_df_columns(pack_df)
            cols = {str(c).strip().lower(): c for c in pack_df.columns}
            id_col = cols.get("id")
            name_col = cols.get("name")
            if id_col and name_col:
                for _, row in pack_df.iterrows():
                    pack_sizes.append({
                        "id": row.get(id_col),
                        "name": str(row.get(name_col) or "").strip(),
                    })

        categories = []
        if category_df is not None and not category_df.empty:
            category_df = _clean_df_columns(category_df)
            cols = {str(c).strip().lower(): c for c in category_df.columns}
            id_col = cols.get("id")
            name_col = cols.get("name")
            code_col = cols.get("code")
            for _, row in category_df.iterrows():
                categories.append({
                    "id": row.get(id_col),
                    "name": str(row.get(name_col) or "").strip(),
                    "code": str(row.get(code_col) or "").strip(),
                })

        return jsonify({
            "status": "success",
            "unit": unit,
            "indent_no": indent_no,
            "departments": departments,
            "pack_sizes": pack_sizes,
            "categories": categories,
        })


    @app.route('/api/purchase/pm_indent/stores')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_pm_indent_stores():
        unit, error = _get_purchase_unit()
        if error:
            return error
        dept_id = request.args.get("department_id")
        if not dept_id:
            return jsonify({"status": "error", "message": "Department is required"}), 400
        try:
            dept_id = int(dept_id)
        except Exception:
            return jsonify({"status": "error", "message": "Invalid department"}), 400

        df = data_fetch.fetch_substores_for_department(unit, dept_id)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch stores"}), 500
        if df.empty:
            return jsonify({"status": "success", "stores": [], "unit": unit})

        df = _clean_df_columns(df)
        cols = {str(c).strip().lower(): c for c in df.columns}
        id_col = cols.get("id")
        name_col = cols.get("storename") or cols.get("name")
        code_col = cols.get("storecode") or cols.get("code")
        stores = []
        for _, row in df.iterrows():
            stores.append({
                "id": row.get(id_col),
                "name": str(row.get(name_col) or "").strip(),
                "code": str(row.get(code_col) or "").strip(),
            })
        return jsonify({"status": "success", "stores": stores, "unit": unit})


    @app.route('/api/purchase/pm_indent/items')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_pm_indent_items():
        unit, error = _get_purchase_unit()
        if error:
            return error
        item_type_id = request.args.get("item_type_id")
        if not item_type_id:
            return jsonify({"status": "error", "message": "Category type is required"}), 400
        try:
            item_type_id = int(item_type_id)
        except Exception:
            return jsonify({"status": "error", "message": "Invalid category type"}), 400

        df = data_fetch.fetch_indent_items_catalog(unit, item_type_id)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch items"}), 500
        if df.empty:
            return jsonify({"status": "success", "items": [], "unit": unit})

        df = _clean_df_columns(df)
        cols = {str(c).strip().lower(): c for c in df.columns}
        id_col = cols.get("id")
        name_col = cols.get("itemname") or cols.get("name")
        pack_col = cols.get("packsizeid") or cols.get("packsize_id")
        rate_col = cols.get("lastporate") or cols.get("rate")
        mrp_col = cols.get("mrp")
        vat_col = cols.get("vat")
        consumption_col = cols.get("code")
        stock_col = cols.get("unitname")
        unit_id_col = cols.get("unitid")

        consumption_df = data_fetch.fetch_last_30_day_item_consumption(unit)
        consumption_map = {}
        if consumption_df is not None and not consumption_df.empty:
            consumption_df = _clean_df_columns(consumption_df)
            cons_cols = {str(c).strip().lower(): c for c in consumption_df.columns}
            cons_item_col = cons_cols.get("itemid") or cons_cols.get("item_id")
            cons_qty_col = cons_cols.get("totalqtyconsumedlast30days") or cons_cols.get("totalqty") or cons_cols.get("qty")
            if cons_item_col and cons_qty_col:
                for _, row in consumption_df.iterrows():
                    try:
                        item_id = int(row[cons_item_col])
                    except Exception:
                        continue
                    try:
                        consumption_map[item_id] = float(row[cons_qty_col] or 0)
                    except Exception:
                        consumption_map[item_id] = 0.0
        item_master_map = {}
        if id_col:
            item_ids = []
            for val in df[id_col].tolist():
                try:
                    item_ids.append(int(val))
                except Exception:
                    continue
            item_master_map = _get_item_master_rate_mrp_map(unit, item_ids)

        rows = []
        for _, row in df.iterrows():
            item_id = row.get(id_col) if id_col else None
            try:
                item_id = int(item_id)
            except Exception:
                item_id = None
            rate_val = _safe_float(row.get(rate_col), None) if rate_col else None
            mrp_val = _safe_float(row.get(mrp_col), None) if mrp_col else None
            vat_val = _safe_float(row.get(vat_col), None) if vat_col else None
            fallback = item_master_map.get(item_id or -1, {})
            if rate_val is None or rate_val <= 0:
                rate_val = fallback.get("rate")
            if mrp_val is None or mrp_val <= 0:
                mrp_val = fallback.get("mrp")
            if vat_val is None or vat_val <= 0:
                vat_val = fallback.get("tax")
            rows.append({
                "id": row.get(id_col),
                "name": str(row.get(name_col) or "").strip(),
                "packsize_id": row.get(pack_col),
                "last_po_rate": rate_val or 0,
                "mrp": mrp_val or 0,
                "vat": vat_val or 0,
                "last_15_days": consumption_map.get(item_id, row.get(consumption_col)),
                "current_stock": row.get(stock_col),
                "unit_id": row.get(unit_id_col),
            })
        rows = _sanitize_json_payload(rows)
        return jsonify({"status": "success", "items": rows, "unit": unit})


    def _find_duplicate_purchase_item_refs(
        items: list,
        *,
        id_keys: tuple[str, ...] = ("item_id", "id", "ItemID", "ItemId"),
        name_keys: tuple[str, ...] = ("item_name", "name", "ItemName"),
        qty_keys: tuple[str, ...] = ("qty", "Qty", "item_qty", "ItemQty"),
        require_positive_qty: bool = False,
    ) -> dict:
        duplicate_ids = []
        duplicate_names = []
        seen_ids = set()
        seen_names = set()

        for raw_item in (items or []):
            item = raw_item or {}
            item_id = 0
            for key in id_keys:
                try:
                    item_id = int(item.get(key) or 0)
                except Exception:
                    item_id = 0
                if item_id:
                    break

            qty = 0.0
            for key in qty_keys:
                if key in item:
                    try:
                        qty = float(item.get(key) or 0)
                    except Exception:
                        qty = 0.0
                    break
            if require_positive_qty and qty <= 0:
                continue

            if item_id > 0:
                if item_id in seen_ids and item_id not in duplicate_ids:
                    duplicate_ids.append(item_id)
                seen_ids.add(item_id)
                continue

            # For rows without item id (manual entries), dedupe by item name.
            name_raw = ""
            for key in name_keys:
                if key in item and item.get(key) is not None:
                    name_raw = str(item.get(key) or "").strip()
                    if name_raw:
                        break
            name_key = name_raw.lower()
            if not name_key:
                continue
            if name_key in seen_names and name_raw not in duplicate_names:
                duplicate_names.append(name_raw)
            seen_names.add(name_key)

        return {"duplicate_ids": duplicate_ids, "duplicate_names": duplicate_names}


    def _format_duplicate_item_labels(items: list, duplicate_ids: list[int]) -> list[str]:
        labels = []
        for dup_id in duplicate_ids or []:
            name = ""
            for item in items or []:
                try:
                    item_id = int((item or {}).get("item_id") or (item or {}).get("id") or 0)
                except Exception:
                    item_id = 0
                if item_id != dup_id:
                    continue
                name = str((item or {}).get("item_name") or (item or {}).get("name") or "").strip()
                if name:
                    break
            labels.append(f"{dup_id} ({name})" if name else str(dup_id))
        return labels


    @app.route('/api/purchase/pm_indent', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_pm_indent_create():
        unit, error = _get_purchase_unit()
        if error:
            return error

        payload = request.get_json(silent=True) or {}
        header = payload.get("header") or {}
        items = payload.get("items") or []

        def _safe_int(val, default=0):
            try:
                return int(val)
            except Exception:
                return default

        department_id = _safe_int(header.get("department_id"))
        store_id = _safe_int(header.get("store_id"))
        item_category_id = _safe_int(header.get("item_category_id"))
        if not department_id:
            _audit_log_event(
                "purchase",
                "indent_create",
                status="error",
                entity_type="indent",
                unit=unit,
                summary="Department is required",
            )
            return jsonify({"status": "error", "message": "Department is required"}), 400
        if not store_id:
            _audit_log_event(
                "purchase",
                "indent_create",
                status="error",
                entity_type="indent",
                unit=unit,
                summary="Store is required",
            )
            return jsonify({"status": "error", "message": "Store is required"}), 400
        if not item_category_id:
            _audit_log_event(
                "purchase",
                "indent_create",
                status="error",
                entity_type="indent",
                unit=unit,
                summary="Category type is required",
            )
            return jsonify({"status": "error", "message": "Category type is required"}), 400
        if not items:
            _audit_log_event(
                "purchase",
                "indent_create",
                status="error",
                entity_type="indent",
                unit=unit,
                summary="At least one item is required",
            )
            return jsonify({"status": "error", "message": "At least one item is required"}), 400
        dup_check = _find_duplicate_purchase_item_refs(items, require_positive_qty=True)
        if dup_check["duplicate_ids"]:
            dup_labels = _format_duplicate_item_labels(items, dup_check["duplicate_ids"])
            preview = ", ".join(dup_labels[:8])
            more = f" (+{len(dup_labels) - 8} more)" if len(dup_labels) > 8 else ""
            msg = f"Duplicate items are not allowed in indent. Remove repeated item(s): {preview}{more}."
            _audit_log_event(
                "purchase",
                "indent_create",
                status="error",
                entity_type="indent",
                unit=unit,
                summary="Duplicate indent items detected",
                details={"duplicates": dup_labels},
            )
            return jsonify({"status": "error", "message": msg, "duplicates": dup_labels}), 400

        now = datetime.now(tz=LOCAL_TZ)
        updated_by = _safe_int(session.get("user_id") or session.get("user") or session.get("username"), 1)
        indent_date = header.get("indent_date") or now.strftime("%Y-%m-%d")
        delivery_start = header.get("delivery_start") or indent_date
        delivery_end = header.get("delivery_end") or "1900-01-01"

        indent_no = str(header.get("indent_no") or "").strip()
        indent_id = _safe_int(header.get("indent_id"))
        indent_params = {
            "pIndentid": indent_id,
            "pIndentnumber": indent_no,
            "pDepartmentid": department_id,
            "pBudgetid": _safe_int(header.get("budget_id")),
            "pRemarks": header.get("remarks") or "",
            "pPropindication": _safe_int(header.get("prop_indication"), -1),
            "pIndentnature": header.get("indent_nature") or "",
            "pDeliverystartdate": delivery_start,
            "pDeliveryenddate": delivery_end,
            "pItemcategoryid": item_category_id,
            "pUpdatedon": now.strftime("%Y-%m-%d %H:%M:%S"),
            "pUpdatedby": updated_by,
            "pStatus": header.get("status") or "P",
            "pAuthorisedremarks": header.get("authorised_remarks") or "",
            "pAuthorisedby": _safe_int(header.get("authorised_by")),
            "pAuthorisedon": header.get("authorised_on") or "1900-01-01",
            "pAuthorised": _safe_int(header.get("authorised")),
            "pProcurementId": _safe_int(header.get("procurement_id")),
            "pStoreId": store_id,
            "pInsertedby": updated_by,
        }

        if indent_id:
            mst_result = data_fetch.add_pm_indent_mst(unit, indent_params)
        else:
            mst_result = data_fetch.add_pm_indent_mst_with_autonumber(unit, indent_params)
        if mst_result.get("error"):
            _audit_log_event(
                "purchase",
                "indent_create",
                status="error",
                entity_type="indent",
                unit=unit,
                summary="Indent creation failed",
                details={"error": mst_result.get("error")},
            )
            return jsonify({"status": "error", "message": mst_result["error"]}), 500

        indent_id = mst_result.get("indent_id") or indent_id
        indent_no = mst_result.get("indent_no") or indent_no
        if not indent_id and indent_no:
            indent_id = data_fetch.fetch_indent_id_by_number(unit, indent_no)
        if not indent_id:
            _audit_log_event(
                "purchase",
                "indent_create",
                status="error",
                entity_type="indent",
                unit=unit,
                summary="Failed to create indent",
                details={"indent_no": indent_no},
            )
            return jsonify({"status": "error", "message": "Failed to create indent"}), 500

        detail_errors = []
        for item in items:
            try:
                item_id = _safe_int(item.get("item_id") or item.get("id"))
                qty = float(item.get("qty") or 0)
                if not item_id or qty <= 0:
                    continue
                rate = float(item.get("rate") or item.get("last_po_rate") or 0)
                tax_pct = float(item.get("tax_pct") or item.get("vat") or 0)
                tax_amount = float(item.get("tax_amount") or 0)
                if tax_amount == 0 and tax_pct:
                    tax_amount = (rate * qty) * tax_pct / 100
                estimated_cost = float(item.get("estimated_cost") or 0)
                if estimated_cost == 0:
                    estimated_cost = (rate * qty) + tax_amount

                detail_params = {
                    "pIndentdetailid": _safe_int(item.get("detail_id")),
                    "pIndentid": int(indent_id),
                    "pItemid": item_id,
                    "pItemrate": rate,
                    "pItemqty": qty,
                    "pEstimatedcost": estimated_cost,
                    "pSalestax": float(item.get("sales_tax") or 0),
                    "pExcisetax": float(item.get("excise_tax") or 0),
                    "pEscalated": _safe_int(item.get("escalated")),
                    "pLandingrate": float(item.get("landing_rate") or 0),
                    "pDeliveryStartDate": item.get("delivery_start") or delivery_start or "1900-01-01",
                    "pDeliveryendDate": item.get("delivery_end") or delivery_end or "1900-01-01",
                    "pAuthoriseQty": float(item.get("authorise_qty") or 0),
                    "ppacksizeId": _safe_int(item.get("packsize_id")),
                    "pfreeqty": float(item.get("free_qty") or 0),
                    "pDiscount": float(item.get("discount") or 0),
                    "pTax": tax_pct,
                    "pTaxAmount": tax_amount,
                    "pVATOn": item.get("vat_on") or "M",
                    "pVAT": item.get("vat_code") or "E",
                    "pMRP": float(item.get("mrp") or 0),
                    "pConsumeQty": float(item.get("consume_qty") or 0),
                    "pIssueQty": float(item.get("issue_qty") or 0),
                }
                res = data_fetch.add_pm_indent_details(unit, detail_params)
                if res.get("error"):
                    detail_errors.append(res["error"])
            except Exception as e:
                detail_errors.append(str(e))

        if detail_errors:
            _audit_log_event(
                "purchase",
                "indent_create",
                status="partial",
                entity_type="indent",
                entity_id=str(indent_id),
                unit=unit,
                summary="Indent created but some items failed",
                details={"indent_no": indent_no, "errors": detail_errors},
            )
            return jsonify({
                "status": "partial",
                "indent_id": indent_id,
                "indent_no": indent_no,
                "message": "Indent created but some items failed",
                "errors": detail_errors,
            }), 207

        _audit_log_event(
            "purchase",
            "indent_create",
            status="success",
            entity_type="indent",
            entity_id=str(indent_id),
            unit=unit,
            summary="Indent created",
            details={
                "indent_no": indent_no,
                "department_id": department_id,
                "store_id": store_id,
                "item_category_id": item_category_id,
                "item_count": len(items),
                "status": indent_params.get("pStatus"),
            },
        )
        return jsonify({"status": "success", "indent_id": indent_id, "indent_no": indent_no})


    @app.route('/api/purchase/pm_indent/pending')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_pm_indent_pending():
        unit, error = _get_purchase_unit()
        if error:
            return error
        df = data_fetch.fetch_pending_pm_indents(unit)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch pending indents"}), 500
        if df.empty:
            return jsonify({"status": "success", "indents": [], "unit": unit})

        df = _clean_df_columns(df)
        cols = {str(c).strip().lower(): c for c in df.columns}
        id_col = cols.get("indentid") or cols.get("id")
        num_col = cols.get("indent number") or cols.get("indentnumber")
        date_col = cols.get("indent date") or cols.get("indentdate") or cols.get("deliverystartdate")
        status_col = cols.get("status") or cols.get("indentstatus") or cols.get("indent_status")
        rows = []
        for _, row in df.iterrows():
            rows.append({
                "id": row.get(id_col),
                "number": str(row.get(num_col) or "").strip(),
                "date": row.get(date_col),
                "status": str(row.get(status_col) or "").strip().upper() if status_col else "",
            })
        rows = _sanitize_json_payload(rows)
        return jsonify({"status": "success", "indents": rows, "unit": unit})


    @app.route('/api/purchase/pm_indent/drafts')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_pm_indent_drafts():
        unit, error = _get_purchase_unit()
        if error:
            return error
        df = data_fetch.fetch_pm_indents_by_status(unit, ["D", "P"])
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch draft indents"}), 500
        if df.empty:
            return jsonify({"status": "success", "indents": [], "unit": unit})

        df = _clean_df_columns(df)
        cols = {str(c).strip().lower(): c for c in df.columns}
        id_col = cols.get("indentid") or cols.get("id")
        num_col = cols.get("indentnumber") or cols.get("indent number")
        date_col = cols.get("deliverystartdate") or cols.get("indentdate") or cols.get("indent date")
        status_col = cols.get("status")
        rows = []
        for _, row in df.iterrows():
            rows.append({
                "id": row.get(id_col),
                "number": str(row.get(num_col) or "").strip(),
                "date": row.get(date_col),
                "status": str(row.get(status_col) or "").strip().upper(),
            })
        rows = _sanitize_json_payload(rows)
        return jsonify({"status": "success", "indents": rows, "unit": unit})


    @app.route('/api/purchase/pm_indent/<int:indent_id>')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_pm_indent_details(indent_id: int):
        unit, error = _get_purchase_unit()
        if error:
            return error

        mst_df = data_fetch.fetch_pm_indent_mst_other(unit, indent_id)
        if mst_df is None:
            return jsonify({"status": "error", "message": "Failed to fetch indent"}), 500
        if mst_df.empty:
            return jsonify({"status": "error", "message": "Indent not found"}), 404

        mst_df = _clean_df_columns(mst_df)
        mst_row = mst_df.iloc[0].to_dict()
        header = _sanitize_json_payload(mst_row)

        items_df = data_fetch.fetch_pm_indent_items_detail(unit, indent_id)
        if items_df is None:
            return jsonify({"status": "error", "message": "Failed to fetch indent items"}), 500
        items = []
        if not items_df.empty:
            items_df = _clean_df_columns(items_df)
            items = _sanitize_json_payload(items_df.to_dict(orient="records"))

        return jsonify({"status": "success", "header": header, "items": items, "unit": unit})


    @app.route('/api/purchase/pm_indent_update', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_pm_indent_update():
        unit, error = _get_purchase_unit()
        if error:
            return error

        payload = request.get_json(silent=True) or {}
        header = payload.get("header") or {}
        items = payload.get("items") or []

        def _safe_int(val, default=0):
            try:
                return int(val)
            except Exception:
                return default

        indent_id = _safe_int(header.get("indent_id"))
        if not indent_id:
            _audit_log_event(
                "purchase",
                "indent_update",
                status="error",
                entity_type="indent",
                unit=unit,
                summary="Indent ID is required for update",
            )
            return jsonify({"status": "error", "message": "Indent ID is required for update"}), 400
        if not items:
            _audit_log_event(
                "purchase",
                "indent_update",
                status="error",
                entity_type="indent",
                entity_id=str(indent_id),
                unit=unit,
                summary="At least one item is required",
            )
            return jsonify({"status": "error", "message": "At least one item is required"}), 400
        dup_check = _find_duplicate_purchase_item_refs(items, require_positive_qty=True)
        if dup_check["duplicate_ids"]:
            dup_labels = _format_duplicate_item_labels(items, dup_check["duplicate_ids"])
            preview = ", ".join(dup_labels[:8])
            more = f" (+{len(dup_labels) - 8} more)" if len(dup_labels) > 8 else ""
            msg = f"Duplicate items are not allowed in indent. Remove repeated item(s): {preview}{more}."
            _audit_log_event(
                "purchase",
                "indent_update",
                status="error",
                entity_type="indent",
                entity_id=str(indent_id),
                unit=unit,
                summary="Duplicate indent items detected",
                details={"duplicates": dup_labels},
            )
            return jsonify({"status": "error", "message": msg, "duplicates": dup_labels}), 400

        mst_df = data_fetch.fetch_pm_indent_mst_other(unit, indent_id)
        if mst_df is None:
            _audit_log_event(
                "purchase",
                "indent_update",
                status="error",
                entity_type="indent",
                entity_id=str(indent_id),
                unit=unit,
                summary="Failed to fetch indent",
            )
            return jsonify({"status": "error", "message": "Failed to fetch indent"}), 500
        if mst_df.empty:
            _audit_log_event(
                "purchase",
                "indent_update",
                status="error",
                entity_type="indent",
                entity_id=str(indent_id),
                unit=unit,
                summary="Indent not found",
            )
            return jsonify({"status": "error", "message": "Indent not found"}), 404
        mst_df = _clean_df_columns(mst_df)
        mst_row = mst_df.iloc[0].to_dict()
        status_raw = str(_row_value(mst_row, ["Status"]) or "").strip().upper()
        if status_raw != "D":
            _audit_log_event(
                "purchase",
                "indent_update",
                status="error",
                entity_type="indent",
                entity_id=str(indent_id),
                unit=unit,
                summary="Only Draft indents can be updated",
                details={"current_status": status_raw},
            )
            return jsonify({"status": "error", "message": "Only Draft indents can be updated"}), 400

        now = datetime.now(tz=LOCAL_TZ)
        updated_by = _safe_int(session.get("user_id") or session.get("user") or session.get("username"), 1)
        indent_date = header.get("indent_date") or _row_value(mst_row, ["DeliveryStartDate", "IndentDate"]) or now.strftime("%Y-%m-%d")
        delivery_start = header.get("delivery_start") or indent_date
        delivery_end = header.get("delivery_end") or _row_value(mst_row, ["DeliveryEndDate"]) or "1900-01-01"

        indent_params = {
            "pIndentid": indent_id,
            "pIndentnumber": header.get("indent_no") or _row_value(mst_row, ["IndentNumber"]) or "",
            "pDepartmentid": _safe_int(header.get("department_id") or _row_value(mst_row, ["DepartmentId"])),
            "pBudgetid": _safe_int(header.get("budget_id") or _row_value(mst_row, ["BudgetId"])),
            "pRemarks": header.get("remarks") or _row_value(mst_row, ["Remarks"]) or "",
            "pPropindication": _safe_int(header.get("prop_indication") or _row_value(mst_row, ["PropIndication"]), -1),
            "pIndentnature": header.get("indent_nature") or _row_value(mst_row, ["IndentNature"]) or "",
            "pDeliverystartdate": delivery_start,
            "pDeliveryenddate": delivery_end,
            "pItemcategoryid": _safe_int(header.get("item_category_id") or _row_value(mst_row, ["ItemCategoryId"])),
            "pUpdatedon": now.strftime("%Y-%m-%d %H:%M:%S"),
            "pUpdatedby": updated_by,
            "pStatus": header.get("status") or status_raw or "P",
            "pAuthorisedremarks": header.get("authorised_remarks") or _row_value(mst_row, ["AuthorisedRemarks"]) or "",
            "pAuthorisedby": _safe_int(header.get("authorised_by") or _row_value(mst_row, ["AuthorisedBy"])),
            "pAuthorisedon": header.get("authorised_on") or _row_value(mst_row, ["AuthorisedOn"]) or "1900-01-01",
            "pAuthorised": _safe_int(header.get("authorised") or _row_value(mst_row, ["Authorised"])),
            "pProcurementId": _safe_int(header.get("procurement_id") or _row_value(mst_row, ["ProcurementId"])),
            "pStoreId": _safe_int(header.get("store_id") or _row_value(mst_row, ["StoreId"])),
        }

        upd_result = data_fetch.update_pm_indent_mst(unit, indent_params)
        if upd_result.get("error"):
            _audit_log_event(
                "purchase",
                "indent_update",
                status="error",
                entity_type="indent",
                entity_id=str(indent_id),
                unit=unit,
                summary="Indent update failed",
                details={"error": upd_result.get("error")},
            )
            return jsonify({"status": "error", "message": upd_result["error"]}), 500

        clear_result = data_fetch.clear_pm_indent_details(unit, indent_id)
        if clear_result.get("error"):
            _audit_log_event(
                "purchase",
                "indent_update",
                status="error",
                entity_type="indent",
                entity_id=str(indent_id),
                unit=unit,
                summary="Failed to clear indent items before update",
                details={"error": clear_result.get("error")},
            )
            return jsonify({"status": "error", "message": clear_result["error"]}), 500

        detail_errors = []
        for item in items:
            try:
                item_id = _safe_int(item.get("item_id") or item.get("id"))
                qty = float(item.get("qty") or 0)
                if not item_id or qty <= 0:
                    continue
                rate = float(item.get("rate") or item.get("last_po_rate") or 0)
                tax_pct = float(item.get("tax_pct") or item.get("vat") or 0)
                tax_amount = float(item.get("tax_amount") or 0)
                if tax_amount == 0 and tax_pct:
                    tax_amount = (rate * qty) * tax_pct / 100
                estimated_cost = float(item.get("estimated_cost") or 0)
                if estimated_cost == 0:
                    estimated_cost = (rate * qty) + tax_amount

                detail_params = {
                    "pIndentdetailid": _safe_int(item.get("detail_id")),
                    "pIndentid": int(indent_id),
                    "pItemid": item_id,
                    "pItemrate": rate,
                    "pItemqty": qty,
                    "pEstimatedcost": estimated_cost,
                    "pSalestax": float(item.get("sales_tax") or 0),
                    "pExcisetax": float(item.get("excise_tax") or 0),
                    "pEscalated": _safe_int(item.get("escalated")),
                    "pLandingrate": float(item.get("landing_rate") or 0),
                    "pDeliveryStartDate": item.get("delivery_start") or delivery_start or "1900-01-01",
                    "pDeliveryendDate": item.get("delivery_end") or delivery_end or "1900-01-01",
                    "pAuthoriseQty": float(item.get("authorise_qty") or 0),
                    "ppacksizeId": _safe_int(item.get("packsize_id")),
                    "pfreeqty": float(item.get("free_qty") or 0),
                    "pDiscount": float(item.get("discount") or 0),
                    "pTax": tax_pct,
                    "pTaxAmount": tax_amount,
                    "pVATOn": item.get("vat_on") or "M",
                    "pVAT": item.get("vat_code") or "E",
                    "pMRP": float(item.get("mrp") or 0),
                    "pConsumeQty": float(item.get("consume_qty") or 0),
                    "pIssueQty": float(item.get("issue_qty") or 0),
                }
                res = data_fetch.add_pm_indent_details(unit, detail_params)
                if res.get("error"):
                    detail_errors.append(res["error"])
            except Exception as e:
                detail_errors.append(str(e))

        if detail_errors:
            _audit_log_event(
                "purchase",
                "indent_update",
                status="partial",
                entity_type="indent",
                entity_id=str(indent_id),
                unit=unit,
                summary="Indent updated but some items failed",
                details={"errors": detail_errors, "indent_no": indent_params.get("pIndentnumber")},
            )
            return jsonify({
                "status": "partial",
                "indent_id": indent_id,
                "message": "Indent updated but some items failed",
                "errors": detail_errors,
            }), 207

        _audit_log_event(
            "purchase",
            "indent_update",
            status="success",
            entity_type="indent",
            entity_id=str(indent_id),
            unit=unit,
            summary="Indent updated",
            details={
                "indent_no": indent_params.get("pIndentnumber"),
                "old_status": status_raw,
                "new_status": indent_params.get("pStatus"),
                "item_count": len(items),
            },
        )
        return jsonify({"status": "success", "indent_id": indent_id, "indent_no": indent_params.get("pIndentnumber")})


    @app.route('/api/purchase/pm_indent/authorize', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_pm_indent_authorize():
        unit, error = _get_purchase_unit()
        if error:
            return error
        payload = request.get_json(silent=True) or {}
        indent_id = payload.get("indent_id")
        action = str(payload.get("action") or "").strip().lower()
        remarks = payload.get("remarks") or ""
        if not indent_id:
            _audit_log_event(
                "purchase",
                "indent_authorize",
                status="error",
                entity_type="indent",
                unit=unit,
                summary="Indent ID is required",
            )
            return jsonify({"status": "error", "message": "Indent ID is required"}), 400
        if action not in {"authorize", "cancel"}:
            _audit_log_event(
                "purchase",
                "indent_authorize",
                status="error",
                entity_type="indent",
                entity_id=str(indent_id),
                unit=unit,
                summary="Invalid action",
                details={"action": action},
            )
            return jsonify({"status": "error", "message": "Invalid action"}), 400

        mst_df = data_fetch.fetch_pm_indent_mst_other(unit, int(indent_id))
        if mst_df is None:
            _audit_log_event(
                "purchase",
                "indent_authorize",
                status="error",
                entity_type="indent",
                entity_id=str(indent_id),
                unit=unit,
                summary="Failed to fetch indent",
            )
            return jsonify({"status": "error", "message": "Failed to fetch indent"}), 500
        if mst_df.empty:
            _audit_log_event(
                "purchase",
                "indent_authorize",
                status="error",
                entity_type="indent",
                entity_id=str(indent_id),
                unit=unit,
                summary="Indent not found",
            )
            return jsonify({"status": "error", "message": "Indent not found"}), 404

        mst_df = _clean_df_columns(mst_df)
        row = {str(k).strip().lower(): v for k, v in mst_df.iloc[0].to_dict().items()}
        now = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        try:
            updated_by = int(payload.get("user_id") or session.get("user_id") or session.get("user") or session.get("username") or 1)
        except Exception:
            updated_by = 1
        status_val = "A" if action == "authorize" else "C"
        authorised_flag = -1 if action == "authorize" else 0

        def _pick(*names, default=None):
            for name in names:
                key = name.lower()
                if key in row:
                    return row.get(key)
            return default

        params = {
            "pIndentid": int(indent_id),
            "pIndentnumber": _pick("IndentNumber", "Indent Number", default=""),
            "pDepartmentid": _pick("DepartmentId", default=0),
            "pBudgetid": _pick("BudgetId", default=0),
            "pRemarks": _pick("Remarks", default=""),
            "pPropindication": _pick("PropIndication", default=-1),
            "pIndentnature": _pick("IndentNature", default=""),
            "pDeliverystartdate": _pick("DeliveryStartDate", default="1900-01-01"),
            "pDeliveryenddate": _pick("DeliveryEndDate", default="1900-01-01"),
            "pItemcategoryid": _pick("ItemCategoryId", default=0),
            "pUpdatedon": now,
            "pUpdatedby": updated_by,
            "pStatus": status_val,
            "pAuthorisedremarks": remarks,
            "pAuthorisedby": updated_by if action == "authorize" else 0,
            "pAuthorisedon": now,
            "pAuthorised": authorised_flag,
            "pProcurementId": _pick("ProcurementTypeID", "ProcurementTypeId", default=0),
            "pStoreId": _pick("Storeid", "StoreId", default=0),
        }

        result = data_fetch.update_pm_indent_mst(unit, params)
        if result.get("error"):
            _audit_log_event(
                "purchase",
                "indent_authorize",
                status="error",
                entity_type="indent",
                entity_id=str(indent_id),
                unit=unit,
                summary="Indent authorization failed",
                details={"error": result.get("error"), "action": action},
            )
            return jsonify({"status": "error", "message": result["error"]}), 500

        detail_update = None
        if action == "authorize":
            detail_update = data_fetch.update_pm_indent_details_authorised_qty(unit, int(indent_id), None)
        else:
            detail_update = data_fetch.update_pm_indent_details_authorised_qty(unit, int(indent_id), 0)
        if detail_update and detail_update.get("error"):
            _audit_log_event(
                "purchase",
                "indent_authorize",
                status="error",
                entity_type="indent",
                entity_id=str(indent_id),
                unit=unit,
                summary="Indent detail update failed",
                details={"error": detail_update.get("error"), "action": action},
            )
            return jsonify({"status": "error", "message": detail_update["error"]}), 500

        _audit_log_event(
            "purchase",
            "indent_authorize",
            status="success",
            entity_type="indent",
            entity_id=str(indent_id),
            unit=unit,
            summary="Indent authorization updated",
            details={"action": action, "remarks": remarks},
        )
        return jsonify({"status": "success", "indent_id": int(indent_id), "action": action})


    @app.route('/api/purchase/indents')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_indents():
        unit, error = _get_purchase_unit()
        if error:
            return error

        df = data_fetch.fetch_authorized_po_indents(unit)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch indents"}), 500
        if df.empty:
            return jsonify({"status": "success", "indents": []})

        df = _clean_df_columns(df)
        cols_map = {str(c).strip().lower(): c for c in df.columns}
        id_col = cols_map.get("indentid") or cols_map.get("id")
        num_col = cols_map.get("indentnumber") or cols_map.get("indent_no") or cols_map.get("indentno")
        date_col = (
            cols_map.get("indentdate")
            or cols_map.get("indent date")
            or cols_map.get("indent_date")
            or cols_map.get("indentdt")
            or cols_map.get("indent_dt")
            or cols_map.get("deliverydate")
            or cols_map.get("deliverystartdate")
            or cols_map.get("delivery start date")
            or cols_map.get("date")
        )
        if not date_col:
            for col in df.columns:
                col_l = str(col).strip().lower()
                if "date" in col_l and ("indent" in col_l or "delivery" in col_l):
                    date_col = col
                    break
        if not date_col:
            for col in df.columns:
                col_l = str(col).strip().lower()
                if "date" in col_l:
                    date_col = col
                    break

        indents = []
        for _, row in df.iterrows():
            indents.append({
                "id": row.get(id_col),
                "number": str(row.get(num_col) or "").strip(),
                "date": row.get(date_col),
            })
        indents = _sanitize_json_payload(indents)
        indents.sort(key=lambda r: (r.get("id") or 0), reverse=True)
        return jsonify({"status": "success", "indents": indents, "unit": unit})


    @app.route('/api/purchase/indent/<int:indent_id>/details')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_indent_details(indent_id: int):
        unit, error = _get_purchase_unit()
        if error:
            return error
        df = data_fetch.fetch_indent_details_for_po(unit, indent_id)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch indent details"}), 500
        if df.empty:
            return jsonify({"status": "success", "items": [], "unit": unit})

        df = _clean_df_columns(df)
        cols = []
        seen = {}
        for col in df.columns:
            base = str(col).strip()
            if base in seen:
                seen[base] += 1
                cols.append(f"{base}_{seen[base]}")
            else:
                seen[base] = 0
                cols.append(base)
        df.columns = cols
        rows = df.to_dict(orient="records")
        for row in rows:
            qty_val = row.get("ItemQty")
            if qty_val in (None, ""):
                for key in row.keys():
                    key_l = key.lower()
                    if key_l in ("itemqty", "item_qty", "indentqty", "indent_qty", "reqqty", "requiredqty", "required_qty", "qty"):
                        qty_val = row.get(key)
                        break
                if qty_val in (None, ""):
                    for key in row.keys():
                        if "itemqty" in key.lower():
                            qty_val = row.get(key)
                            break
            row["ItemQty"] = qty_val
        rows = _sanitize_json_payload(rows)
        return jsonify({"status": "success", "items": rows, "unit": unit})


    @app.route('/api/purchase/indent/<int:indent_id>/suppliers')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_indent_suppliers(indent_id: int):
        unit, error = _get_purchase_unit()
        if error:
            return error
        df = data_fetch.fetch_indent_suppliers_for_po(unit, indent_id)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch indent suppliers"}), 500
        if df.empty:
            return jsonify({"status": "success", "suppliers": [], "unit": unit})
        df = _clean_df_columns(df)
        rows = _sanitize_json_payload(df.to_dict(orient="records"))
        return jsonify({"status": "success", "suppliers": rows, "unit": unit})


    @app.route('/api/purchase/items_last_rate')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_items_last_rate():
        unit, error = _get_purchase_unit()
        if error:
            return error
        df = data_fetch.fetch_items_last_po_rate(unit)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch items"}), 500
        if df.empty:
            return jsonify({"status": "success", "items": [], "unit": unit})

        df = _clean_df_columns(df)
        stock_df = data_fetch.fetch_store_stock_summary(unit, [2, 3, 15])
        stock_map = {}
        if stock_df is not None and not stock_df.empty:
            stock_df = _clean_df_columns(stock_df)
            stock_cols = {str(c).strip().lower(): c for c in stock_df.columns}
            stock_item_col = stock_cols.get("itemid") or stock_cols.get("item_id") or stock_cols.get("id")
            stock_qty_col = stock_cols.get("currentstock") or stock_cols.get("stock") or stock_cols.get("qty") or stock_cols.get("balance")
            if stock_item_col and stock_qty_col:
                for _, row in stock_df.iterrows():
                    try:
                        item_id = int(row[stock_item_col])
                    except Exception:
                        continue
                    try:
                        stock_map[item_id] = float(row[stock_qty_col] or 0)
                    except Exception:
                        stock_map[item_id] = 0.0

        consumption_df = data_fetch.fetch_last_30_day_item_consumption(unit)
        consumption_map = {}
        if consumption_df is not None and not consumption_df.empty:
            consumption_df = _clean_df_columns(consumption_df)
            cons_cols = {str(c).strip().lower(): c for c in consumption_df.columns}
            cons_item_col = cons_cols.get("itemid") or cons_cols.get("item_id")
            cons_qty_col = cons_cols.get("totalqtyconsumedlast30days") or cons_cols.get("totalqty") or cons_cols.get("qty")
            if cons_item_col and cons_qty_col:
                for _, row in consumption_df.iterrows():
                    try:
                        item_id = int(row[cons_item_col])
                    except Exception:
                        continue
                    try:
                        consumption_map[item_id] = float(row[cons_qty_col] or 0)
                    except Exception:
                        consumption_map[item_id] = 0.0

        cols = {str(c).strip().lower(): c for c in df.columns}
        id_col = next((c for c in df.columns if str(c).strip().lower() in ("id", "itemid", "item_id")), None)
        if id_col:
            item_ids = []
            for val in df[id_col].tolist():
                try:
                    item_ids.append(int(val))
                except Exception:
                    continue
            item_master_map = _get_item_master_rate_mrp_map(unit, item_ids)
            rate_cols = [
                cols.get("lastporate"),
                cols.get("last_po_rate"),
                cols.get("itemrate"),
                cols.get("rate"),
                cols.get("standardrate"),
            ]
            mrp_cols = [
                cols.get("mrp"),
                cols.get("salesprice"),
                cols.get("sales_price"),
            ]
            tax_cols = [
                cols.get("vat"),
                cols.get("salestax"),
                cols.get("sales_tax"),
                cols.get("tax"),
            ]
            rates = []
            mrps = []
            taxes = []
            for _, row in df.iterrows():
                try:
                    item_id = int(row.get(id_col))
                except Exception:
                    item_id = None
                fallback = item_master_map.get(item_id or -1, {})
                rate_val = _first_numeric_value(row, rate_cols)
                if rate_val is None or rate_val <= 0:
                    rate_val = fallback.get("rate")
                if rate_val is None:
                    rate_val = 0.0
                mrp_val = _first_numeric_value(row, mrp_cols)
                if mrp_val is None or mrp_val <= 0:
                    mrp_val = fallback.get("mrp")
                if mrp_val is None:
                    mrp_val = 0.0
                tax_val = _first_numeric_value(row, tax_cols)
                if tax_val is None or tax_val <= 0:
                    tax_val = fallback.get("tax")
                if tax_val is None:
                    tax_val = 0.0
                rates.append(rate_val)
                mrps.append(mrp_val)
                taxes.append(tax_val)

            df["LastPoRate"] = rates
            df["ItemRate"] = rates
            df["MRP"] = mrps
            df["Mrp"] = mrps
            df["VAT"] = taxes
            df["Tax"] = taxes

            def _map_stock_value(val):
                try:
                    item_id = int(val)
                except Exception:
                    return 0.0
                return stock_map.get(item_id, 0.0)

            df["CurrentStock"] = df[id_col].apply(_map_stock_value)
            def _map_consumption_value(val):
                try:
                    item_id = int(val)
                except Exception:
                    return 0.0
                return consumption_map.get(item_id, 0.0)

            df["Last30Days"] = df[id_col].apply(_map_consumption_value)
        else:
            df["CurrentStock"] = 0.0
            df["Last30Days"] = 0.0
        rows = _sanitize_json_payload(df.to_dict(orient="records"))
        return jsonify({"status": "success", "items": rows, "unit": unit})
