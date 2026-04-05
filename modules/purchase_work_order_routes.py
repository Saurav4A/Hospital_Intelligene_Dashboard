from flask import jsonify, request, send_file, session
from datetime import date, datetime

from modules import data_fetch


def register_purchase_work_order_routes(
    app,
    *,
    login_required,
    get_work_order_unit,
    ensure_work_order_schema,
    build_purchase_department_payload,
    clean_df_columns,
    normalize_purchase_unit_text,
    sanitize_json_payload,
    safe_float,
    safe_int,
    audit_log_event,
    build_work_order_fallback_item,
    build_work_order_header_payload,
    build_work_order_pdf_buffer,
    build_work_order_supplier_dispatch_email_body,
    create_work_order_otp_request,
    fallback_work_order_summary_from_header,
    fetch_latest_purchase_otp_request,
    fetch_purchase_approval_meta,
    fetch_purchase_otp_request_by_id,
    format_indian_currency,
    insert_purchase_otp_request,
    is_truthy,
    mark_central_otp_used,
    mark_purchase_otp_used,
    normalize_work_order_body_html,
    normalize_work_order_header_for_audit,
    normalize_work_order_items,
    purchase_is_valid_email,
    purchase_normalize_email,
    resolve_po_print_format_for_render,
    resolve_purchasing_department_name,
    resolve_work_order_totals,
    send_graph_mail_with_attachment,
    send_purchase_work_order_approval_email,
    summarize_work_order_totals,
    validate_purchase_otp,
    work_order_body_to_plain_text,
    otp_work_order_request_type,
    local_tz,
):
    """Register Purchase work-order routes."""
    _get_work_order_unit = get_work_order_unit
    _ensure_work_order_schema = ensure_work_order_schema
    _build_purchase_department_payload = build_purchase_department_payload
    _clean_df_columns = clean_df_columns
    _normalize_purchase_unit_text = normalize_purchase_unit_text
    _sanitize_json_payload = sanitize_json_payload
    _safe_float = safe_float
    _safe_int = safe_int
    _audit_log_event = audit_log_event
    _build_work_order_fallback_item = build_work_order_fallback_item
    _build_work_order_header_payload = build_work_order_header_payload
    _build_work_order_pdf_buffer = build_work_order_pdf_buffer
    _build_work_order_supplier_dispatch_email_body = build_work_order_supplier_dispatch_email_body
    _create_work_order_otp_request = create_work_order_otp_request
    _fallback_work_order_summary_from_header = fallback_work_order_summary_from_header
    _fetch_latest_purchase_otp_request = fetch_latest_purchase_otp_request
    _fetch_purchase_approval_meta = fetch_purchase_approval_meta
    _fetch_purchase_otp_request_by_id = fetch_purchase_otp_request_by_id
    _format_indian_currency = format_indian_currency
    _insert_purchase_otp_request = insert_purchase_otp_request
    _is_truthy = is_truthy
    _mark_central_otp_used = mark_central_otp_used
    _mark_purchase_otp_used = mark_purchase_otp_used
    _normalize_work_order_body_html = normalize_work_order_body_html
    _normalize_work_order_header_for_audit = normalize_work_order_header_for_audit
    _normalize_work_order_items = normalize_work_order_items
    _purchase_is_valid_email = purchase_is_valid_email
    _purchase_normalize_email = purchase_normalize_email
    _resolve_po_print_format_for_render = resolve_po_print_format_for_render
    _resolve_purchasing_department_name = resolve_purchasing_department_name
    _resolve_work_order_totals = resolve_work_order_totals
    _send_graph_mail_with_attachment = send_graph_mail_with_attachment
    _send_purchase_work_order_approval_email = send_purchase_work_order_approval_email
    _summarize_work_order_totals = summarize_work_order_totals
    _validate_purchase_otp = validate_purchase_otp
    _work_order_body_to_plain_text = work_order_body_to_plain_text
    OTP_WORK_ORDER_REQUEST_TYPE = otp_work_order_request_type
    LOCAL_TZ = local_tz

    @app.route('/api/purchase/work_order/init')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_work_order_init():
        unit, error = _get_work_order_unit()
        if error:
            return error
        _ensure_work_order_schema(unit)

        unit_df = data_fetch.fetch_purchase_unit_master(unit, include_inactive=False)
        supplier_df = data_fetch.fetch_iv_suppliers(unit)
        purchasing_departments_payload, default_purchasing_dept_id = _build_purchase_department_payload(unit)

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
                suppliers.append(
                    {
                        "id": row.get(id_col),
                        "name": str(row.get(name_col) or "").strip(),
                        "code": str(row.get(code_col) or "").strip(),
                        "email": str(row.get(email_col) or "").strip(),
                    }
                )

        return jsonify(
            {
                "status": "success",
                "unit": unit,
                "wo_no": None,
                "units": _sanitize_json_payload(units),
                "suppliers": suppliers,
                "purchasing_departments": _sanitize_json_payload(purchasing_departments_payload),
                "default_purchasing_dept_id": default_purchasing_dept_id,
            }
        )


    @app.route('/api/purchase/work_order_lookup')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_work_order_lookup():
        unit, error = _get_work_order_unit()
        if error:
            return error
        _ensure_work_order_schema(unit)

        query = (request.args.get("q") or "").strip()
        if not query:
            return jsonify({"status": "error", "message": "Please enter a Work Order number or ID"}), 400

        wo_id = None
        wo_no = None
        if query.isdigit():
            wo_id = int(query)
        else:
            wo_no = query.upper() if query.upper().startswith("WO-") else query

        header_df = data_fetch.fetch_work_order_header(unit, wo_id=wo_id, wo_no=wo_no)
        if header_df is None:
            return jsonify({"status": "error", "message": "Failed to fetch Work Order"}), 500
        if header_df.empty:
            return jsonify({"status": "error", "message": "Work Order not found"}), 404

        header_df = _clean_df_columns(header_df)
        header_row = header_df.iloc[0].to_dict()
        wo_id_val = _safe_int(header_row.get("ID"))
        if wo_id_val > 0 and not str(header_row.get("PONo") or "").strip():
            try:
                wo_no_fix = data_fetch.ensure_work_order_number(unit, wo_id_val, preferred_wo_no=wo_no)
                ensured_wo_no = str(wo_no_fix.get("wo_no") or "").strip()
                if ensured_wo_no:
                    header_row["PONo"] = ensured_wo_no
            except Exception:
                pass
        status_code = str(header_row.get("Status") or "").strip().upper()
        header_payload = _build_work_order_header_payload(header_row, status_code=status_code)

        if status_code == "P":
            otp_rec = _fetch_latest_purchase_otp_request(wo_id_val)
            if otp_rec:
                header_payload["otp_request_id"] = otp_rec.get("request_id")

        items_df = data_fetch.fetch_work_order_items(unit, wo_id_val)
        if items_df is None:
            return jsonify({"status": "error", "message": "Failed to fetch Work Order items"}), 500

        items_payload = []
        if not items_df.empty:
            items_df = _clean_df_columns(items_df)
            for row in items_df.to_dict(orient="records"):
                cgst_pct = _safe_float(row.get("CGSTPct"))
                sgst_pct = _safe_float(row.get("SGSTPct"))
                igst_pct = _safe_float(row.get("IGSTPct"))
                cgst_amt = _safe_float(row.get("CGSTAmt"))
                sgst_amt = _safe_float(row.get("SGSTAmt"))
                igst_amt = _safe_float(row.get("IGSTAmt"))
                items_payload.append(
                    _sanitize_json_payload(
                        {
                            "detail_id": row.get("DetailID"),
                            "line_no": row.get("LineNo"),
                            "item_name": row.get("ItemName"),
                            "unit": row.get("UnitName"),
                            "qty": row.get("Qty"),
                            "rate": row.get("Rate"),
                            "discount_pct": row.get("Discount"),
                            "taxable_amt": row.get("TaxableAmount"),
                            "cgst_pct": cgst_pct,
                            "sgst_pct": sgst_pct,
                            "igst_pct": igst_pct,
                            "gst_pct": (cgst_pct + sgst_pct) if (cgst_pct or sgst_pct) else igst_pct,
                            "cgst_amt": cgst_amt,
                            "sgst_amt": sgst_amt,
                            "igst_amt": igst_amt,
                            "gst_amt": cgst_amt + sgst_amt + igst_amt,
                            "for_amt": row.get("ForAmount"),
                            "net_amt": row.get("NetAmount"),
                            "line_notes": row.get("LineNotes"),
                        }
                    )
                )

        header_payload["totals"] = _sanitize_json_payload(
            _fallback_work_order_summary_from_header(header_row, _summarize_work_order_totals(items_payload))
        )
        return jsonify({"status": "success", "header": header_payload, "items": items_payload, "unit": unit})


    @app.route('/api/purchase/work_order_list')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_work_order_list():
        unit, error = _get_work_order_unit()
        if error:
            return error
        _ensure_work_order_schema(unit)

        status = request.args.get("status") or "open"
        limit_raw = request.args.get("limit") or "200"
        query = request.args.get("q")
        item_query = request.args.get("item_q")
        try:
            limit = int(limit_raw)
        except Exception:
            limit = 200
        df = data_fetch.fetch_work_order_list(unit, status=status, limit=limit, query=query, item_query=item_query)
        if df is None:
            return jsonify({"status": "error", "message": "Failed to fetch Work Order list"}), 500
        if df.empty:
            return jsonify({"status": "success", "items": [], "unit": unit})
        df = _clean_df_columns(df)
        rows = _sanitize_json_payload(df.to_dict(orient="records"))
        return jsonify({"status": "success", "items": rows, "unit": unit})


    @app.route('/api/purchase/work_order', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_create_work_order():
        unit, error = _get_work_order_unit()
        if error:
            return error
        _ensure_work_order_schema(unit)

        payload = request.get_json(silent=True) or {}
        header = payload.get("header") or {}
        items = payload.get("items") or []
        body_html = _normalize_work_order_body_html(header.get("body_html"))
        body_text = _work_order_body_to_plain_text(body_html)

        supplier_id = _safe_int(header.get("supplier_id"))
        if supplier_id <= 0:
            _audit_log_event(
                "purchase",
                "work_order_create",
                status="error",
                entity_type="work_order",
                unit=unit,
                summary="Supplier is required",
                details={"wo_no": header.get("wo_no"), "item_count": len(items)},
            )
            return jsonify({"status": "error", "message": "Supplier is required"}), 400
        if not items and not body_text:
            _audit_log_event(
                "purchase",
                "work_order_create",
                status="error",
                entity_type="work_order",
                unit=unit,
                summary="Work Order body is required",
                details={"wo_no": header.get("wo_no")},
            )
            return jsonify({"status": "error", "message": "Enter the Work Order body before saving."}), 400

        normalized_items, item_errors = _normalize_work_order_items(items)
        if item_errors:
            _audit_log_event(
                "purchase",
                "work_order_create",
                status="error",
                entity_type="work_order",
                unit=unit,
                summary="Invalid Work Order lines",
                details={"errors": item_errors, "wo_no": header.get("wo_no")},
            )
            return jsonify({"status": "error", "message": "Invalid Work Order lines", "errors": item_errors}), 400

        totals = _resolve_work_order_totals(normalized_items, header.get("totals"))
        if not normalized_items:
            if totals.get("grand_total", 0) <= 0 and totals.get("gross_total", 0) <= 0 and totals.get("taxable_total", 0) <= 0:
                _audit_log_event(
                    "purchase",
                    "work_order_create",
                    status="error",
                    entity_type="work_order",
                    unit=unit,
                    summary="Work Order totals are required",
                    details={"wo_no": header.get("wo_no")},
                )
                return jsonify({"status": "error", "message": "Enter Work Order totals before saving."}), 400
            normalized_items = [_build_work_order_fallback_item(header, body_text, totals)]

        status_raw = str(header.get("status") or "Draft").strip().lower()
        status_map = {"draft": "D", "pending approval": "P", "approved": "A"}
        status_code = status_map.get(status_raw, "D")
        purchasing_dept_id = _safe_int(header.get("purchasing_dept_id"))
        if status_code == "P" and purchasing_dept_id <= 0:
            _audit_log_event(
                "purchase",
                "work_order_create",
                status="error",
                entity_type="work_order",
                unit=unit,
                summary="Purchasing department is required for approval",
                details={"wo_no": header.get("wo_no")},
            )
            return jsonify({"status": "error", "message": "Please select Purchasing Dept before submitting for approval."}), 400

        now = datetime.now(tz=LOCAL_TZ)

        def _num_or_text(val):
            try:
                if val is None or val == "":
                    return None
                return float(val)
            except Exception:
                return val

        wo_params = {
            "pId": _safe_int(header.get("wo_id")),
            "pSupplierid": supplier_id,
            "pTenderid": _safe_int(header.get("tender_id")),
            "pPono": header.get("wo_no"),
            "pPodate": header.get("wo_date"),
            "pDeliveryterms": header.get("delivery_terms"),
            "pPaymentsterms": header.get("payment_terms"),
            "pOtherterms": header.get("other_terms"),
            "pTaxid": _safe_int(header.get("tax_id")),
            "pTax": totals.get("tax_total") or 0,
            "pDiscount": totals.get("discount_total") or 0,
            "pAmount": totals.get("grand_total") or 0,
            "pCreditdays": _safe_int(header.get("credit_days")),
            "pPocomplete": 0,
            "pNotes": header.get("notes"),
            "pSpecialNotes": header.get("special_notes"),
            "WorkOrderBodyHtml": body_html,
            "SeniorApprovalAuthorityName": header.get("senior_approval_authority_name"),
            "SeniorApprovalAuthorityDesignation": header.get("senior_approval_authority_designation"),
            "pPreparedby": header.get("prepared_by"),
            "pCustom1": _num_or_text(header.get("freight_charges")),
            "pCustom2": _num_or_text(header.get("packing_charges")),
            "pUpdatedby": _safe_int(header.get("updated_by")),
            "pUpdatedon": header.get("updated_on"),
            "pSignauthorityperson": header.get("sign_authority_person"),
            "pSignauthoritypdesig": header.get("sign_authority_desig"),
            "pRefno": header.get("ref_no"),
            "pSubject": header.get("subject"),
            "pAuthorizationid": _safe_int(header.get("authorization_id")),
            "pPurchaseIndentId": 0,
            "pInsertedByUserID": session.get("username") or session.get("user"),
            "pInsertedON": now.strftime("%Y-%m-%d %H:%M:%S"),
            "pInsertedMacName": header.get("mac_name"),
            "pInsertedMacID": header.get("mac_id"),
            "pInsertedIPAddress": request.remote_addr,
            "Against": "Work Order",
            "QuotationId": _safe_int(header.get("quotation_id")),
            "TotalFORe": totals.get("for_total") or 0,
            "TotalExciseAmt": totals.get("tax_total") or 0,
            "AgainstId": 0,
            "Status": status_code,
            "PurchasingDeptId": purchasing_dept_id if purchasing_dept_id > 0 else None,
        }

        mst_result = data_fetch.add_iv_work_order_mst(unit, wo_params)
        if mst_result.get("error"):
            _audit_log_event(
                "purchase",
                "work_order_create",
                status="error",
                entity_type="work_order",
                unit=unit,
                summary="Work Order creation failed",
                details={"error": mst_result.get("error"), "wo_no": header.get("wo_no")},
            )
            return jsonify({"status": "error", "message": mst_result["error"]}), 500
        wo_id = mst_result.get("wo_id")
        if not wo_id:
            _audit_log_event(
                "purchase",
                "work_order_create",
                status="error",
                entity_type="work_order",
                unit=unit,
                summary="Work Order creation failed (missing ID)",
                details={"wo_no": header.get("wo_no")},
            )
            return jsonify({"status": "error", "message": "Failed to create Work Order"}), 500

        wo_no = str(mst_result.get("wo_no") or "").strip()
        if not wo_no:
            try:
                wo_no_fix = data_fetch.ensure_work_order_number(unit, int(wo_id), preferred_wo_no=header.get("wo_no"))
                ensured_wo_no = str(wo_no_fix.get("wo_no") or "").strip()
                if ensured_wo_no:
                    wo_no = ensured_wo_no
            except Exception:
                pass
        if not wo_no:
            wo_no = f"WO-{wo_id}"

        detail_params = []
        for item in normalized_items:
            detail_params.append(
                {
                    "WOID": int(wo_id),
                    "LineNo": item.get("line_no"),
                    "ItemName": item.get("item_name"),
                    "UnitName": item.get("unit_name"),
                    "Qty": item.get("qty"),
                    "Rate": item.get("rate"),
                    "DiscountPct": item.get("discount_pct"),
                    "TaxableAmount": item.get("taxable_amount"),
                    "CGSTPct": item.get("cgst_pct"),
                    "SGSTPct": item.get("sgst_pct"),
                    "IGSTPct": item.get("igst_pct"),
                    "CGSTAmt": item.get("cgst_amt"),
                    "SGSTAmt": item.get("sgst_amt"),
                    "IGSTAmt": item.get("igst_amt"),
                    "ForAmount": item.get("for_amount"),
                    "NetAmount": item.get("net_amount"),
                    "LineNotes": item.get("line_notes"),
                    "CreatedBy": session.get("username") or session.get("user"),
                    "UpdatedBy": session.get("username") or session.get("user"),
                    "_row_no": item.get("line_no"),
                }
            )
        bulk_result = data_fetch.add_work_order_dtl_many(unit, detail_params)
        if bulk_result.get("error"):
            _audit_log_event(
                "purchase",
                "work_order_create",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Failed to save Work Order detail rows",
                details={"wo_no": wo_no, "error": bulk_result.get("error")},
            )
            return jsonify({"status": "error", "message": bulk_result.get("error")}), 500
        detail_errors = list(bulk_result.get("errors") or [])
        if detail_errors:
            _audit_log_event(
                "purchase",
                "work_order_create",
                status="partial",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Work Order created but some rows failed",
                details={"wo_no": wo_no, "errors": detail_errors},
            )
            return jsonify({"status": "partial", "wo_id": wo_id, "wo_no": wo_no, "errors": detail_errors}), 207

        otp_request_id = None
        if status_code == "P":
            otp_result = _create_work_order_otp_request(
                unit=unit,
                wo_no=wo_no,
                wo_id=wo_id,
                amount=totals.get("grand_total") or 0,
                supplier_name=header.get("supplier_name") or "",
                reason=header.get("subject") or header.get("notes") or "",
                requested_by=session.get("username") or session.get("user") or "",
                purchasing_dept_id=purchasing_dept_id if purchasing_dept_id > 0 else None,
            )
            if otp_result.get("error"):
                _audit_log_event(
                    "purchase",
                    "work_order_create",
                    status="error",
                    entity_type="work_order",
                    entity_id=str(wo_id),
                    unit=unit,
                    summary="OTP request failed",
                    details={"error": otp_result.get("error"), "wo_no": wo_no},
                )
                return jsonify({"status": "error", "message": otp_result["error"]}), 500
            otp_request_id = otp_result.get("request_id")
            if otp_request_id:
                _insert_purchase_otp_request(wo_id, wo_no, unit, int(otp_request_id), session.get("username") or session.get("user"))

        response = {"status": "success", "wo_id": wo_id, "wo_no": wo_no}
        if otp_request_id:
            response["request_id"] = otp_request_id
        _audit_log_event(
            "purchase",
            "work_order_create",
            status="success",
            entity_type="work_order",
            entity_id=str(wo_id),
            unit=unit,
            summary="Work Order created",
            details={
                "wo_no": wo_no,
                "header": _normalize_work_order_header_for_audit(header, totals, status_code),
                "item_count": len(normalized_items),
                "otp_request_id": otp_request_id,
            },
            request_id=str(otp_request_id) if otp_request_id else None,
        )
        return jsonify(response)


    @app.route('/api/purchase/work_order_update', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_update_work_order():
        unit, error = _get_work_order_unit()
        if error:
            return error
        _ensure_work_order_schema(unit)

        payload = request.get_json(silent=True) or {}
        header = payload.get("header") or {}
        items = payload.get("items") or []
        body_html = _normalize_work_order_body_html(header.get("body_html"))
        body_text = _work_order_body_to_plain_text(body_html)

        wo_id = _safe_int(header.get("wo_id"))
        if wo_id <= 0:
            _audit_log_event(
                "purchase",
                "work_order_update",
                status="error",
                entity_type="work_order",
                unit=unit,
                summary="Work Order ID is required for update",
                details={"wo_no": header.get("wo_no")},
            )
            return jsonify({"status": "error", "message": "Work Order ID is required for update"}), 400
        if not items and not body_text:
            _audit_log_event(
                "purchase",
                "work_order_update",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Work Order body is required",
                details={"wo_no": header.get("wo_no")},
            )
            return jsonify({"status": "error", "message": "Enter the Work Order body before saving."}), 400

        normalized_items, item_errors = _normalize_work_order_items(items)
        if item_errors:
            _audit_log_event(
                "purchase",
                "work_order_update",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Invalid Work Order lines",
                details={"errors": item_errors, "wo_no": header.get("wo_no")},
            )
            return jsonify({"status": "error", "message": "Invalid Work Order lines", "errors": item_errors}), 400

        header_df = data_fetch.fetch_work_order_header(unit, wo_id=wo_id)
        if header_df is None:
            _audit_log_event(
                "purchase",
                "work_order_update",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Failed to fetch Work Order for update",
            )
            return jsonify({"status": "error", "message": "Failed to fetch Work Order for update"}), 500
        if header_df.empty:
            _audit_log_event(
                "purchase",
                "work_order_update",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Work Order not found",
            )
            return jsonify({"status": "error", "message": "Work Order not found"}), 404
        header_df = _clean_df_columns(header_df)
        existing_header = header_df.iloc[0].to_dict()
        current_status = str(existing_header.get("Status") or "").strip().upper()
        if current_status not in {"D", "P"}:
            _audit_log_event(
                "purchase",
                "work_order_update",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Only Draft or Pending Approval Work Orders can be updated",
                details={"current_status": current_status},
            )
            return jsonify({"status": "error", "message": "Only Draft or Pending Approval Work Orders can be updated"}), 400

        totals = _resolve_work_order_totals(normalized_items, header.get("totals"))
        if not normalized_items:
            if totals.get("grand_total", 0) <= 0 and totals.get("gross_total", 0) <= 0 and totals.get("taxable_total", 0) <= 0:
                _audit_log_event(
                    "purchase",
                    "work_order_update",
                    status="error",
                    entity_type="work_order",
                    entity_id=str(wo_id),
                    unit=unit,
                    summary="Work Order totals are required",
                    details={"wo_no": header.get("wo_no")},
                )
                return jsonify({"status": "error", "message": "Enter Work Order totals before saving."}), 400
            normalized_items = [_build_work_order_fallback_item(header, body_text, totals)]
        status_raw = str(header.get("status") or "Draft").strip().lower()
        status_map = {"draft": "D", "pending approval": "P", "approved": "A"}
        status_code = status_map.get(status_raw, "D")
        purchasing_dept_id = _safe_int(header.get("purchasing_dept_id"))
        if status_code == "P" and purchasing_dept_id <= 0:
            _audit_log_event(
                "purchase",
                "work_order_update",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Purchasing department is required for approval",
                details={"wo_no": header.get("wo_no")},
            )
            return jsonify({"status": "error", "message": "Please select Purchasing Dept before submitting for approval."}), 400

        now = datetime.now(tz=LOCAL_TZ)

        def _num_or_text(val):
            try:
                if val is None or val == "":
                    return None
                return float(val)
            except Exception:
                return val

        wo_params = {
            "pId": wo_id,
            "pSupplierid": _safe_int(header.get("supplier_id") or existing_header.get("SupplierID")),
            "pTenderid": _safe_int(header.get("tender_id")),
            "pPono": header.get("wo_no") or existing_header.get("PONo"),
            "pPodate": header.get("wo_date"),
            "pDeliveryterms": header.get("delivery_terms"),
            "pPaymentsterms": header.get("payment_terms"),
            "pOtherterms": header.get("other_terms"),
            "pTaxid": _safe_int(header.get("tax_id")),
            "pTax": totals.get("tax_total") or 0,
            "pDiscount": totals.get("discount_total") or 0,
            "pAmount": totals.get("grand_total") or 0,
            "pCreditdays": _safe_int(header.get("credit_days")),
            "pPocomplete": 0,
            "pNotes": header.get("notes"),
            "pSpecialNotes": header.get("special_notes"),
            "WorkOrderBodyHtml": body_html,
            "SeniorApprovalAuthorityName": header.get("senior_approval_authority_name"),
            "SeniorApprovalAuthorityDesignation": header.get("senior_approval_authority_designation"),
            "pPreparedby": header.get("prepared_by"),
            "pCustom1": _num_or_text(header.get("freight_charges")),
            "pCustom2": _num_or_text(header.get("packing_charges")),
            "pSignauthorityperson": header.get("sign_authority_person"),
            "pSignauthoritypdesig": header.get("sign_authority_desig"),
            "pRefno": header.get("ref_no"),
            "pSubject": header.get("subject"),
            "pAuthorizationid": _safe_int(header.get("authorization_id")),
            "pPurchaseIndentId": 0,
            "Against": "Work Order",
            "QuotationId": _safe_int(header.get("quotation_id")),
            "TotalFORe": totals.get("for_total") or 0,
            "TotalExciseAmt": totals.get("tax_total") or 0,
            "AgainstId": 0,
            "Status": status_code,
            "PurchasingDeptId": purchasing_dept_id if purchasing_dept_id > 0 else None,
            "pUpdatedby": _safe_int(header.get("updated_by")),
            "pUpdatedon": now.strftime("%Y-%m-%d %H:%M:%S"),
        }

        mst_result = data_fetch.update_iv_work_order_mst(unit, wo_params)
        if mst_result.get("error"):
            _audit_log_event(
                "purchase",
                "work_order_update",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Work Order update failed",
                details={"error": mst_result.get("error"), "wo_no": header.get("wo_no")},
            )
            return jsonify({"status": "error", "message": mst_result["error"]}), 500

        wo_no = str(mst_result.get("wo_no") or header.get("wo_no") or existing_header.get("PONo") or "").strip()
        if not wo_no:
            try:
                wo_no_fix = data_fetch.ensure_work_order_number(unit, int(wo_id), preferred_wo_no=header.get("wo_no"))
                ensured_wo_no = str(wo_no_fix.get("wo_no") or "").strip()
                if ensured_wo_no:
                    wo_no = ensured_wo_no
            except Exception:
                pass
        if not wo_no:
            wo_no = f"WO-{wo_id}"

        clear_result = data_fetch.clear_work_order_dtl(unit, wo_id)
        if clear_result.get("error"):
            _audit_log_event(
                "purchase",
                "work_order_update",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Failed to clear Work Order lines before update",
                details={"error": clear_result.get("error")},
            )
            return jsonify({"status": "error", "message": clear_result["error"]}), 500

        detail_params = []
        for item in normalized_items:
            detail_params.append(
                {
                    "WOID": int(wo_id),
                    "LineNo": item.get("line_no"),
                    "ItemName": item.get("item_name"),
                    "UnitName": item.get("unit_name"),
                    "Qty": item.get("qty"),
                    "Rate": item.get("rate"),
                    "DiscountPct": item.get("discount_pct"),
                    "TaxableAmount": item.get("taxable_amount"),
                    "CGSTPct": item.get("cgst_pct"),
                    "SGSTPct": item.get("sgst_pct"),
                    "IGSTPct": item.get("igst_pct"),
                    "CGSTAmt": item.get("cgst_amt"),
                    "SGSTAmt": item.get("sgst_amt"),
                    "IGSTAmt": item.get("igst_amt"),
                    "ForAmount": item.get("for_amount"),
                    "NetAmount": item.get("net_amount"),
                    "LineNotes": item.get("line_notes"),
                    "CreatedBy": session.get("username") or session.get("user"),
                    "UpdatedBy": session.get("username") or session.get("user"),
                    "_row_no": item.get("line_no"),
                }
            )
        bulk_result = data_fetch.add_work_order_dtl_many(unit, detail_params)
        if bulk_result.get("error"):
            _audit_log_event(
                "purchase",
                "work_order_update",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Failed to save Work Order detail rows",
                details={"wo_no": wo_no, "error": bulk_result.get("error")},
            )
            return jsonify({"status": "error", "message": bulk_result.get("error")}), 500
        detail_errors = list(bulk_result.get("errors") or [])
        if detail_errors:
            _audit_log_event(
                "purchase",
                "work_order_update",
                status="partial",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Work Order updated but some rows failed",
                details={"wo_no": wo_no, "errors": detail_errors},
            )
            return jsonify({"status": "partial", "wo_id": wo_id, "wo_no": wo_no, "errors": detail_errors}), 207

        otp_request_id = None
        if status_code == "P":
            otp_result = _create_work_order_otp_request(
                unit=unit,
                wo_no=wo_no,
                wo_id=wo_id,
                amount=totals.get("grand_total") or 0,
                supplier_name=header.get("supplier_name") or existing_header.get("SupplierName") or "",
                reason=header.get("subject") or header.get("notes") or "",
                requested_by=session.get("username") or session.get("user") or "",
                purchasing_dept_id=purchasing_dept_id if purchasing_dept_id > 0 else None,
            )
            if otp_result.get("error"):
                _audit_log_event(
                    "purchase",
                    "work_order_update",
                    status="error",
                    entity_type="work_order",
                    entity_id=str(wo_id),
                    unit=unit,
                    summary="OTP request failed",
                    details={"error": otp_result.get("error"), "wo_no": wo_no},
                )
                return jsonify({"status": "error", "message": otp_result["error"]}), 500
            otp_request_id = otp_result.get("request_id")
            if otp_request_id:
                _insert_purchase_otp_request(wo_id, wo_no, unit, int(otp_request_id), session.get("username") or session.get("user"))

        response = {"status": "success", "wo_id": wo_id, "wo_no": wo_no}
        if otp_request_id:
            response["request_id"] = otp_request_id
        _audit_log_event(
            "purchase",
            "work_order_update",
            status="success",
            entity_type="work_order",
            entity_id=str(wo_id),
            unit=unit,
            summary="Work Order updated",
            details={
                "wo_no": wo_no,
                "old_header": _normalize_work_order_header_for_audit(existing_header),
                "new_header": _normalize_work_order_header_for_audit(header, totals, status_code),
                "item_count": len(normalized_items),
                "otp_request_id": otp_request_id,
            },
            request_id=str(otp_request_id) if otp_request_id else None,
        )
        return jsonify(response)


    @app.route('/api/purchase/work_order_approve', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_work_order_approve():
        unit, error = _get_work_order_unit()
        if error:
            return error
        _ensure_work_order_schema(unit)

        payload = request.get_json(silent=True) or {}
        wo_id = _safe_int(payload.get("wo_id"))
        request_id = _safe_int(payload.get("request_id"))
        otp = str(payload.get("otp") or "").strip()
        auto_email_requested = _is_truthy(payload.get("auto_email_supplier_pdf"))
        supplier_email_override = _purchase_normalize_email(payload.get("supplier_email") or payload.get("to_email") or payload.get("email"))
        raw_auto_email_format = str(payload.get("auto_email_format") or payload.get("format") or "").strip().lower()
        is_it = (session.get("role") or "").strip() == "IT"
        auto_email_print_format = "def" if is_it and raw_auto_email_format == "def" else None

        if wo_id <= 0 or request_id <= 0 or not otp:
            _audit_log_event(
                "purchase",
                "work_order_approve",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id) if wo_id > 0 else None,
                unit=unit,
                summary="Work Order ID, request ID, and OTP are required",
            )
            return jsonify({"status": "error", "message": "Work Order ID, request ID, and OTP are required"}), 400

        header_df = data_fetch.fetch_work_order_header(unit, wo_id=wo_id)
        if header_df is None:
            _audit_log_event(
                "purchase",
                "work_order_approve",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Failed to fetch Work Order",
            )
            return jsonify({"status": "error", "message": "Failed to fetch Work Order"}), 500
        if header_df.empty:
            _audit_log_event(
                "purchase",
                "work_order_approve",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Work Order not found",
            )
            return jsonify({"status": "error", "message": "Work Order not found"}), 404
        header_df = _clean_df_columns(header_df)
        header_row = header_df.iloc[0].to_dict()
        auto_email_print_format = _resolve_po_print_format_for_render(
            unit,
            requested_format=raw_auto_email_format,
            header_row=header_row,
            purchasing_dept_id=_safe_int(header_row.get("PurchasingDeptId")),
        )
        status_code = str(header_row.get("Status") or "").strip().upper()
        if status_code != "P":
            _audit_log_event(
                "purchase",
                "work_order_approve",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Only Pending Approval Work Orders can be approved",
                details={"current_status": status_code},
            )
            return jsonify({"status": "error", "message": "Only Pending Approval Work Orders can be approved"}), 400

        otp_result = _validate_purchase_otp(request_id, otp)
        if otp_result.get("error"):
            _audit_log_event(
                "purchase",
                "work_order_approve",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="OTP validation failed",
                details={"error": otp_result.get("error")},
                request_id=str(request_id),
            )
            return jsonify({"status": "error", "message": otp_result["error"]}), 500
        if not otp_result.get("valid"):
            _audit_log_event(
                "purchase",
                "work_order_approve",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Invalid OTP",
                details={"message": otp_result.get("message")},
                request_id=str(request_id),
            )
            return jsonify({"status": "error", "message": otp_result.get("message") or "Invalid OTP"}), 400

        req_type = str(otp_result.get("request_type") or "").strip().upper()
        if req_type and req_type != OTP_WORK_ORDER_REQUEST_TYPE:
            _audit_log_event(
                "purchase",
                "work_order_approve",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="OTP request type mismatch",
                details={"request_type": req_type},
                request_id=str(request_id),
            )
            return jsonify({"status": "error", "message": "OTP request type mismatch"}), 400

        otp_wo_no = str(otp_result.get("bill_no") or "").strip().upper()
        if wo_id > 0 and not str(header_row.get("PONo") or "").strip():
            try:
                wo_no_fix = data_fetch.ensure_work_order_number(unit, wo_id, preferred_wo_no=otp_wo_no or None)
                ensured_wo_no = str(wo_no_fix.get("wo_no") or "").strip()
                if ensured_wo_no:
                    header_row["PONo"] = ensured_wo_no
            except Exception:
                pass
        wo_no = str(header_row.get("PONo") or "").strip().upper()
        if wo_no and otp_wo_no and wo_no != otp_wo_no:
            _audit_log_event(
                "purchase",
                "work_order_approve",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="OTP does not match this Work Order",
                details={"wo_no": wo_no, "otp_wo_no": otp_wo_no},
                request_id=str(request_id),
            )
            return jsonify({"status": "error", "message": "OTP does not match this Work Order"}), 400

        status_result = data_fetch.update_work_order_status(unit, wo_id, "A")
        if status_result.get("error"):
            _audit_log_event(
                "purchase",
                "work_order_approve",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Work Order approval failed",
                details={"error": status_result.get("error")},
                request_id=str(request_id),
            )
            return jsonify({"status": "error", "message": status_result["error"]}), 500

        user = session.get("username") or session.get("user") or ""
        _mark_central_otp_used(request_id, user)
        _mark_purchase_otp_used(wo_id, request_id, user)
        try:
            req_meta = _fetch_purchase_otp_request_by_id(request_id)
            mail_result = _send_purchase_work_order_approval_email(unit, wo_id, header_row, req_meta)
            if mail_result.get("status") == "error":
                print(f"Work Order approval email failed: {mail_result.get('message')}")
        except Exception as e:
            print(f"Work Order approval email error: {e}")

        auto_email_result = {"requested": bool(auto_email_requested), "status": "skipped", "message": "Auto email option not selected"}
        if auto_email_requested:
            wo_no_val = str(header_row.get("PONo") or f"WO-{wo_id}")
            supplier_id = _safe_int(header_row.get("SupplierID"))
            supplier_email = _purchase_normalize_email(header_row.get("SupplierEmail"))
            if _purchase_is_valid_email(supplier_email_override):
                supplier_email = supplier_email_override
                header_row["SupplierEmail"] = supplier_email
            if not _purchase_is_valid_email(supplier_email):
                auto_email_result = {
                    "requested": True,
                    "status": "skipped",
                    "message": "Supplier email is missing or invalid",
                    "recipient": supplier_email,
                }
                _audit_log_event(
                    "purchase",
                    "work_order_auto_email_on_approve",
                    status="error",
                    entity_type="work_order",
                    entity_id=str(wo_id),
                    unit=unit,
                    summary="Supplier auto-email skipped due to invalid email",
                    details={"wo_no": wo_no_val, "supplier_id": supplier_id, "recipient": supplier_email},
                    request_id=str(request_id),
                )
            else:
                try:
                    items_df = data_fetch.fetch_work_order_items(unit, wo_id)
                    if items_df is None:
                        raise RuntimeError("Failed to fetch Work Order items for supplier email")
                    items_df = _clean_df_columns(items_df)
                    items_rows = items_df.to_dict(orient="records") if not items_df.empty else []

                    approval_meta = _fetch_purchase_approval_meta(wo_id)
                    header_pdf = dict(header_row)
                    header_pdf["Status"] = "A"
                    header_pdf["SupplierEmail"] = supplier_email
                    header_pdf["PurchasingDeptName"] = _resolve_purchasing_department_name(_safe_int(header_row.get("PurchasingDeptId")))
                    pdf_buffer = _build_work_order_pdf_buffer(
                        unit,
                        header_pdf,
                        items_rows,
                        approval_meta,
                        print_format=auto_email_print_format,
                    )
                    pdf_bytes = pdf_buffer.getvalue() if pdf_buffer else b""

                    snapshot = {
                        "unit": unit,
                        "wo_no": wo_no_val,
                        "wo_date": header_row.get("PODate").strftime("%d-%b-%Y")
                        if isinstance(header_row.get("PODate"), (datetime, date))
                        else str(header_row.get("PODate") or "")[:10],
                        "supplier": str(header_row.get("SupplierName") or ""),
                        "amount": _format_indian_currency(header_row.get("Amount") or 0),
                        "subject": str(header_row.get("Subject") or ""),
                    }
                    mail_subject = f"Work Order {wo_no_val}"
                    mail_body = _build_work_order_supplier_dispatch_email_body(snapshot)
                    mail_filename = f"WO_{wo_no_val}.pdf"
                    mail_result_supplier = _send_graph_mail_with_attachment(
                        subject=mail_subject,
                        body_html=mail_body,
                        to_recipients=[supplier_email],
                        filename=mail_filename,
                        content_bytes=pdf_bytes,
                    )
                    if str(mail_result_supplier.get("status") or "").strip().lower() == "success":
                        auto_email_result = {"requested": True, "status": "success", "recipient": supplier_email}
                        _audit_log_event(
                            "purchase",
                            "work_order_auto_email_on_approve",
                            status="success",
                            entity_type="work_order",
                            entity_id=str(wo_id),
                            unit=unit,
                            summary="Supplier Work Order PDF auto-emailed on approval",
                            details={"wo_no": wo_no_val, "supplier_id": supplier_id, "recipient": supplier_email},
                            request_id=str(request_id),
                        )
                    else:
                        err_msg = str(mail_result_supplier.get("message") or "Failed to send supplier email")
                        auto_email_result = {
                            "requested": True,
                            "status": "error",
                            "recipient": supplier_email,
                            "message": err_msg,
                        }
                        _audit_log_event(
                            "purchase",
                            "work_order_auto_email_on_approve",
                            status="error",
                            entity_type="work_order",
                            entity_id=str(wo_id),
                            unit=unit,
                            summary="Supplier Work Order PDF auto-email failed",
                            details={
                                "wo_no": wo_no_val,
                                "supplier_id": supplier_id,
                                "recipient": supplier_email,
                                "mail_result": mail_result_supplier,
                            },
                            request_id=str(request_id),
                        )
                except Exception as e:
                    auto_email_result = {
                        "requested": True,
                        "status": "error",
                        "recipient": supplier_email,
                        "message": str(e),
                    }
                    _audit_log_event(
                        "purchase",
                        "work_order_auto_email_on_approve",
                        status="error",
                        entity_type="work_order",
                        entity_id=str(wo_id),
                        unit=unit,
                        summary="Supplier Work Order PDF auto-email error",
                        details={"wo_no": wo_no_val, "supplier_id": supplier_id, "recipient": supplier_email, "error": str(e)},
                        request_id=str(request_id),
                    )

        _audit_log_event(
            "purchase",
            "work_order_approve",
            status="success",
            entity_type="work_order",
            entity_id=str(wo_id),
            unit=unit,
            summary="Work Order approved",
            details={"wo_no": header_row.get("PONo"), "auto_email": auto_email_result if auto_email_result.get("requested") else None},
            request_id=str(request_id),
        )
        return jsonify({"status": "success", "wo_id": wo_id, "wo_no": header_row.get("PONo"), "auto_email": auto_email_result})


    @app.route('/api/purchase/work_order_resend_otp', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_work_order_resend_otp():
        unit, error = _get_work_order_unit()
        if error:
            return error
        _ensure_work_order_schema(unit)

        payload = request.get_json(silent=True) or {}
        wo_id = _safe_int(payload.get("wo_id"))
        if wo_id <= 0:
            _audit_log_event(
                "purchase",
                "work_order_resend_otp",
                status="error",
                entity_type="work_order",
                unit=unit,
                summary="Work Order ID is required",
            )
            return jsonify({"status": "error", "message": "Work Order ID is required"}), 400

        header_df = data_fetch.fetch_work_order_header(unit, wo_id=wo_id)
        if header_df is None:
            _audit_log_event(
                "purchase",
                "work_order_resend_otp",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Failed to fetch Work Order",
            )
            return jsonify({"status": "error", "message": "Failed to fetch Work Order"}), 500
        if header_df.empty:
            _audit_log_event(
                "purchase",
                "work_order_resend_otp",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="Work Order not found",
            )
            return jsonify({"status": "error", "message": "Work Order not found"}), 404
        header_df = _clean_df_columns(header_df)
        header_row = header_df.iloc[0].to_dict()
        status_code = str(header_row.get("Status") or "").strip().upper()
        if status_code != "P":
            _audit_log_event(
                "purchase",
                "work_order_resend_otp",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="OTP can be resent only for Pending Approval Work Orders",
                details={"current_status": status_code},
            )
            return jsonify({"status": "error", "message": "OTP can be resent only for Pending Approval Work Orders"}), 400

        wo_no = str(header_row.get("PONo") or f"WO-{wo_id}")
        amount_total = header_row.get("Amount") or 0
        purchasing_dept_id = _safe_int(header_row.get("PurchasingDeptId"))
        otp_result = _create_work_order_otp_request(
            unit=unit,
            wo_no=wo_no,
            wo_id=wo_id,
            amount=amount_total,
            supplier_name=header_row.get("SupplierName") or "",
            reason=header_row.get("Subject") or header_row.get("Notes") or "",
            requested_by=session.get("username") or session.get("user") or "",
            purchasing_dept_id=purchasing_dept_id if purchasing_dept_id > 0 else None,
        )
        if otp_result.get("error"):
            _audit_log_event(
                "purchase",
                "work_order_resend_otp",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id),
                unit=unit,
                summary="OTP request failed",
                details={"error": otp_result.get("error"), "wo_no": wo_no},
            )
            return jsonify({"status": "error", "message": otp_result["error"]}), 500
        otp_request_id = otp_result.get("request_id")
        if otp_request_id:
            _insert_purchase_otp_request(wo_id, wo_no, unit, int(otp_request_id), session.get("username") or session.get("user"))

        _audit_log_event(
            "purchase",
            "work_order_resend_otp",
            status="success",
            entity_type="work_order",
            entity_id=str(wo_id),
            unit=unit,
            summary="OTP resent",
            details={"wo_no": wo_no, "request_id": otp_request_id},
            request_id=str(otp_request_id) if otp_request_id else None,
        )
        return jsonify({"status": "success", "wo_id": wo_id, "wo_no": wo_no, "request_id": otp_request_id})


    @app.route('/api/purchase/work_order_print')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_work_order_print():
        unit, error = _get_work_order_unit()
        if error:
            return error
        _ensure_work_order_schema(unit)

        wo_id_raw = request.args.get("wo_id")
        wo_no = (request.args.get("wo_no") or "").strip()
        wo_id = int(wo_id_raw) if str(wo_id_raw or "").isdigit() else None
        raw_format = (request.args.get("format") or "").strip().lower()
        is_it = (session.get("role") or "").strip() == "IT"
        print_format = "def" if is_it and raw_format == "def" else None
        if not wo_id and not wo_no:
            return jsonify({"status": "error", "message": "Work Order ID or number is required"}), 400

        header_df = data_fetch.fetch_work_order_header(unit, wo_id=wo_id, wo_no=wo_no)
        if header_df is None:
            return jsonify({"status": "error", "message": "Failed to fetch Work Order"}), 500
        if header_df.empty:
            return jsonify({"status": "error", "message": "Work Order not found"}), 404
        header_df = _clean_df_columns(header_df)
        header = header_df.iloc[0].to_dict()
        header["PurchasingDeptName"] = _resolve_purchasing_department_name(_safe_int(header.get("PurchasingDeptId")))
        wo_id_val = _safe_int(header.get("ID"), wo_id)
        if wo_id_val > 0 and not str(header.get("PONo") or "").strip():
            try:
                wo_no_fix = data_fetch.ensure_work_order_number(unit, wo_id_val, preferred_wo_no=wo_no)
                ensured_wo_no = str(wo_no_fix.get("wo_no") or "").strip()
                if ensured_wo_no:
                    header["PONo"] = ensured_wo_no
            except Exception:
                pass

        items_df = data_fetch.fetch_work_order_items(unit, wo_id_val)
        if items_df is None:
            return jsonify({"status": "error", "message": "Failed to fetch Work Order items"}), 500
        items_df = _clean_df_columns(items_df)
        items_rows = items_df.to_dict(orient="records") if not items_df.empty else []
        approval_meta = _fetch_purchase_approval_meta(wo_id_val)

        pdf_buffer = _build_work_order_pdf_buffer(unit, header, items_rows, approval_meta, print_format=print_format)
        filename = f"WO_{header.get('PONo') or wo_no or wo_id_val}.pdf"
        return send_file(pdf_buffer, mimetype="application/pdf", as_attachment=False, download_name=filename)


    @app.route('/api/purchase/work_order_email_pdf', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_purchase_work_order_email_pdf():
        unit, error = _get_work_order_unit()
        if error:
            return error
        _ensure_work_order_schema(unit)

        payload = request.get_json(silent=True) or {}
        wo_id = _safe_int(payload.get("wo_id"))
        wo_no = str(payload.get("wo_no") or "").strip()
        to_email = _purchase_normalize_email(payload.get("to_email") or payload.get("email"))
        raw_format = str(payload.get("format") or "").strip().lower()
        is_it = (session.get("role") or "").strip() == "IT"
        print_format = "def" if is_it and raw_format == "def" else None

        if wo_id <= 0 and not wo_no:
            _audit_log_event(
                "purchase",
                "work_order_email_pdf",
                status="error",
                entity_type="work_order",
                unit=unit,
                summary="Work Order ID or number is required",
            )
            return jsonify({"status": "error", "message": "Work Order ID or number is required"}), 400
        if not _purchase_is_valid_email(to_email):
            _audit_log_event(
                "purchase",
                "work_order_email_pdf",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id) if wo_id > 0 else None,
                unit=unit,
                summary="Invalid recipient email",
                details={"to_email": to_email, "wo_no": wo_no},
            )
            return jsonify({"status": "error", "message": "Enter a valid recipient email address"}), 400

        header_df = data_fetch.fetch_work_order_header(unit, wo_id=wo_id if wo_id > 0 else None, wo_no=wo_no)
        if header_df is None:
            _audit_log_event(
                "purchase",
                "work_order_email_pdf",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id) if wo_id > 0 else None,
                unit=unit,
                summary="Failed to fetch Work Order",
                details={"wo_no": wo_no},
            )
            return jsonify({"status": "error", "message": "Failed to fetch Work Order"}), 500
        if header_df.empty:
            _audit_log_event(
                "purchase",
                "work_order_email_pdf",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id) if wo_id > 0 else None,
                unit=unit,
                summary="Work Order not found",
                details={"wo_no": wo_no},
            )
            return jsonify({"status": "error", "message": "Work Order not found"}), 404

        header_df = _clean_df_columns(header_df)
        header_row = header_df.iloc[0].to_dict()
        wo_id_val = _safe_int(header_row.get("ID"), wo_id)
        if wo_id_val > 0 and not str(header_row.get("PONo") or "").strip():
            try:
                wo_no_fix = data_fetch.ensure_work_order_number(unit, wo_id_val, preferred_wo_no=wo_no)
                ensured_wo_no = str(wo_no_fix.get("wo_no") or "").strip()
                if ensured_wo_no:
                    header_row["PONo"] = ensured_wo_no
            except Exception:
                pass
        wo_no_val = str(header_row.get("PONo") or wo_no or f"WO-{wo_id_val}")
        status_code = str(header_row.get("Status") or "").strip().upper()
        if status_code != "A":
            _audit_log_event(
                "purchase",
                "work_order_email_pdf",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id_val),
                unit=unit,
                summary="Only approved Work Orders can be emailed",
                details={"wo_no": wo_no_val, "status": status_code},
            )
            return jsonify({"status": "error", "message": "Only approved Work Orders can be emailed"}), 400

        supplier_id = _safe_int(header_row.get("SupplierID"))
        if supplier_id <= 0:
            _audit_log_event(
                "purchase",
                "work_order_email_pdf",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id_val),
                unit=unit,
                summary="Supplier not linked to Work Order",
                details={"wo_no": wo_no_val},
            )
            return jsonify({"status": "error", "message": "Supplier not linked to Work Order"}), 400

        email_update_result = data_fetch.update_iv_supplier_email(unit, supplier_id, to_email)
        if email_update_result.get("error"):
            err_msg = str(email_update_result.get("error") or "Failed to update supplier email")
            http_status = 404 if "not found" in err_msg.lower() else 500
            _audit_log_event(
                "purchase",
                "work_order_email_pdf",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id_val),
                unit=unit,
                summary="Failed to update supplier email before dispatch",
                details={"wo_no": wo_no_val, "supplier_id": supplier_id, "to_email": to_email, "error": err_msg},
            )
            return jsonify({"status": "error", "message": err_msg}), http_status

        items_df = data_fetch.fetch_work_order_items(unit, wo_id_val)
        if items_df is None:
            _audit_log_event(
                "purchase",
                "work_order_email_pdf",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id_val),
                unit=unit,
                summary="Failed to fetch Work Order items",
                details={"wo_no": wo_no_val},
            )
            return jsonify({"status": "error", "message": "Failed to fetch Work Order items"}), 500
        items_df = _clean_df_columns(items_df)
        items_rows = items_df.to_dict(orient="records") if not items_df.empty else []

        approval_meta = _fetch_purchase_approval_meta(wo_id_val)
        header_pdf = dict(header_row)
        header_pdf["PurchasingDeptName"] = _resolve_purchasing_department_name(_safe_int(header_row.get("PurchasingDeptId")))
        header_pdf["SupplierEmail"] = to_email
        pdf_buffer = _build_work_order_pdf_buffer(unit, header_pdf, items_rows, approval_meta, print_format=print_format)
        pdf_bytes = pdf_buffer.getvalue() if pdf_buffer else b""

        snapshot = {
            "unit": unit,
            "wo_no": wo_no_val,
            "wo_date": header_row.get("PODate").strftime("%d-%b-%Y")
            if isinstance(header_row.get("PODate"), (datetime, date))
            else str(header_row.get("PODate") or "")[:10],
            "supplier": str(header_row.get("SupplierName") or ""),
            "amount": _format_indian_currency(header_row.get("Amount") or 0),
            "subject": str(header_row.get("Subject") or ""),
        }
        mail_subject = f"Work Order {wo_no_val}"
        mail_body = _build_work_order_supplier_dispatch_email_body(snapshot)
        mail_filename = f"WO_{wo_no_val}.pdf"
        mail_result = _send_graph_mail_with_attachment(
            subject=mail_subject,
            body_html=mail_body,
            to_recipients=[to_email],
            filename=mail_filename,
            content_bytes=pdf_bytes,
        )
        if str(mail_result.get("status") or "").strip().lower() != "success":
            err_msg = str(mail_result.get("message") or "Failed to send email")
            _audit_log_event(
                "purchase",
                "work_order_email_pdf",
                status="error",
                entity_type="work_order",
                entity_id=str(wo_id_val),
                unit=unit,
                summary="Work Order email send failed",
                details={"wo_no": wo_no_val, "supplier_id": supplier_id, "recipient": to_email, "mail_result": mail_result},
            )
            return jsonify({"status": "error", "message": err_msg, "mail_status": mail_result}), 500

        _audit_log_event(
            "purchase",
            "work_order_email_pdf",
            status="success",
            entity_type="work_order",
            entity_id=str(wo_id_val),
            unit=unit,
            summary="Work Order PDF emailed to supplier",
            details={"wo_no": wo_no_val, "supplier_id": supplier_id, "recipient": to_email, "mail_result": mail_result},
        )
        return jsonify(
            {
                "status": "success",
                "message": f"Work Order PDF emailed to {to_email}",
                "wo_id": wo_id_val,
                "wo_no": wo_no_val,
                "recipient": to_email,
                "mail_status": mail_result,
            }
        )


