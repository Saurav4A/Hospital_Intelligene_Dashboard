from flask import jsonify, render_template, request, session
from datetime import date, datetime
from decimal import Decimal
import json
import re

from modules import data_fetch


def register_service_addition_routes(app, *, login_required, allowed_units_for_session):
    """Register service-addition related routes without changing existing URLs."""
    _allowed_units_for_session = allowed_units_for_session
    @app.route('/service-addition')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def service_addition():
        allowed_units = _allowed_units_for_session()
        role = (session.get("role") or "").strip()
        account_id = session.get("accountid") or 0
        if not account_id and role.lower() == "it":
            account_id = 1
        return render_template(
            'service_addition.html',
            allowed_units=allowed_units,
            role=role,
            account_id=account_id,
        )


    def _get_service_addition_unit():
        unit = (request.args.get("unit") or request.form.get("unit") or "").strip().upper()
        if request.is_json:
            data = request.get_json(silent=True) or {}
            unit = unit or str(data.get("unit") or "").strip().upper()
        allowed_units = _allowed_units_for_session()
        if not allowed_units:
            return None, (jsonify({"status": "error", "message": "No unit access assigned"}), 403)
        if not unit:
            if len(allowed_units) == 1:
                unit = allowed_units[0]
            else:
                return None, (jsonify({"status": "error", "message": "Please select a unit"}), 400)
        if unit not in allowed_units:
            return None, (jsonify({"status": "error", "message": f"Unit {unit} not permitted"}), 403)
        return unit, None


    def _service_addition_bool(raw) -> bool:
        if isinstance(raw, bool):
            return raw
        if raw is None:
            return False
        return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


    def _service_addition_int(raw, default=None):
        try:
            if raw is None or str(raw).strip() == "":
                return default
            return int(float(raw))
        except Exception:
            return default


    def _service_addition_decimal(raw, default=Decimal("0")):
        try:
            if raw is None or str(raw).strip() == "":
                return Decimal(str(default))
            return Decimal(str(raw))
        except Exception:
            return Decimal(str(default))


    def _service_addition_billing_classes(raw):
        if raw is None:
            return []
        items = []
        if isinstance(raw, (list, tuple, set)):
            items = list(raw)
        elif isinstance(raw, str):
            txt = raw.strip()
            if not txt:
                return []
            try:
                parsed = json.loads(txt)
                if isinstance(parsed, (list, tuple, set)):
                    items = list(parsed)
                else:
                    items = [parsed]
            except Exception:
                items = [part.strip() for part in txt.split(",") if part.strip()]
        else:
            items = [raw]

        result = []
        for item in items:
            val = _service_addition_int(item)
            if val in (1, 2) and val not in result:
                result.append(val)
        return result


    def _service_addition_id_list(raw):
        if raw is None:
            return []
        items = []
        if isinstance(raw, (list, tuple, set)):
            items = list(raw)
        elif isinstance(raw, str):
            txt = raw.strip()
            if not txt:
                return []
            try:
                parsed = json.loads(txt)
                if isinstance(parsed, (list, tuple, set)):
                    items = list(parsed)
                else:
                    items = [parsed]
            except Exception:
                items = [part.strip() for part in txt.split(",") if part.strip()]
        else:
            items = [raw]

        result = []
        for item in items:
            val = _service_addition_int(item)
            if val and val > 0 and val not in result:
                result.append(val)
        return result


    def _service_addition_tariff_rows(raw):
        if raw is None:
            return []
        items = []
        if isinstance(raw, (list, tuple, set)):
            items = list(raw)
        elif isinstance(raw, str):
            txt = raw.strip()
            if not txt:
                return []
            try:
                parsed = json.loads(txt)
                if isinstance(parsed, (list, tuple, set)):
                    items = list(parsed)
                else:
                    items = [parsed]
            except Exception:
                return []
        elif isinstance(raw, dict):
            items = [raw]
        else:
            return []

        rows = []
        for item in items:
            if not isinstance(item, dict):
                continue
            scheme_id = _service_addition_int(
                item.get("tariff_scheme_id")
                or item.get("TariffScheme_ID")
                or item.get("tariffscheme_id")
            )
            billing_id = _service_addition_int(
                item.get("billing_class_id")
                or item.get("BillingClassID")
                or item.get("billingclass_id")
            )
            if not scheme_id or not billing_id:
                continue
            rate = _service_addition_decimal(
                item.get("rate") or item.get("tariff_rate"), Decimal("0")
            )
            discount = _service_addition_decimal(
                item.get("discount_amt")
                or item.get("discount")
                or item.get("tariff_discount"),
                Decimal("0"),
            )
            rows.append({
                "tariff_scheme_id": scheme_id,
                "billing_class_id": billing_id,
                "rate": rate,
                "discount_amt": discount,
            })
        return rows


    def _service_addition_rows(cursor):
        cols = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
        out = []
        for row in rows:
            rec = {}
            for idx, col in enumerate(cols):
                val = row[idx]
                if isinstance(val, (datetime, date)):
                    val = val.isoformat(sep=" ")
                if isinstance(val, Decimal):
                    val = float(val)
                if isinstance(val, memoryview):
                    val = val.tobytes().decode(errors="ignore")
                if isinstance(val, bytes):
                    val = val.decode(errors="ignore")
                rec[col] = val
            out.append(rec)
        return out


    def _service_addition_next_code(last_code: str | None, fallback_prefix: str | None = None) -> str:
        code = (last_code or "").strip()
        prefix = ""
        digits = ""
        if code:
            match = re.search(r"(.*?)(\d+)$", code)
            if match:
                prefix = match.group(1)
                digits = match.group(2)
            else:
                prefix = code
        if not digits:
            fallback = (fallback_prefix or "").strip()
            if fallback and not fallback.endswith("-"):
                fallback = fallback + "-"
            return (fallback or prefix) + "0001"
        next_num = str(int(digits) + 1).zfill(len(digits))
        return f"{prefix}{next_num}"


    @app.route('/api/service_addition/init')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_service_addition_init():
        from modules.db_connection import get_sql_connection

        unit, err = _get_service_addition_unit()
        if err:
            return err
        conn = get_sql_connection(unit)
        if not conn:
            return jsonify({"status": "error", "message": "Unable to connect to database"}), 500
        try:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT Department_ID, Department_Code, Department_Name, Deactive
                FROM dbo.Department_Mst
                ORDER BY Department_Name
            """)
            departments = _service_addition_rows(cursor)

            cursor.execute("""
                SELECT SubDepartment_ID, SubDepartment_Code, SubDepartment_Name, Department_Id, Deactive
                FROM dbo.SubDepartment_Mst
                ORDER BY SubDepartment_Name
            """)
            subdepartments = _service_addition_rows(cursor)

            cursor.execute("""
                SELECT ServiceCategory_ID, ServiceCategory_Code, ServiceCategory_Name, Deactive, Surcharge
                FROM dbo.Service_Category_Mst
                ORDER BY ServiceCategory_Name
            """)
            categories = _service_addition_rows(cursor)

            cursor.execute("""
                SELECT TariffScheme_ID, TariffScheme_Code, TariffScheme_Desc, Deactive, IsBaseTariffScheme
                FROM dbo.CPATariffScheme_Mst
                ORDER BY TariffScheme_Desc
            """)
            tariff_schemes = _service_addition_rows(cursor)

            return jsonify({
                "status": "success",
                "unit": unit,
                "departments": departments,
                "subdepartments": subdepartments,
                "categories": categories,
                "tariff_schemes": tariff_schemes,
                "billing_classes": [
                    {"id": 1, "label": "OPD"},
                    {"id": 2, "label": "IPD"},
                ],
                "room_id": 0,
            })
        finally:
            try:
                conn.close()
            except Exception:
                pass


    @app.route('/api/service_addition/search')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_service_addition_search():
        from modules.db_connection import get_sql_connection

        unit, err = _get_service_addition_unit()
        if err:
            return err
        query = (request.args.get("q") or "").strip()
        if len(query) < 2:
            return jsonify({"status": "success", "services": []})

        conn = get_sql_connection(unit)
        if not conn:
            return jsonify({"status": "error", "message": "Unable to connect to database"}), 500
        try:
            cursor = conn.cursor()
            like = f"%{query}%"
            cursor.execute("""
                SELECT TOP 25 Service_ID, Service_Code, Service_Name, Category_Id,
                       DepartmentId, SubDepartmentId, Deactive, CGHSCode, BasicRate
                FROM dbo.Service_Mst
                WHERE Service_Code LIKE ? OR Service_Name LIKE ?
                ORDER BY Service_Name
            """, (like, like))
            services = _service_addition_rows(cursor)
            return jsonify({
                "status": "success",
                "services": services,
            })
        finally:
            try:
                conn.close()
            except Exception:
                pass


    @app.route('/api/service_addition/detail')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_service_addition_detail():
        from modules.db_connection import get_sql_connection

        unit, err = _get_service_addition_unit()
        if err:
            return err
        service_id = _service_addition_int(request.args.get("service_id"))
        if not service_id:
            return jsonify({"status": "error", "message": "Service ID is required."}), 400

        conn = get_sql_connection(unit)
        if not conn:
            return jsonify({"status": "error", "message": "Unable to connect to database"}), 500
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT Service_ID, Service_Code, Service_Name, DepartmentId, SubDepartmentId, Category_Id,
                       Deactive, Autorender, PatInvestigationVisible, DefaultItem, CGHSCode,
                       BasicRate, ProfitPer, SaleRate, Discount, FinalRate,
                       EmergencyCharges, Surcharge, DoctorCut, DocRequired,
                       ClinicalReportingFlag, SurchargeAmt, IsBedSideProcedure, Remarks,
                       IsOutSourceService, IsExclusiveService, IsRenewal, ServiceAliasName,
                       BlockedByTPA, IsRoutineService
                FROM dbo.Service_Mst
                WHERE Service_ID = ?
            """, (service_id,))
            services = _service_addition_rows(cursor)
            if not services:
                return jsonify({"status": "error", "message": "Service not found."}), 404
            service = services[0]

            cursor.execute("""
                SELECT Tariff_ID, TariffScheme_ID, BillingClassID, Rate, DiscountAmt
                FROM dbo.ServiceTariffSheet
                WHERE ServiceId = ?
                ORDER BY Tariff_ID DESC
            """, (service_id,))
            tariffs = _service_addition_rows(cursor)
            return jsonify({
                "status": "success",
                "service": service,
                "tariffs": tariffs,
            })
        finally:
            try:
                conn.close()
            except Exception:
                pass


    @app.route('/api/service_addition/next_code')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_service_addition_next_code():
        from modules.db_connection import get_sql_connection

        unit, err = _get_service_addition_unit()
        if err:
            return err
        category_id = _service_addition_int(request.args.get("category_id"))
        if not category_id:
            return jsonify({"status": "error", "message": "Category is required."}), 400

        conn = get_sql_connection(unit)
        if not conn:
            return jsonify({"status": "error", "message": "Unable to connect to database"}), 500
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TOP 1 Service_Code
                FROM dbo.Service_Mst
                WHERE Category_Id = ? AND Service_Code IS NOT NULL
                ORDER BY Service_ID DESC
            """, (category_id,))
            row = cursor.fetchone()
            last_code = str(row[0]).strip() if row and row[0] else ""

            cursor.execute("""
                SELECT ServiceCategory_Code
                FROM dbo.Service_Category_Mst
                WHERE ServiceCategory_ID = ?
            """, (category_id,))
            cat_row = cursor.fetchone()
            category_code = str(cat_row[0]).strip() if cat_row and cat_row[0] else ""

            next_code = _service_addition_next_code(last_code, category_code)
            return jsonify({
                "status": "success",
                "unit": unit,
                "category_id": category_id,
                "last_code": last_code,
                "next_code": next_code,
            })
        finally:
            try:
                conn.close()
            except Exception:
                pass


    @app.route('/api/service_addition/submit', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_service_addition_submit():
        from modules.db_connection import get_sql_connection

        unit, err = _get_service_addition_unit()
        if err:
            return err
        payload = request.get_json(silent=True) if request.is_json else None
        data = payload or request.form or {}

        role = (session.get("role") or "").strip()
        is_it = role == "IT"
        service_id = _service_addition_int(data.get("service_id"))
        mode = str(data.get("mode") or "").strip().lower()
        is_update = bool(service_id) or mode == "update"
        service_code = (data.get("service_code") or "").strip()
        service_name = (data.get("service_name") or "").strip()
        department_id = _service_addition_int(data.get("department_id"))
        subdepartment_id = _service_addition_int(data.get("subdepartment_id"))
        category_id = _service_addition_int(data.get("category_id"))
        raw_tariff = data.get("tariff_scheme_ids")
        if raw_tariff is None:
            raw_tariff = data.get("tariff_scheme_id")
        tariff_scheme_ids = _service_addition_id_list(raw_tariff)
        tariff_rows = _service_addition_tariff_rows(data.get("tariff_rows"))
        raw_billing = data.get("billing_class_ids")
        if raw_billing is None:
            raw_billing = data.get("billing_class_id")
        billing_classes = _service_addition_billing_classes(raw_billing)
        if billing_classes == [] and raw_billing is None:
            billing_classes = [1, 2]

        if is_update and not service_id:
            return jsonify({"status": "error", "message": "Service ID is required to update."}), 400
        if not service_code or not service_name:
            return jsonify({"status": "error", "message": "Service code and name are required."}), 400
        if not department_id or not category_id:
            return jsonify({"status": "error", "message": "Department and category are required."}), 400
        if not tariff_scheme_ids or not billing_classes:
            return jsonify({"status": "error", "message": "Tariff scheme and billing class are required."}), 400

        service_type_id = None
        service_alias = (data.get("service_alias_name") or "").strip()
        cghs_code = (data.get("cghs_code") or "").strip()
        remarks = (data.get("remarks") or "").strip()

        raw_basic = data.get("basic_rate")
        basic_rate_calc = _service_addition_decimal(raw_basic, Decimal("0"))
        basic_rate = None if raw_basic is None or str(raw_basic).strip() == "" else basic_rate_calc
        profit_per = _service_addition_decimal(data.get("profit_per"), Decimal("0"))
        sale_rate = _service_addition_decimal(data.get("sale_rate"), Decimal("0"))
        discount = _service_addition_decimal(data.get("discount"), Decimal("0"))
        final_rate = _service_addition_decimal(data.get("final_rate"), Decimal("0"))

        if sale_rate == 0 and basic_rate_calc != 0:
            sale_rate = (basic_rate_calc * (Decimal("1") + (profit_per / Decimal("100")))).quantize(Decimal("0.01"))
        if final_rate == 0 and sale_rate != 0:
            final_rate = (sale_rate - discount).quantize(Decimal("0.01"))

        tariff_rate = _service_addition_decimal(
            data.get("tariff_rate"),
            final_rate if final_rate != 0 else sale_rate
        )
        if sale_rate == 0 and tariff_rate != 0:
            sale_rate = tariff_rate
        if final_rate == 0 and tariff_rate != 0:
            final_rate = tariff_rate
        tariff_discount = _service_addition_decimal(data.get("tariff_discount"), Decimal("0"))
        rows_to_apply = []
        if tariff_rows:
            row_map = {}
            for row in tariff_rows:
                key = (row["tariff_scheme_id"], row["billing_class_id"])
                row_map[key] = row
            rows_to_apply = list(row_map.values())
        else:
            for scheme_id in tariff_scheme_ids:
                for billing_class_id in billing_classes:
                    rows_to_apply.append({
                        "tariff_scheme_id": scheme_id,
                        "billing_class_id": billing_class_id,
                        "rate": tariff_rate,
                        "discount_amt": tariff_discount,
                    })

        deactive = _service_addition_bool(data.get("deactive"))
        autorender = _service_addition_bool(data.get("autorender"))
        pat_visible = _service_addition_bool(data.get("pat_investigation_visible"))
        doc_required = _service_addition_bool(data.get("doc_required"))
        is_outsource = _service_addition_bool(data.get("is_outsource_service"))
        is_exclusive = _service_addition_bool(data.get("is_exclusive_service"))
        is_renewal = _service_addition_bool(data.get("is_renewal"))
        is_routine = _service_addition_bool(data.get("is_routine_service"))
        blocked_by_tpa = _service_addition_bool(data.get("blocked_by_tpa"))
        clinical_reporting_flag = _service_addition_bool(data.get("clinical_reporting_flag"))
        is_bedside_procedure = _service_addition_bool(data.get("is_bedside_procedure"))

        emergency_charges = _service_addition_decimal(data.get("emergency_charges"), Decimal("0"))
        surcharge = _service_addition_decimal(data.get("surcharge"), Decimal("0"))
        surcharge_amt = _service_addition_decimal(data.get("surcharge_amt"), Decimal("0"))
        doctor_cut = _service_addition_decimal(data.get("doctor_cut"), Decimal("0"))

        updated_by = _service_addition_int(data.get("updated_by"), 0)
        updated_by_alt = _service_addition_int(data.get("updated_by_alt"), updated_by)
        default_item = _service_addition_int(data.get("default_item"), 0)
        approval_required = False

        conn = get_sql_connection(unit)
        if not conn:
            return jsonify({"status": "error", "message": "Unable to connect to database"}), 500
        try:
            try:
                conn.autocommit = False
            except Exception:
                pass
            cursor = conn.cursor()
            if is_update:
                cursor.execute("""
                    SELECT Service_ID, Deactive
                    FROM dbo.Service_Mst WITH (UPDLOCK, HOLDLOCK)
                    WHERE Service_ID = ?
                """, (service_id,))
                row = cursor.fetchone()
                if not row:
                    raise RuntimeError("Service not found for update")
                if not is_it:
                    try:
                        deactive = bool(int(row[1]))
                    except Exception:
                        deactive = bool(row[1])
                cursor.execute("""
                    UPDATE dbo.Service_Mst
                    SET Service_Code = ?, Service_Name = ?, Deactive = ?, DepartmentId = ?,
                        SubDepartmentId = ?, Category_Id = ?, Updated_By = ?, Updated_On = GETDATE(),
                        Autorender = ?, ServiceType = ?, PatInvestigationVisible = ?,
                        UpdatedBy = ?, UpdatedON = GETDATE(), BasicRate = ?, ProfitPer = ?, SaleRate = ?,
                        Discount = ?, FinalRate = ?, DefaultItem = ?, EmergencyCharges = ?, Surcharge = ?,
                        DoctorCut = ?, DocRequired = ?, ClinicalReportingFlag = ?, SurchargeAmt = ?,
                        IsBedSideProcedure = ?, Remarks = ?, CGHSCode = ?, IsOutSourceService = ?, IsExclusiveService = ?,
                        IsRenewal = ?, ServiceAliasName = ?, BlockedByTPA = ?, IsRoutineService = ?
                    WHERE Service_ID = ?
                """, (
                    service_code, service_name, int(deactive), department_id,
                    subdepartment_id, category_id, updated_by,
                    int(autorender), service_type_id, int(pat_visible),
                    updated_by_alt, basic_rate, profit_per, sale_rate,
                    discount, final_rate, default_item, emergency_charges, surcharge,
                    doctor_cut, int(doc_required), int(clinical_reporting_flag), surcharge_amt,
                    int(is_bedside_procedure), remarks, cghs_code, int(is_outsource), int(is_exclusive),
                    int(is_renewal), service_alias, int(blocked_by_tpa), int(is_routine),
                    service_id
                ))
            else:
                if not is_it:
                    approval_required = True
                    deactive = True
                cursor.execute("""
                    SELECT ISNULL(MAX(Service_ID), 0) + 1
                    FROM dbo.Service_Mst WITH (UPDLOCK, HOLDLOCK)
                """)
                next_service_id_row = cursor.fetchone()
                next_service_id = int(next_service_id_row[0]) if next_service_id_row and next_service_id_row[0] else None
                if not next_service_id:
                    raise RuntimeError("Unable to generate Service_ID")
                cursor.execute("""
                    INSERT INTO dbo.Service_Mst (
                        Service_ID, Service_Code, Service_Name, Deactive, DepartmentId, SubDepartmentId, Category_Id,
                        Updated_By, Updated_On, Autorender, ServiceType, PatInvestigationVisible,
                        UpdatedBy, UpdatedON, BasicRate, ProfitPer, SaleRate, Discount, FinalRate,
                        DefaultItem, EmergencyCharges, Surcharge, DoctorCut, DocRequired,
                    ClinicalReportingFlag, SurchargeAmt, IsBedSideProcedure, Remarks, CGHSCode,
                    IsOutSourceService, IsExclusiveService, IsRenewal, ServiceAliasName,
                    BlockedByTPA, IsRoutineService
                )
                OUTPUT INSERTED.Service_ID
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, ?, ?,
                    ?, GETDATE(), ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?
                )
            """, (
                next_service_id, service_code, service_name, int(deactive), department_id, subdepartment_id, category_id,
                updated_by, int(autorender), service_type_id, int(pat_visible),
                updated_by_alt, basic_rate, profit_per, sale_rate, discount, final_rate,
                default_item, emergency_charges, surcharge, doctor_cut, int(doc_required),
                int(clinical_reporting_flag), surcharge_amt, int(is_bedside_procedure), remarks,
                cghs_code, int(is_outsource), int(is_exclusive), int(is_renewal), service_alias,
                int(blocked_by_tpa), int(is_routine)
            ))
                row = cursor.fetchone()
                service_id = int(row[0]) if row and row[0] else next_service_id

            tariff_ids = []
            cursor.execute("""
                SELECT Tariff_ID, TariffScheme_ID, BillingClassID
                FROM dbo.ServiceTariffSheet WITH (UPDLOCK, HOLDLOCK)
                WHERE ServiceId = ?
            """, (service_id,))
            existing_tariffs = _service_addition_rows(cursor)
            existing_map = {}
            for row in existing_tariffs:
                scheme_val = _service_addition_int(row.get("TariffScheme_ID"))
                billing_val = _service_addition_int(row.get("BillingClassID"))
                tariff_val = _service_addition_int(row.get("Tariff_ID"))
                if scheme_val and billing_val and tariff_val:
                    existing_map[(scheme_val, billing_val)] = tariff_val

            next_tariff_id = None
            for row in rows_to_apply:
                scheme_id = row["tariff_scheme_id"]
                billing_class_id = row["billing_class_id"]
                row_rate = row["rate"]
                row_discount = row["discount_amt"]
                key = (scheme_id, billing_class_id)
                existing_id = existing_map.get(key)
                if existing_id:
                    cursor.execute("""
                        UPDATE dbo.ServiceTariffSheet
                        SET Rate = ?, DiscountAmt = ?, UpdatedBy = ?, UpdatedOn = GETDATE(),
                            UpdatedMacName = ?
                        WHERE Tariff_ID = ?
                    """, (
                        row_rate, row_discount, updated_by_alt,
                        (session.get("username") or ""),
                        existing_id
                    ))
                    tariff_ids.append(existing_id)
                else:
                    if next_tariff_id is None:
                        cursor.execute("""
                            SELECT ISNULL(MAX(Tariff_ID), 0) + 1
                            FROM dbo.ServiceTariffSheet WITH (UPDLOCK, HOLDLOCK)
                        """)
                        next_tariff_id_row = cursor.fetchone()
                        next_tariff_id = int(next_tariff_id_row[0]) if next_tariff_id_row and next_tariff_id_row[0] else None
                        if not next_tariff_id:
                            raise RuntimeError("Unable to generate Tariff_ID")
                    cursor.execute("""
                        INSERT INTO dbo.ServiceTariffSheet (
                            Tariff_ID, ServiceId, TariffScheme_ID, RoomID, BillingClassID,
                            Rate, DiscountAmt, UpdatedBy, UpdatedOn, UpdatedMacName,
                            InsertedByUserID, InsertedON
                        )
                        OUTPUT INSERTED.Tariff_ID
                        VALUES (?, ?, ?, 0, ?, ?, ?, ?, GETDATE(), ?, ?, GETDATE())
                    """, (
                        next_tariff_id, service_id, scheme_id, billing_class_id,
                        row_rate, row_discount, updated_by_alt,
                        (session.get("username") or ""),
                        updated_by
                    ))
                    tariff_row = cursor.fetchone()
                    if tariff_row and tariff_row[0]:
                        tariff_ids.append(int(tariff_row[0]))
                    next_tariff_id += 1

            try:
                conn.commit()
            except Exception:
                pass
            if approval_required:
                _notify_it_service_approval(
                    service_id,
                    service_code,
                    service_name,
                    unit,
                    session.get("username") or ""
                )
            action = "updated" if is_update else "added"
            return jsonify({
                "status": "success",
                "service_id": service_id,
                "tariff_id": tariff_ids[0] if tariff_ids else None,
                "tariff_ids": tariff_ids,
                "action": action,
                "approval_required": approval_required,
                "message": f"Service {action} successfully."
            })
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            verb = "update" if is_update else "add"
            return jsonify({"status": "error", "message": f"Failed to {verb} service: {e}"}), 500
        finally:
            try:
                conn.close()
            except Exception:
                pass

    @app.route('/health-package-service-addition')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def health_package_service_addition():
        allowed_units = _allowed_units_for_session()
        role = (session.get("role") or "").strip()
        embed = str(request.args.get("embed") or "").strip() == "1"
        account_id = session.get("accountid") or 0
        if not account_id and role.lower() == "it":
            account_id = 1
        return render_template(
            'health_package_service_addition.html',
            allowed_units=allowed_units,
            role=role,
            embed=embed,
            account_id=account_id,
        )


    @app.route('/ip-package-service-addition')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def ip_package_service_addition():
        allowed_units = _allowed_units_for_session()
        role = (session.get("role") or "").strip()
        embed = str(request.args.get("embed") or "").strip() == "1"
        account_id = session.get("accountid") or 0
        if not account_id and role.lower() == "it":
            account_id = 1
        return render_template(
            'ip_package_service_addition.html',
            allowed_units=allowed_units,
            role=role,
            embed=embed,
            account_id=account_id,
        )


    def _get_health_package_unit():
        unit = (request.args.get("unit") or request.form.get("unit") or "").strip().upper()
        if request.is_json:
            data = request.get_json(silent=True) or {}
            unit = unit or str(data.get("unit") or "").strip().upper()
        allowed_units = _allowed_units_for_session()
        if not allowed_units:
            return None, (jsonify({"status": "error", "message": "No unit access assigned"}), 403)
        if not unit:
            if len(allowed_units) == 1:
                unit = allowed_units[0]
            else:
                return None, (jsonify({"status": "error", "message": "Please select a unit"}), 400)
        if unit not in allowed_units:
            return None, (jsonify({"status": "error", "message": f"Unit {unit} not permitted"}), 403)
        return unit, None


    def _health_package_rows(cursor):
        cols = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
        out = []
        for row in rows:
            rec = {}
            for idx, col in enumerate(cols):
                val = row[idx]
                if isinstance(val, (datetime, date)):
                    val = val.isoformat(sep=" ")
                if isinstance(val, Decimal):
                    val = float(val)
                if isinstance(val, memoryview):
                    val = val.tobytes().decode(errors="ignore")
                if isinstance(val, bytes):
                    val = val.decode(errors="ignore")
                rec[col] = val
            out.append(rec)
        return out


    def _health_package_parse_date(raw):
        if isinstance(raw, datetime):
            return raw
        if isinstance(raw, date):
            return datetime.combine(raw, datetime.min.time())
        if raw is None or str(raw).strip() == "":
            return None
        text = str(raw).strip()
        try:
            return datetime.fromisoformat(text)
        except Exception:
            try:
                return datetime.strptime(text, "%Y-%m-%d")
            except Exception:
                return None


    def _health_package_next_code(cursor) -> str:
        cursor.execute("""
            SELECT ISNULL(MAX(HealthPlanID), 0)
            FROM dbo.CPAHealthPlanMst
        """)
        row = cursor.fetchone()
        next_id = int(row[0]) + 1 if row and row[0] is not None else 1
        while True:
            candidate = f"PHP-{next_id}"
            cursor.execute(
                "SELECT 1 FROM dbo.CPAHealthPlanMst WHERE HealthPlanCode = ?",
                (candidate,),
            )
            if not cursor.fetchone():
                return candidate
            next_id += 1


    def _health_package_services(raw):
        if raw is None:
            return []
        items = []
        if isinstance(raw, (list, tuple, set)):
            items = list(raw)
        elif isinstance(raw, str):
            txt = raw.strip()
            if not txt:
                return []
            try:
                parsed = json.loads(txt)
                if isinstance(parsed, (list, tuple, set)):
                    items = list(parsed)
                elif isinstance(parsed, dict):
                    items = [parsed]
                else:
                    items = [parsed]
            except Exception:
                return []
        elif isinstance(raw, dict):
            items = [raw]
        else:
            return []

        rows = []
        for item in items:
            if not isinstance(item, dict):
                continue
            service_id = _service_addition_int(
                item.get("service_id") or item.get("pServiceid") or item.get("ServiceID")
            )
            if not service_id:
                continue
            quantity = _service_addition_int(
                item.get("quantity") or item.get("pQuantity") or 1,
                1,
            )
            rate = _service_addition_decimal(
                item.get("rate") or item.get("pRate"), Decimal("0")
            )
            category_id = _service_addition_int(
                item.get("category_id") or item.get("Category_Id") or item.get("ServiceCategoryID")
            )
            service_code = str(item.get("service_code") or item.get("Service_Code") or "").strip()
            category_code = str(
                item.get("service_category_code")
                or item.get("ServiceCategory_Code")
                or item.get("ServiceCategoryCode")
                or ""
            ).strip()
            service_name = str(item.get("service_name") or item.get("Service_Name") or "").strip()
            cghs_code = str(item.get("cghs_code") or item.get("CGHSCode") or "").strip()
            amount = rate * Decimal(quantity or 0)
            rows.append({
                "service_id": service_id,
                "quantity": quantity or 1,
                "rate": rate,
                "amount": amount,
                "category_id": category_id,
                "service_code": service_code,
                "service_category_code": category_code,
                "service_name": service_name,
                "cghs_code": cghs_code,
            })
        return rows


    @app.route('/api/health_package/init')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_health_package_init():
        from modules.db_connection import get_sql_connection

        unit, err = _get_health_package_unit()
        if err:
            return err
        conn = get_sql_connection(unit)
        if not conn:
            return jsonify({"status": "error", "message": "Unable to connect to database"}), 500
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TariffScheme_ID, TariffScheme_Code, TariffScheme_Desc, Deactive, IsBaseTariffScheme
                FROM dbo.CPATariffScheme_Mst
                ORDER BY TariffScheme_Desc
            """)
            tariff_schemes = _health_package_rows(cursor)
            return jsonify({
                "status": "success",
                "unit": unit,
                "tariff_schemes": tariff_schemes,
                "search_modes": [
                    {"id": 1, "label": "Service Name"},
                    {"id": 2, "label": "Service Code"},
                    {"id": 3, "label": "CGHS Code"},
                ],
            })
        finally:
            try:
                conn.close()
            except Exception:
                pass


    @app.route('/api/health_package/search')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_health_package_search():
        from modules.db_connection import get_sql_connection

        unit, err = _get_health_package_unit()
        if err:
            return err
        query = (request.args.get("q") or "").strip()
        page = _service_addition_int(request.args.get("page"), 1)
        page_size = _service_addition_int(request.args.get("page_size"), 20)
        page = max(1, page or 1)
        page_size = max(1, min(100, page_size or 20))
        if len(query) < 2 and not query.isdigit():
            query = ""

        conn = get_sql_connection(unit)
        if not conn:
            return jsonify({"status": "error", "message": "Unable to connect to database"}), 500
        try:
            cursor = conn.cursor()
            where_sql = "PackageOrHCPlan = RTRIM('HCP')"
            params = []
            if query:
                like = f"%{query}%"
                package_id = _service_addition_int(query, -1)
                where_sql += " AND (HealthPlanName LIKE ? OR HealthPlanCode LIKE ? OR HealthPlanID = ?)"
                params.extend([like, like, package_id])

            cursor.execute(f"SELECT COUNT(1) FROM dbo.CPAHealthPlanMst WHERE {where_sql}", params)
            count_row = cursor.fetchone()
            total = int(count_row[0]) if count_row and count_row[0] is not None else 0

            start_row = (page - 1) * page_size + 1
            end_row = page * page_size
            cursor.execute(f"""
                WITH Pack AS (
                    SELECT
                        HealthPlanID, HealthPlanCode, HealthPlanName,
                        LaunchDate, PackageCost, ValidityPeriod,
                        NoOfVisitsAllowed, Deactive, Discount, BasicCost,
                        (CASE WHEN Deactive = 1 THEN 'Deactive' ELSE 'Active' end) as Status,
                        ROW_NUMBER() OVER (ORDER BY Deactive, HealthPlanName, HealthPlanID) AS rn
                    FROM dbo.CPAHealthPlanMst
                    WHERE {where_sql}
                )
                SELECT HealthPlanID, HealthPlanCode, HealthPlanName,
                       LaunchDate, PackageCost, ValidityPeriod,
                       NoOfVisitsAllowed, Deactive, Discount, BasicCost, Status
                FROM Pack
                WHERE rn BETWEEN ? AND ?
                ORDER BY rn
            """, (*params, start_row, end_row))
            packages = _health_package_rows(cursor)
            return jsonify({
                "status": "success",
                "packages": packages,
                "page": page,
                "page_size": page_size,
                "total": total,
            })
        finally:
            try:
                conn.close()
            except Exception:
                pass


    @app.route('/api/health_package/next_code')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_health_package_next_code():
        from modules.db_connection import get_sql_connection

        unit, err = _get_health_package_unit()
        if err:
            return err

        conn = get_sql_connection(unit)
        if not conn:
            return jsonify({"status": "error", "message": "Unable to connect to database"}), 500
        try:
            cursor = conn.cursor()
            next_code = _health_package_next_code(cursor)
            return jsonify({
                "status": "success",
                "next_code": next_code,
            })
        finally:
            try:
                conn.close()
            except Exception:
                pass


    @app.route('/api/health_package/detail')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_health_package_detail():
        from modules.db_connection import get_sql_connection

        unit, err = _get_health_package_unit()
        if err:
            return err
        package_id = _service_addition_int(request.args.get("package_id"))
        if not package_id:
            return jsonify({"status": "error", "message": "Package ID is required."}), 400

        conn = get_sql_connection(unit)
        if not conn:
            return jsonify({"status": "error", "message": "Unable to connect to database"}), 500
        try:
            cursor = conn.cursor()
            cursor.execute("EXEC dbo.usp_GetOldPackDtl ?", (package_id,))
            pack_row = cursor.fetchone()
            if not pack_row:
                return jsonify({"status": "error", "message": "Package not found."}), 404
            package = {
                "HealthPlanID": package_id,
                "HealthPlanCode": pack_row[0],
                "HealthPlanName": pack_row[1],
                "LaunchDate": pack_row[2].isoformat(sep=" ") if isinstance(pack_row[2], (datetime, date)) else pack_row[2],
                "PackageCost": pack_row[3],
                "ValidityPeriod": pack_row[4],
                "NoOfVisitsAllowed": pack_row[5],
                "Discount": pack_row[6],
                "BasicCost": pack_row[7],
                "Deactive": pack_row[8],
            }
            cursor.execute("EXEC dbo.usp_GetOldPackServiceDtl ?", (package_id,))
            services = _health_package_rows(cursor)

            cat_ids = sorted({int(row.get("Category_Id")) for row in services if row.get("Category_Id") is not None})
            cat_map = {}
            if cat_ids:
                placeholders = ",".join("?" for _ in cat_ids)
                cursor.execute(
                    f"""
                    SELECT ServiceCategory_ID, ServiceCategory_Code
                    FROM dbo.Service_Category_Mst
                    WHERE ServiceCategory_ID IN ({placeholders})
                    """,
                    tuple(cat_ids),
                )
                for row in cursor.fetchall():
                    cat_map[int(row[0])] = str(row[1] or "").strip()
            for row in services:
                cat_id = row.get("Category_Id")
                if cat_id is not None:
                    row["ServiceCategory_Code"] = cat_map.get(int(cat_id), "")

            return jsonify({
                "status": "success",
                "package": package,
                "services": services,
            })
        finally:
            try:
                conn.close()
            except Exception:
                pass


    @app.route('/api/health_package/services')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_health_package_services():
        from modules.db_connection import get_sql_connection

        unit, err = _get_health_package_unit()
        if err:
            return err
        query = (request.args.get("q") or "").strip()
        if len(query) < 2:
            return jsonify({"status": "success", "services": []})
        scheme_id = _service_addition_int(request.args.get("scheme_id"))
        tag = _service_addition_int(request.args.get("tag"), 1)
        if not scheme_id:
            return jsonify({"status": "error", "message": "Tariff scheme is required."}), 400

        conn = get_sql_connection(unit)
        if not conn:
            return jsonify({"status": "error", "message": "Unable to connect to database"}), 500
        try:
            cursor = conn.cursor()
            cursor.execute("EXEC dbo.usp_GetServicesBillingNewUp ?, ?, ?", (scheme_id, query, tag))
            services = _health_package_rows(cursor)

            cat_ids = sorted({int(row.get("Category_Id")) for row in services if row.get("Category_Id") is not None})
            cat_map = {}
            if cat_ids:
                placeholders = ",".join("?" for _ in cat_ids)
                cursor.execute(
                    f"""
                    SELECT ServiceCategory_ID, ServiceCategory_Code
                    FROM dbo.Service_Category_Mst
                    WHERE ServiceCategory_ID IN ({placeholders})
                    """,
                    tuple(cat_ids),
                )
                for row in cursor.fetchall():
                    cat_map[int(row[0])] = str(row[1] or "").strip()
            for row in services:
                cat_id = row.get("Category_Id")
                if cat_id is not None:
                    row["ServiceCategory_Code"] = cat_map.get(int(cat_id), "")

            return jsonify({
                "status": "success",
                "services": services,
            })
        finally:
            try:
                conn.close()
            except Exception:
                pass


    @app.route('/api/health_package/submit', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_health_package_submit():
        from modules.db_connection import get_sql_connection

        unit, err = _get_health_package_unit()
        if err:
            return err
        payload = request.get_json(silent=True) if request.is_json else None
        data = payload or request.form or {}

        mode = str(data.get("mode") or "").strip().lower()
        package_id = _service_addition_int(data.get("package_id"))
        is_update = bool(package_id) and mode == "update"
        package_code = str(data.get("package_code") or "").strip()
        package_name = str(data.get("package_name") or "").strip()
        launch_date = _health_package_parse_date(data.get("launch_date"))
        package_cost = _service_addition_decimal(data.get("package_cost"), Decimal("0"))
        validity_period = _service_addition_int(data.get("validity_period"), 0)
        visits_allowed = _service_addition_int(data.get("visits_allowed"), 0)
        discount_amount = _service_addition_decimal(data.get("discount_amount"), Decimal("0"))
        basic_cost = _service_addition_decimal(data.get("basic_cost"), Decimal("0"))
        activation = _service_addition_int(data.get("activation"), 0)
        updated_by = _service_addition_int(data.get("updated_by"), 0)
        package_or_plan = str(data.get("package_or_plan") or "HCP").strip() or "HCP"
        services = _health_package_services(data.get("services"))

        if not package_code or not package_name or not launch_date:
            return jsonify({"status": "error", "message": "Package code, name, and launch date are required."}), 400
        if not services:
            return jsonify({"status": "error", "message": "Add at least one service before saving."}), 400
        if mode == "update" and not package_id:
            return jsonify({"status": "error", "message": "Select a package to update."}), 400

        conn = get_sql_connection(unit)
        if not conn:
            return jsonify({"status": "error", "message": "Unable to connect to database"}), 500
        try:
            cursor = conn.cursor()
            updated_on = datetime.now()

            if not is_update:
                cursor.execute("""
                    SELECT TOP 1 HealthPlanID
                    FROM dbo.CPAHealthPlanMst
                    WHERE PackageOrHCPlan = ? AND HealthPlanCode = ?
                """, (package_or_plan, package_code))
                if cursor.fetchone():
                    return jsonify({
                        "status": "error",
                        "message": "Package code already exists. Please refresh to generate a new code."
                    }), 409

            if is_update:
                cursor.execute("""
                    EXEC dbo.usp_updateCPAHealthPackageMst
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                """, (
                    package_id, package_code, package_name, launch_date, float(package_cost),
                    validity_period, visits_allowed, updated_by, updated_on,
                    float(discount_amount), float(basic_cost), package_or_plan,
                    str(updated_by), updated_on, activation,
                ))
                try:
                    cursor.fetchone()
                except Exception:
                    pass
                try:
                    while cursor.nextset():
                        pass
                except Exception:
                    pass
                cursor.execute("""
                    DELETE FROM dbo.CPAHealthPlanDtlNew
                    WHERE HealthPlanID = ?
                """, (package_id,))
            else:
                cursor.execute("""
                    EXEC dbo.usp_addCPAHealthPackageMst
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                """, (
                    0, package_code, package_name, launch_date, float(package_cost),
                    validity_period, visits_allowed, updated_by, updated_on,
                    float(discount_amount), float(basic_cost), package_or_plan,
                    str(updated_by), updated_on, activation,
                ))
                row = cursor.fetchone()
                package_id = int(row[0]) if row and row[0] else None
                try:
                    while cursor.nextset():
                        pass
                except Exception:
                    pass

            if not package_id:
                raise RuntimeError("Package ID not created")

            missing_service_ids = [
                svc.get("service_id") for svc in services
                if svc.get("service_id") and not svc.get("category_id")
            ]
            if missing_service_ids:
                placeholders = ",".join("?" for _ in missing_service_ids)
                cursor.execute(
                    f"""
                    SELECT Service_ID, Service_Code, Category_Id
                    FROM dbo.Service_Mst
                    WHERE Service_ID IN ({placeholders})
                    """,
                    tuple(missing_service_ids),
                )
                svc_map = {int(row[0]): {"code": row[1], "category_id": row[2]} for row in cursor.fetchall()}
                for svc in services:
                    svc_id = svc.get("service_id")
                    if not svc_id:
                        continue
                    info = svc_map.get(int(svc_id))
                    if info:
                        if not svc.get("service_code"):
                            svc["service_code"] = str(info.get("code") or "").strip()
                        if not svc.get("category_id"):
                            svc["category_id"] = info.get("category_id")

            missing_categories = [svc.get("service_id") for svc in services if not svc.get("category_id")]
            if missing_categories:
                raise RuntimeError(f"Category missing for services {missing_categories}")

            missing_cat_codes = {
                s["category_id"] for s in services
                if s.get("category_id") and not s.get("service_category_code")
            }
            cat_map = {}
            if missing_cat_codes:
                placeholders = ",".join("?" for _ in missing_cat_codes)
                cursor.execute(
                    f"""
                    SELECT ServiceCategory_ID, ServiceCategory_Code
                    FROM dbo.Service_Category_Mst
                    WHERE ServiceCategory_ID IN ({placeholders})
                    """,
                    tuple(sorted(missing_cat_codes)),
                )
                for row in cursor.fetchall():
                    cat_map[int(row[0])] = str(row[1] or "").strip()

            for svc in services:
                category_code = svc.get("service_category_code") or cat_map.get(svc.get("category_id"), "")
                cursor.execute("""
                    EXEC dbo.usp_addCPAHealthPackageDtl
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                """, (
                    package_id,
                    svc.get("service_id"),
                    svc.get("service_code"),
                    svc.get("category_id"),
                    category_code,
                    int(svc.get("quantity") or 1),
                    float(svc.get("rate") or 0),
                    float(svc.get("amount") or 0),
                    str(updated_by),
                    updated_on,
                ))

            try:
                conn.commit()
            except Exception:
                pass

            action = "updated" if is_update else ("copied" if mode == "copy" else "added")
            return jsonify({
                "status": "success",
                "package_id": package_id,
                "action": action,
                "message": f"Package {action} successfully.",
            })
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            verb = "update" if is_update else "add"
            return jsonify({"status": "error", "message": f"Failed to {verb} package: {e}"}), 500
        finally:
            try:
                conn.close()
            except Exception:
                pass


    @app.route('/api/ip_package/init')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_ip_package_init():
        unit, err = _get_health_package_unit()
        if err:
            return err
        result = data_fetch.fetch_ip_package_init(unit)
        http_status = int(result.get("http_status") or (200 if result.get("status") == "success" else 500))
        return jsonify(result), http_status


    @app.route('/api/ip_package/search')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_ip_package_search():
        unit, err = _get_health_package_unit()
        if err:
            return err
        query = (request.args.get("q") or "").strip()
        page = _service_addition_int(request.args.get("page"), 1)
        page_size = _service_addition_int(request.args.get("page_size"), 20)
        result = data_fetch.search_ip_packages(unit, query, page, page_size)
        http_status = int(result.get("http_status") or (200 if result.get("status") == "success" else 500))
        return jsonify(result), http_status


    @app.route('/api/ip_package/next_code')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_ip_package_next_code():
        unit, err = _get_health_package_unit()
        if err:
            return err
        result = data_fetch.get_ip_package_next_code(unit, "IPP-")
        http_status = int(result.get("http_status") or (200 if result.get("status") == "success" else 500))
        return jsonify(result), http_status


    @app.route('/api/ip_package/detail')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_ip_package_detail():
        unit, err = _get_health_package_unit()
        if err:
            return err
        package_id = _service_addition_int(request.args.get("package_id"))
        if not package_id:
            return jsonify({"status": "error", "message": "Package ID is required."}), 400
        result = data_fetch.get_ip_package_detail(unit, package_id)
        http_status = int(result.get("http_status") or (200 if result.get("status") == "success" else 500))
        return jsonify(result), http_status


    @app.route('/api/ip_package/services')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_ip_package_services():
        unit, err = _get_health_package_unit()
        if err:
            return err
        query = (request.args.get("q") or "").strip()
        scheme_id = _service_addition_int(request.args.get("scheme_id"))
        tag = _service_addition_int(request.args.get("tag"), 1)
        if not scheme_id:
            return jsonify({"status": "error", "message": "Tariff scheme is required."}), 400
        result = data_fetch.search_ip_package_services(unit, scheme_id, query, tag)
        http_status = int(result.get("http_status") or (200 if result.get("status") == "success" else 500))
        return jsonify(result), http_status


    @app.route('/api/ip_package/submit', methods=['POST'])
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_ip_package_submit():
        unit, err = _get_health_package_unit()
        if err:
            return err
        payload = request.get_json(silent=True) if request.is_json else None
        data = payload or request.form or {}
        result = data_fetch.save_ip_package(
            unit,
            data,
            username=(session.get("username") or ""),
        )
        http_status = int(result.get("http_status") or (200 if result.get("status") == "success" else 500))
        return jsonify(result), http_status
