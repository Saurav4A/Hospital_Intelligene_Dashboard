from flask import jsonify, render_template, request, send_file
from datetime import datetime, timedelta
import io
import os
import sqlite3
import pandas as pd
import pyodbc

import config


def register_occupancy_routes(
    app,
    *,
    login_required,
    allowed_units_for_session,
    pick_db_cfg_for_proc,
    build_conn_str_from_dbconfig,
    save_occupancy_snapshot,
    fetch_snapshot,
    pct_change,
    ensure_snapshot_detail_table,
    abs_local_db_path,
    ensure_local_store,
    export_cache_get_bytes,
    export_cache_put_bytes,
    save_occupancy_snapshot_with_subtype,
    build_occupancy_snapshot_pdf_buffer,
    build_occupancy_average_pdf_buffer,
    collect_current_occupancy_rows,
    build_current_occupancy_pdf_buffer,
    local_tz,
):
    """Register occupancy dashboard and export routes."""
    _allowed_units_for_session = allowed_units_for_session
    _pick_db_cfg_for_proc = pick_db_cfg_for_proc
    _build_conn_str_from_dbconfig = build_conn_str_from_dbconfig
    _save_occupancy_snapshot = save_occupancy_snapshot
    _fetch_snapshot = fetch_snapshot
    _pct_change = pct_change
    _ensure_snapshot_detail_table = ensure_snapshot_detail_table
    _abs_local_db_path = abs_local_db_path
    _ensure_local_store = ensure_local_store
    _export_cache_get_bytes = export_cache_get_bytes
    _export_cache_put_bytes = export_cache_put_bytes
    _save_occupancy_snapshot_with_subtype = save_occupancy_snapshot_with_subtype
    _build_occupancy_snapshot_pdf_buffer = build_occupancy_snapshot_pdf_buffer
    _build_occupancy_average_pdf_buffer = build_occupancy_average_pdf_buffer
    _collect_current_occupancy_rows = collect_current_occupancy_rows
    _build_current_occupancy_pdf_buffer = build_current_occupancy_pdf_buffer
    LOCAL_TZ = local_tz

    @app.route('/occupancy')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def occupancy_dashboard():
        allowed_units = _allowed_units_for_session()
        return render_template('occupancy.html', allowed_units=allowed_units)

    @app.route('/get_occupancy_data')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def get_occupancy_data():
        try:
            today = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")

            allowed_units = _allowed_units_for_session()
            if not allowed_units:
                return jsonify({"status": "error", "message": "No unit access assigned"}), 403

            db_cfgs = getattr(config, "DB_CONFIGS", {}) or {}
            if db_cfgs:
                db_cfgs = {u: cfg for u, cfg in db_cfgs.items() if u.upper() in allowed_units}
            if not db_cfgs:
                chosen_cfg = _pick_db_cfg_for_proc()
                if not chosen_cfg:
                    return jsonify({"status": "error", "message": "No suitable DB config found"}), 500
                db_cfgs = {"_FALLBACK_": chosen_cfg}

            overall = {}
            errors = []

            for unit_name, cfg in db_cfgs.items():
                try:
                    conn_str = _build_conn_str_from_dbconfig(cfg)
                    with pyodbc.connect(conn_str) as conn:
                        try:
                            df = pd.read_sql(
                                "EXEC dbo.usp_RptIPDPatient_Occupancy @FromDate=?, @ToDate=?",
                                conn,
                                params=(today, today)
                            )
                        except Exception:
                            df = pd.read_sql("EXEC dbo.usp_RptIPDPatient_Occupancy", conn)

                    if df is None or df.empty:
                        continue

                    df = df.where(pd.notna(df), None)

                    ward_col = next((c for c in ["Ward_Name", "WardName", "Ward", "WardDesc", "WardDescription"] if c in df.columns), None)
                    bed_label_col = next((c for c in ["Bed_Name", "BedName", "Bed", "BedNo", "Bed_No"] if c in df.columns), None)
                    status_col = next((c for c in ["Status", "PatientStatus", "ClinicalStatus", "DischargeStatus", "CurrentStatus", "CaseStatus"] if c in df.columns), None)

                    if "Bed_ID" not in df.columns:
                        errors.append(f"{unit_name}: result missing Bed_ID")
                        continue
                    if not ward_col:
                        ward_col = "Ward"
                        df[ward_col] = "Unknown Ward"

                    import math
                    def _sv(v):
                        try:
                            if v is None or (isinstance(v, float) and math.isnan(v)) or pd.isna(v):
                                return None
                        except Exception:
                            pass
                        if isinstance(v, str):
                            s = v.strip()
                            if s == "" or s.lower() in ("nan", "none", "null"):
                                return None
                            return s
                        return v

                    def _get(row, *keys):
                        for k in keys:
                            if k and k in row:
                                val = _sv(row.get(k))
                                if val is not None:
                                    return val
                        return None

                    def _has_patient(row):
                        return any([
                            _get(row, "Patient") is not None,
                            _get(row, "Patient_Name") is not None,
                            _get(row, "Regno") is not None,
                            _get(row, "Visit_ID") is not None
                        ])

                    def _row_color(row):
                        st = (_get(row, status_col) or "").lower()
                        if "clinic" in st and "disch" in st:
                            return "yellow"
                        if "occup" in st:
                            return "green"
                        if "vac" in st:
                            return "blue"
                        return "green" if _has_patient(row) else "blue"

                    df["_PrefWard"] = unit_name.upper() + " | " + df[ward_col].astype(str)

                    dict_rows = df.to_dict(orient="records")
                    for ward_name, g_idx in df.groupby("_PrefWard", sort=True).groups.items():
                        rows = [dict_rows[i] for i in g_idx]
                        bedlist = []
                        for r in rows:
                            status_text = (_get(r, status_col) or "").lower()
                            color = _row_color(r)
                            is_vacant = ("vac" in status_text) or (color == "blue")
                            bed = {
                                "Bed_ID": _sv(_get(r, "Bed_ID")),
                                "Bed_Label": _sv(_get(r, bed_label_col, "Bed_Name", "BedName", "Bed", "BedNo", "Bed_No", "Bed_ID")),
                                "Color": color,
                                "Patient_ID": _sv(_get(r, "Regno")),
                                "Patient_Name": _sv(_get(r, "Patient", "Patient_Name")),
                                "IPD_No": _sv(_get(r, "Visit_ID")),
                                "Room": _sv(_get(r, "Room", "Room_Name", "RoomName")),
                            }
                            if is_vacant:
                                bed["Patient_ID"] = None
                                bed["Patient_Name"] = None
                                bed["IPD_No"] = None
                            bedlist.append(bed)

                        overall.setdefault(ward_name, []).extend(bedlist)

                except Exception as ex:
                    errors.append(f"{unit_name}: {ex}")

            if not overall:
                msg = " ; ".join(errors) if errors else "No data returned from any DB"
                print(f"Occupancy fetch error: {msg}")
                return jsonify({"status": "error", "message": msg}), 500

            legend = {"green": "Occupied", "yellow": "Clinically Discharged", "blue": "Vacant"}

            # Persist today's snapshot (idempotent per-day per-unit)
            try:
                _save_occupancy_snapshot(overall)
            except Exception as _e:
                print(f"Snapshot save skipped: {_e}")

            return jsonify({"status": "success", "legend": legend, "data": overall})

        except Exception as e:
            print(f"Occupancy fetch error: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    @app.route('/occupancy_trend')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def occupancy_trend():
        requested_unit = (request.args.get("unit") or "").strip().upper() or None

        allowed_units = _allowed_units_for_session()
        if not allowed_units:
            return jsonify({"status": "error", "message": "No unit access assigned"}), 403

        if requested_unit:
            if requested_unit not in allowed_units:
                return jsonify({"status": "error", "message": f"Unit {requested_unit} not permitted for your role"}), 403
            unit = requested_unit
        else:
            if len(allowed_units) == 1:
                unit = allowed_units[0]
            else:
                unit = None

        today_dt = datetime.now(tz=LOCAL_TZ)
        fmt = "%Y-%m-%d"
        today = today_dt.strftime(fmt)
        d1    = (today_dt - timedelta(days=1)).strftime(fmt)
        w1    = (today_dt - timedelta(days=7)).strftime(fmt)
        m1    = (today_dt - timedelta(days=30)).strftime(fmt)

        curr = _fetch_snapshot(today, unit)
        if not curr:
            return jsonify({
                "status": "error",
                "message": "No snapshot for today yet. Open the Occupancy dashboard once today to record it."
            }), 404

        resp = {
            "status": "success",
            "unit": unit or "ALL",
            "today": today,
            "metrics": {
                "occupied": curr["occupied"],
                "clinically_discharged": curr["clinically_discharged"],
                "vacant": curr["vacant"],
                "total_beds": curr["total_beds"],
                "occupancy_rate_pct": round(curr["occupancy_rate"] * 100.0, 2)
            },
            "comparisons": {}
        }

        for label, date_str in (("d1", d1), ("w1", w1), ("m1", m1)):
            prev = _fetch_snapshot(date_str, unit)
            if prev:
                resp["comparisons"][label] = {
                    "date": date_str,
                    "delta_pct_occupied": _pct_change(curr["occupied"], prev["occupied"]),
                    "delta_pct_rate": _pct_change(curr["occupancy_rate"], prev["occupancy_rate"])
                }
            else:
                resp["comparisons"][label] = {
                    "date": date_str,
                    "delta_pct_occupied": None,
                    "delta_pct_rate": None,
                    "note": "no snapshot for that date"
                }

        return jsonify(resp)

    @app.route('/api/occupancy_snapshot_dates')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_occupancy_snapshot_dates():
        """Get list of available snapshot dates"""
        try:
            _ensure_snapshot_detail_table()
        
            with sqlite3.connect(_abs_local_db_path()) as conn:
                rows = conn.execute("""
                    SELECT DISTINCT snapshot_date 
                    FROM occupancy_snapshot_detail 
                    ORDER BY snapshot_date DESC
                    LIMIT 90
                """).fetchall()
        
            dates = [row[0] for row in rows]
            return jsonify({"status": "success", "dates": dates})
        
        except Exception as e:
            print(f"Error fetching snapshot dates: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    @app.route('/api/occupancy_snapshot_average')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_occupancy_snapshot_average():
        """Average occupancy percentage between two dates (summary table)."""
        from_date = (request.args.get("from") or "").strip()
        to_date = (request.args.get("to") or "").strip()
        unit = (request.args.get("unit") or "").strip().upper()

        if not from_date or not to_date:
            return jsonify({"status": "error", "message": "from/to dates are required"}), 400

        try:
            from_dt = datetime.strptime(from_date, "%Y-%m-%d").date()
            to_dt = datetime.strptime(to_date, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"status": "error", "message": "Invalid date format (expected YYYY-MM-DD)"}), 400

        if from_dt > to_dt:
            return jsonify({"status": "error", "message": "from date cannot be after to date"}), 400

        allowed_units = _allowed_units_for_session()
        if not allowed_units:
            return jsonify({"status": "error", "message": "No unit access"}), 403

        if unit and unit != "ALL":
            if unit not in allowed_units:
                return jsonify({"status": "error", "message": "Unit not allowed"}), 403
            units_to_use = [unit]
        else:
            units_to_use = allowed_units

        _ensure_local_store()
        with sqlite3.connect(_abs_local_db_path()) as conn:
            placeholders = ",".join("?" * len(units_to_use))
            query = f"""
                SELECT snapshot_date, unit, occupied, clinically_discharged, total_beds, occupancy_rate
                FROM occupancy_snapshot
                WHERE snapshot_date BETWEEN ? AND ?
                  AND unit IN ({placeholders})
            """
            rows = conn.execute(query, [from_date, to_date] + units_to_use).fetchall()

        if not rows:
            return jsonify({
                "status": "success",
                "from": from_date,
                "to": to_date,
                "unit": unit or "ALL",
                "rows": 0,
                "days": 0,
                "simple_avg_pct": 0.0,
                "weighted_avg_pct": 0.0,
                "message": "No snapshot data for this range"
            })

        total_occ = 0
        total_beds = 0
        rate_sum = 0.0
        for snapshot_date, _unit, occ, disch, beds, rate in rows:
            total_occ += int(occ or 0) + int(disch or 0)
            total_beds += int(beds or 0)
            rate_sum += float(rate or 0)

        days = len({r[0] for r in rows})
        simple_avg = (rate_sum / len(rows)) * 100.0 if rows else 0.0
        weighted_avg = (total_occ / total_beds) * 100.0 if total_beds else 0.0

        return jsonify({
            "status": "success",
            "from": from_date,
            "to": to_date,
            "unit": unit or "ALL",
            "rows": len(rows),
            "days": days,
            "simple_avg_pct": round(simple_avg, 2),
            "weighted_avg_pct": round(weighted_avg, 2)
        })

    @app.route('/_test_snapshot')
    @login_required(allowed_roles={"IT", "Management"})
    def _test_snapshot():
        """Manually trigger snapshot for testing - IT/Management only"""
        try:
            # Ensure table exists with correct schema (including ward column)
            _ensure_snapshot_detail_table()
        
            # Capture snapshot with Ward + PatientSubType breakdown
            _save_occupancy_snapshot_with_subtype()
        
            # Verify what was saved
            with sqlite3.connect(_abs_local_db_path()) as conn:
                # Count total records
                count = conn.execute("SELECT COUNT(*) FROM occupancy_snapshot_detail").fetchone()[0]
            
                # Get distinct dates
                dates = conn.execute("""
                    SELECT DISTINCT snapshot_date 
                    FROM occupancy_snapshot_detail 
                    ORDER BY snapshot_date DESC
                """).fetchall()
            
                # Get sample data to verify structure
                sample = conn.execute("""
                    SELECT unit, ward, patient_subtype, occupied, 
                           clinically_discharged, vacant, total_beds, 
                           snapshot_time
                    FROM occupancy_snapshot_detail 
                    LIMIT 5
                """).fetchall()
        
            # Format sample data for display
            sample_data = []
            for row in sample:
                unit, ward, subtype, occ, disch, vac, total, snap_time = row
                sample_data.append({
                    "unit": unit,
                    "ward": ward,
                    "patient_subtype": subtype,
                    "occupied": occ,
                    "clinically_discharged": disch,
                    "vacant": vac,
                    "total_beds": total,
                    "snapshot_time": snap_time
                })
        
            return jsonify({
                "status": "success", 
                "message": "Snapshot captured successfully!",
                "total_records": count,
                "dates": [d[0] for d in dates],
                "sample_data": sample_data,
                "note": "Data now includes Ward + Patient Sub-Type breakdown"
            })
        
        except Exception as e:
            import traceback
            return jsonify({
                "status": "error", 
                "message": str(e),
                "traceback": traceback.format_exc()
            }), 500

    @app.route('/api/occupancy_snapshot_data')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_occupancy_snapshot_data():
        """Get snapshot data for a specific date"""
        try:
            date = request.args.get('date')
            if not date:
                return jsonify({"status": "error", "message": "Date parameter required"}), 400
        
            _ensure_snapshot_detail_table()
        
            allowed_units = _allowed_units_for_session()
            if not allowed_units:
                return jsonify({"status": "error", "message": "No unit access"}), 403

            cache_key = (date, ",".join(sorted(allowed_units)))
            cached = _export_cache_get_bytes("occ_snap_xlsx", cache_key)
            if cached:
                return send_file(
                    io.BytesIO(cached),
                    as_attachment=True,
                    download_name=f"Occupancy_Snapshot_{date}.xlsx",
                    mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            cache_key = (date, ",".join(sorted(allowed_units)))
            cached = _export_cache_get_bytes("occ_snap_xlsx", cache_key)
            if cached:
                return send_file(
                    io.BytesIO(cached),
                    as_attachment=True,
                    download_name=f"Occupancy_Snapshot_{date}.xlsx",
                    mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
            with sqlite3.connect(_abs_local_db_path()) as conn:
                placeholders = ','.join('?' * len(allowed_units))
                query = f"""
                    SELECT unit, ward, patient_subtype, occupied, clinically_discharged,
                           vacant, total_beds, occupancy_rate, snapshot_time
                    FROM occupancy_snapshot_detail
                    WHERE snapshot_date = ? AND unit IN ({placeholders})
                    ORDER BY unit, ward, patient_subtype
                """
                rows = conn.execute(query, [date] + allowed_units).fetchall()
        
            if not rows:
                return jsonify({
                    "status": "success",
                    "date": date,
                    "data": {},
                    "message": "No data for this date"
                })
        
            # Group by unit
            data = {}
            for row in rows:
                unit, ward, subtype, occ, disch, vac, total, rate, snap_time = row
                if unit not in data:
                    data[unit] = []
                data[unit].append({
                    "Ward": ward,
                    "PatientSubType": subtype,
                    "Occupied": occ,
                    "ClinicallyDischarged": disch,
                    "Vacant": vac,
                    "TotalBeds": total,
                    # "OccupancyRate": round(rate * 100, 2),
                    "SnapshotTime": snap_time
                })
        
            return jsonify({
                "status": "success",
                "date": date,
                "data": data
            })
        
        except Exception as e:
            print(f"Error fetching snapshot data: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    @app.route('/api/export_occupancy_snapshot')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def api_export_occupancy_snapshot():
        """Export snapshot data to Excel"""
        try:
            date = request.args.get('date')
            if not date:
                return jsonify({"status": "error", "message": "Date parameter required"}), 400
        
            _ensure_snapshot_detail_table()
        
            allowed_units = _allowed_units_for_session()
            if not allowed_units:
                return jsonify({"status": "error", "message": "No unit access"}), 403

            cache_key = (date, ",".join(sorted(allowed_units)))
            cached = _export_cache_get_bytes("occ_snap_xlsx", cache_key)
            if cached:
                return send_file(
                    io.BytesIO(cached),
                    as_attachment=True,
                    download_name=f"Occupancy_Snapshot_{date}.xlsx",
                    mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
            with sqlite3.connect(_abs_local_db_path()) as conn:
                placeholders = ','.join('?' * len(allowed_units))
                query = f"""
                    SELECT unit, ward, patient_subtype, occupied, clinically_discharged,
                           vacant, total_beds, occupancy_rate, snapshot_time
                    FROM occupancy_snapshot_detail
                    WHERE snapshot_date = ? AND unit IN ({placeholders})
                    ORDER BY unit, ward, patient_subtype
                """
                rows = conn.execute(query, [date] + allowed_units).fetchall()
                doc_query = f"""
                    SELECT unit, doctor, occupied
                    FROM occupancy_snapshot_doctor
                    WHERE snapshot_date = ? AND unit IN ({placeholders})
                    ORDER BY unit, occupied DESC, doctor
                """
                doc_rows = conn.execute(doc_query, [date] + allowed_units).fetchall()
        
            if not rows:
                return jsonify({"status": "error", "message": "No data for this date"}), 404
        
            # Prepare data for Excel - FIXED to match frontend
            export_data = []
            for row in rows:
                unit, ward, subtype, occ, disch, vac, total, rate, snap_time = row
                total_active = occ + disch
                export_data.append({
                    'Unit': unit,
                    'Ward': ward,
                    'Patient Sub-Type': subtype,
                    'Occupied': occ,
                    'Clinically Discharged': disch,
                    'Total Active (Beds Used)': total_active,
                    'Snapshot Time': snap_time
                })
        
            df = pd.DataFrame(export_data)
            doctor_export = [
                {
                    "Unit": (unit or "").upper(),
                    "Doctor": doctor or "Unknown Doctor",
                    "Occupied Patients": int(occupied or 0)
                }
                for unit, doctor, occupied in doc_rows
            ]
            doctor_df = pd.DataFrame(doctor_export)
        
            # Create Excel file
            export_dir = os.path.join("data", "exports")
            os.makedirs(export_dir, exist_ok=True)
            file_name = f"Occupancy_Snapshot_{date}.xlsx"
            file_path = os.path.join(export_dir, file_name)
        
            with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
                wb = writer.book
            
                # Format definitions
                header_fmt = wb.add_format({
                    "bold": True, "bg_color": "#1e3a8a",
                    "font_color": "white", "align": "center",
                    "valign": "vcenter", "border": 1
                })
            
                # Write data
                df.to_excel(writer, index=False, sheet_name="Ward Summary")
                ws = writer.sheets["Ward Summary"]
            
                # Apply header formatting
                for col_num, value in enumerate(df.columns.values):
                    ws.write(0, col_num, value, header_fmt)
            
                # Auto-fit columns
                for i, col in enumerate(df.columns):
                    col_len = df[col].astype(str).apply(len).max() if not df.empty else 0
                    max_len = max(col_len, len(col)) + 2
                    ws.set_column(i, i, max_len)
            
                ws.freeze_panes(1, 0)

                if doctor_df.empty:
                    doctor_df = pd.DataFrame(columns=["Unit", "Doctor", "Occupied Patients"])
                doctor_df.to_excel(writer, index=False, sheet_name="Doctor Summary")
                ws_doc = writer.sheets["Doctor Summary"]
                for col_num, value in enumerate(doctor_df.columns.values):
                    ws_doc.write(0, col_num, value, header_fmt)
                for i, col in enumerate(doctor_df.columns):
                    max_len = max(doctor_df[col].astype(str).apply(len).max() if not doctor_df.empty else 0, len(col)) + 2
                    ws_doc.set_column(i, i, max_len)
                ws_doc.freeze_panes(1, 0)
        
            try:
                with open(file_path, "rb") as f:
                    _export_cache_put_bytes("occ_snap_xlsx", f.read(), cache_key)
            except Exception:
                pass
            return send_file(file_path, as_attachment=True)
        
        except Exception as e:
            print(f"Error exporting snapshot: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    @app.route('/_clear_snapshots')
    @login_required(allowed_roles={"IT"})
    def _clear_snapshots():
        """Clear all snapshot data - IT only"""
        try:
            with sqlite3.connect(_abs_local_db_path()) as conn:
                conn.execute("DELETE FROM occupancy_snapshot_detail")
                conn.commit()
        
            return jsonify({
                "status": "success",
                "message": "All snapshots cleared"
            })
        except Exception as e:
            import traceback
            return jsonify({
                "status": "error",
                "message": str(e),
                "traceback": traceback.format_exc()
            }), 500

    @app.route('/api/export_occupancy_snapshot_pdf')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def export_occupancy_snapshot_pdf():
        """Export occupancy snapshot as PDF with ward-level summary"""
        from datetime import datetime

        date = request.args.get('date')
        if not date:
            return jsonify({'status': 'error', 'message': 'Date parameter required'}), 400

        try:
            allowed_units = _allowed_units_for_session()
            if not allowed_units:
                return jsonify({'status': 'error', 'message': 'No unit access'}), 403

            buffer = _build_occupancy_snapshot_pdf_buffer(date, allowed_units)
            if buffer is None:
                return jsonify({'status': 'error', 'message': 'No data found for this date'}), 404
            return send_file(
                buffer,
                mimetype='application/pdf',
                as_attachment=True,
                download_name=f'Occupancy_Snapshot_{date}.pdf'
            )

        except Exception as e:
            print(f"PDF export error: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'status': 'error', 'message': str(e)}), 500

    @app.route('/api/export_occupancy_average_pdf')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def export_occupancy_average_pdf():
        """Export average occupancy between two dates as PDF."""
        from_date = (request.args.get("from") or "").strip()
        to_date = (request.args.get("to") or "").strip()
        unit = (request.args.get("unit") or "").strip().upper()

        if not from_date or not to_date:
            return jsonify({'status': 'error', 'message': 'from/to dates are required'}), 400

        try:
            from_dt = datetime.strptime(from_date, "%Y-%m-%d").date()
            to_dt = datetime.strptime(to_date, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({'status': 'error', 'message': 'Invalid date format (expected YYYY-MM-DD)'}), 400

        if from_dt > to_dt:
            return jsonify({'status': 'error', 'message': 'from date cannot be after to date'}), 400

        allowed_units = _allowed_units_for_session()
        if not allowed_units:
            return jsonify({'status': 'error', 'message': 'No unit access'}), 403

        if unit and unit != "ALL":
            if unit not in allowed_units:
                return jsonify({'status': 'error', 'message': 'Unit not allowed'}), 403
            units_to_use = [unit]
        else:
            units_to_use = allowed_units
        buffer = _build_occupancy_average_pdf_buffer(from_date, to_date, units_to_use)
        if buffer is None:
            return jsonify({'status': 'error', 'message': 'No snapshot data for this range'}), 404

        return send_file(
            buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f'Average_Occupancy_{from_date}_to_{to_date}.pdf'
        )

    @app.route('/api/export_current_occupancy_pdf')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def export_current_occupancy_pdf():
        """Export current live occupancy summary to PDF."""
        from datetime import datetime

        allowed_units = _allowed_units_for_session()
        if not allowed_units:
            return jsonify({'status': 'error', 'message': 'No unit access'}), 403

        try:
            buffer = _build_current_occupancy_pdf_buffer(allowed_units)
            if buffer is None:
                return jsonify({'status': 'error', 'message': 'No occupancy data available'}), 404
            today = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d")
            return send_file(
                buffer,
                mimetype='application/pdf',
                as_attachment=True,
                download_name=f'Current_Occupancy_{today}.pdf'
            )

        except Exception as e:
            print(f"Current occupancy PDF error: {e}")
            return jsonify({'status': 'error', 'message': str(e)}), 500

    @app.route('/api/export_current_occupancy_excel')
    @login_required(allowed_roles={"IT", "Management", "Departmental Head"})
    def export_current_occupancy_excel():
        """Export current occupancy summary to Excel (ward + doctor wise)."""
        from datetime import datetime

        allowed_units = _allowed_units_for_session()
        if not allowed_units:
            return jsonify({'status': 'error', 'message': 'No unit access'}), 403

        cache_key = ",".join(sorted(allowed_units))
        cached = _export_cache_get_bytes("current_occ_xlsx", cache_key)
        if cached:
            stamp = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d_%H%M")
            return send_file(
                io.BytesIO(cached),
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True,
                download_name=f"Current_Occupancy_{stamp}.xlsx"
            )

        try:
            rows_for_excel, doctor_summary = _collect_current_occupancy_rows(allowed_units)
            if not rows_for_excel:
                return jsonify({'status': 'error', 'message': 'No occupancy data available'}), 404

            ward_rows = [{
                "Unit": unit,
                "Ward": ward,
                "Patient Sub-Type": subtype,
                "Occupied": occ,
                "Clinically Discharged": disch,
                "Vacant": vac,
                "Total Beds": total
            } for unit, ward, subtype, occ, disch, vac, total in rows_for_excel]

            doctor_rows = []
            for unit, docs in (doctor_summary or {}).items():
                for doctor_name, count in docs:
                    doctor_rows.append({
                        "Unit": unit,
                        "Doctor": doctor_name,
                        "Occupied Patients": count
                    })

            if not doctor_rows:
                doctor_rows = []

            ward_df = pd.DataFrame(ward_rows)
            doctor_df = pd.DataFrame(doctor_rows)

            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                wb = writer.book
                header_fmt = wb.add_format({
                    "bold": True, "bg_color": "#1e3a8a",
                    "font_color": "white", "align": "center",
                    "valign": "vcenter", "border": 1
                })

                ward_sheet = "Ward Summary"
                ward_df.to_excel(writer, index=False, sheet_name=ward_sheet)
                ws = writer.sheets[ward_sheet]
                for col_num, value in enumerate(ward_df.columns):
                    ws.write(0, col_num, value, header_fmt)
                for i, col in enumerate(ward_df.columns):
                    col_len = ward_df[col].astype(str).apply(len).max() if not ward_df.empty else 0
                    ws.set_column(i, i, max(col_len, len(col)) + 2)
                ws.freeze_panes(1, 0)

                doctor_sheet = "Doctor Summary"
                if doctor_df.empty:
                    doctor_df = pd.DataFrame(columns=["Unit", "Doctor", "Occupied Patients"])
                doctor_df.to_excel(writer, index=False, sheet_name=doctor_sheet)
                ws_doc = writer.sheets[doctor_sheet]
                for col_num, value in enumerate(doctor_df.columns):
                    ws_doc.write(0, col_num, value, header_fmt)
                for i, col in enumerate(doctor_df.columns):
                    col_len = doctor_df[col].astype(str).apply(len).max() if not doctor_df.empty else 0
                    ws_doc.set_column(i, i, max(col_len, len(col)) + 2)
                ws_doc.freeze_panes(1, 0)

            buffer.seek(0)
            stamp = datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d_%H%M")
            _export_cache_put_bytes("current_occ_xlsx", buffer.getvalue(), cache_key)
            return send_file(
                buffer,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True,
                download_name=f"Current_Occupancy_{stamp}.xlsx"
            )

        except Exception as e:
            print(f"Current occupancy Excel error: {e}")
            return jsonify({'status': 'error', 'message': str(e)}), 500
