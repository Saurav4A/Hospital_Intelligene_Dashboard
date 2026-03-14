# modules/otp_routes.py

from flask import Blueprint, request, jsonify
import pyodbc
from modules.db_connection import get_sql_connection
from decimal import Decimal

# Central/OTP DB uses AHL connection (Prodoc2021)
OTP_CENTER_UNIT = "AHL"

otp_bp = Blueprint("otp_bp", __name__)

# CENTRAL OTP DB (Prodoc2021)
OTP_SERVER = "192.168.1.4"
OTP_DB     = "Prodoc2021"

# UNIT → SERVER + DB mapping
DB_MAP = {
    "Dhanbad":  {"server": "192.168.1.4",    "db": "Prodoc2021"},
    "ACI":      {"server": "192.168.20.100", "db": "ACI"},
    "Ballia":   {"server": "192.168.1.102",  "db": "Prodoc2022"},
}


# =====================================================================================
# 1️⃣ REQUEST OTP (Create request + worker will email it)
# =====================================================================================
@otp_bp.route("/api/otp/request", methods=["POST"])
def request_otp():
    data = request.json

    unit          = (data or {}).get("unit")
    bill_no       = (data or {}).get("bill_no")
    receipt_no    = (data or {}).get("receipt_no") or ""
    request_type  = (data or {}).get("request_type")    # DISCOUNT / CANCELLATION
    reason        = (data or {}).get("reason") or ""
    amount        = (data or {}).get("amount")
    uhid          = (data or {}).get("uhid") or ""
    patient_name  = (data or {}).get("patient_name") or ""
    requested_by  = (data or {}).get("requested_by")

    # Basic validation before hitting DB
    missing = []
    if not unit:
        missing.append("unit")
    # Either bill_no or receipt_no must be present
    if not bill_no and not receipt_no:
        missing.append("bill_no or receipt_no")
    if not request_type:
        missing.append("request_type")
    if amount in (None, "", " "):
        missing.append("amount")
    if not requested_by:
        missing.append("requested_by")
    if missing:
        return jsonify({"success": False, "error": f"Missing fields: {', '.join(missing)}"}), 400

    # Normalize values
    request_type = str(request_type or "").strip().upper()
    unit = str(unit or "").strip().upper()
    try:
        amount = Decimal(str(amount))
    except Exception:
        return jsonify({"success": False, "error": "amount must be numeric"}), 400

    try:
        conn = get_sql_connection(OTP_CENTER_UNIT)
        if not conn:
            return jsonify({"success": False, "error": "Unable to connect to OTP database"}), 500
        try:
            conn.autocommit = True
        except Exception:
            pass
        cursor = conn.cursor()

        # First try new signature (with @Unit). If the proc is older, fall back to legacy call.
        try:
            cursor.execute("""
                DECLARE @id INT, @otp VARCHAR(10);
                EXEC dbo.usp_CreateDiscountApprovalRequest
                    @Unit=?, @BillNo=?, @ReceiptNo=?, @RequestType=?, @RequestedAmount=?,
                    @Reason=?, @RequestedByUserName=?, @UHIDNo=?, @PatientName=?,
                    @RequestId=@id OUTPUT, @OtpPlainOut=@otp OUTPUT;
                SELECT @id, @otp;
            """, (unit, bill_no, receipt_no, request_type, amount,
                  reason, requested_by, uhid, patient_name))
        except Exception as e:
            # Too many args -> legacy proc without @Unit
            if "8144" in str(e) or "too many arguments" in str(e).lower():
                cursor.execute("""
                    DECLARE @id INT, @otp VARCHAR(10);
                    EXEC dbo.usp_CreateDiscountApprovalRequest
                        @BillNo=?, @ReceiptNo=?, @RequestType=?, @RequestedAmount=?,
                        @Reason=?, @RequestedByUserName=?, @UHIDNo=?, @PatientName=?,
                        @RequestId=@id OUTPUT, @OtpPlainOut=@otp OUTPUT;
                    SELECT @id, @otp;
                """, (bill_no, receipt_no, request_type, amount,
                      reason, requested_by, uhid, patient_name))
            else:
                raise

        row = cursor.fetchone()
        if not row or not row[0]:
            return jsonify({"success": False, "error": "OTP request not created (proc returned empty id)"}), 500
        request_id, otp_plain = row

        # Verify row exists (defensive)
        try:
            cursor.execute("SELECT TOP 1 Id FROM DiscountApprovalRequest WHERE Id = ?", request_id)
            check = cursor.fetchone()
            if not check:
                return jsonify({"success": False, "error": "OTP row not found after insert"}), 500
            # Ensure Unit is set even if the proc ignored it
            if unit:
                cursor.execute("UPDATE DiscountApprovalRequest SET Unit = ? WHERE Id = ?", unit, request_id)
        except Exception as _e:
            print(f"OTP insert verification failed: id={request_id}, err={_e}")

        try:
            conn.commit()
        except Exception:
            pass
        conn.close()

        return jsonify({"success": True, "request_id": request_id, "otp": otp_plain})

    except Exception as e:
        print(f"OTP request failed: unit={unit}, bill={bill_no}, req_type={request_type}, amount={amount}, user={requested_by}, error={e}")
        return jsonify({"success": False, "error": str(e)}), 500



# =====================================================================================
# 2️⃣ VALIDATE OTP (Check if OTP matches)
# =====================================================================================
@otp_bp.route("/api/otp/validate", methods=["POST"])
def validate_otp():
    data = request.json
    request_id = (data or {}).get("request_id")
    otp        = (data or {}).get("otp")

    if request_id in (None, "", " "):
        return jsonify({"success": False, "error": "Missing request_id"}), 400
    if otp in (None, "", " "):
        return jsonify({"success": False, "error": "Missing otp"}), 400
    try:
        request_id = int(str(request_id).strip())
    except Exception:
        return jsonify({"success": False, "error": "Invalid request_id"}), 400
    otp = str(otp).strip()

    try:
        conn = get_sql_connection(OTP_CENTER_UNIT)
        if not conn:
            return jsonify({"success": False, "error": "Unable to connect to OTP database"}), 500
        cursor = conn.cursor()

        # Debug: check row exists and show status/otp (server-side log only)
        try:
            cursor.execute("SELECT OtpPlain, Status FROM DiscountApprovalRequest WHERE Id = ?", request_id)
            dbg_row = cursor.fetchone()
            if not dbg_row:
                return jsonify({"success": False, "message": "Request not found"}), 404
            print(f"Validate OTP debug: id={request_id}, db_otp={dbg_row.OtpPlain}, status={dbg_row.Status}")
        except Exception as e:
            print(f"Validate OTP pre-check failed for id={request_id}: {e}")

        cursor.execute("""
            DECLARE @valid BIT, @msg NVARCHAR(200),
                    @bill VARCHAR(50), @rtype VARCHAR(20),
                    @amt DECIMAL(18,2);

            EXEC dbo.usp_ValidateDiscountOtp
                @RequestId=?, @EnteredOtp=?,
                @IsValid=@valid OUTPUT, @Message=@msg OUTPUT,
                @BillNoOut=@bill OUTPUT, @RequestTypeOut=@rtype OUTPUT,
                @RequestedAmtOut=@amt OUTPUT;

            SELECT @valid AS Valid, @msg AS Message,
                   @bill AS BillNo, @rtype AS ReqType, @amt AS Amount;
        """, (request_id, otp))

        row = cursor.fetchone()
        conn.close()

        if not row.Valid:
            return jsonify({"success": False, "message": row.Message}), 400

        return jsonify({
            "success": True,
            "bill_no": row.BillNo,
            "request_type": row.ReqType,
            "amount_approved": float(row.Amount)
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# =====================================================================================
# 3️⃣ APPLY DISCOUNT (In correct unit server)
# =====================================================================================
@otp_bp.route("/api/billing/apply_discount", methods=["POST"])
def apply_discount():
    data = request.json

    unit            = data.get("unit")
    bill_no         = data.get("bill_no")
    discount_amount = data.get("discount_amount")
    force           = bool(data.get("force"))
    request_id      = data.get("request_id")
    username        = data.get("user")

    try:
        # --------------------------
        # Step 1: Apply discount inside correct server with due checks
        # --------------------------
        from modules import data_fetch
        result = data_fetch.apply_bill_discount(
            (unit or "").strip().upper(),
            data.get("bill_id"),
            bill_no,
            discount_amount,
            force=force
        )
        if result.get("warning"):
            return jsonify({"success": False, "warning": True, **result}), 409
        if result.get("error"):
            return jsonify({"success": False, "error": result["error"]}), 500

        # --------------------------
        # Step 2: Mark OTP request as USED in Prodoc2021
        # --------------------------
        main_conn = get_sql_connection(OTP_CENTER_UNIT)
        if not main_conn:
            return jsonify({"success": False, "error": "Unable to connect to OTP database"}), 500
        mc = main_conn.cursor()

        mc.execute("""
            UPDATE DiscountApprovalRequest
            SET Status='USED', AppliedAt=GETDATE(), AppliedByUserName=?
            WHERE Id = ?;
        """, (username, request_id))

        main_conn.commit()
        main_conn.close()

        return jsonify({"success": True})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =====================================================================================
# 5 APPLY RECEIPT CANCELLATION (Central table only)
# =====================================================================================
@otp_bp.route("/api/receipts/apply_cancellation", methods=["POST"])
def apply_receipt_cancellation():
    """
    Simple cancellation path for receipts: mark the OTP request as USED in the central table.
    Also mark the receipt cancelled in the target unit's Receipt_mst.
    """
    data = request.json

    unit       = (data or {}).get("unit")
    receipt_no = (data or {}).get("receipt_no")
    request_id = data.get("request_id")
    username   = data.get("user")

    missing = []
    if request_id in (None, "", " "):
        missing.append("request_id")
    if not receipt_no:
        missing.append("receipt_no")
    if not unit:
        missing.append("unit")
    if missing:
        return jsonify({"success": False, "error": f"Missing fields: {', '.join(missing)}"}), 400

    try:
        # Apply cancellation in the target unit
        from modules import data_fetch
        result = data_fetch.apply_receipt_cancellation((unit or "").strip().upper(), receipt_no, username)
        if not result.get("success"):
            return jsonify({"success": False, "error": result.get("error", "Receipt cancellation failed")}), 500

        main_conn = get_sql_connection(OTP_CENTER_UNIT)
        if not main_conn:
            return jsonify({"success": False, "error": "Unable to connect to OTP database"}), 500
        mc = main_conn.cursor()

        mc.execute("""
            UPDATE DiscountApprovalRequest
            SET Status='USED', AppliedAt=GETDATE(), AppliedByUserName=ISNULL(?, AppliedByUserName)
            WHERE Id = ?;
        """, (username, request_id))

        main_conn.commit()
        main_conn.close()

        return jsonify({"success": True})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# =====================================================================================
# 4️⃣ APPLY CANCELLATION (In correct unit server)
# =====================================================================================
@otp_bp.route("/api/billing/apply_cancellation", methods=["POST"])
def apply_cancellation():
    data = request.json

    unit       = data.get("unit")
    bill_no    = data.get("bill_no")
    request_id = data.get("request_id")
    username   = data.get("user")

    try:
        # --------------------------
        # Step 1: Apply cancellation inside correct server
        # --------------------------
        conn = get_sql_connection((unit or "").strip().upper())
        if not conn:
            return jsonify({"success": False, "error": f"Unable to connect to unit {unit}"}), 500
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE Billing_Mst
            SET BillStatus='CANCELLED',
                CancelledAt=GETDATE(),
                CancelledBy=?
            WHERE BillNo = ?;
        """, (username, bill_no))

        conn.commit()
        conn.close()

        # --------------------------
        # Step 2: Mark OTP request as USED in Prodoc2021
        # --------------------------
        main_conn = get_sql_connection(OTP_CENTER_UNIT)
        if not main_conn:
            return jsonify({"success": False, "error": "Unable to connect to OTP database"}), 500
        mc = main_conn.cursor()

        mc.execute("""
            UPDATE DiscountApprovalRequest
            SET Status='USED', AppliedAt=GETDATE(), AppliedByUserName=?
            WHERE Id = ?;
        """, (username, request_id))

        main_conn.commit()
        main_conn.close()

        return jsonify({"success": True})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
