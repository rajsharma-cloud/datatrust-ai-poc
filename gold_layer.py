"""
GOLD layer: simple business views over SILVER (sql/02_gold_views.sql).

Runs only if silver of the same batch succeeded. It (re)creates the views and checks
that gold agrees with silver for the business date before marking the batch as published.

Run alone:  python gold_layer.py <batch_id> <business_date>
"""
import sys
from datetime import datetime

import db_utils
import config


def validate_gold(conn, business_date: str = None) -> str:
    """Gold must agree with silver: captured amount in gold.daily_revenue = sum in silver.payments.
    business_date=None (full load) checks the totals over all dates."""
    if business_date is None:
        gold_amount, silver_amount, days, mismatch_days = conn.execute("""
            SELECT (SELECT SUM(gross_revenue) FROM gold.daily_revenue),
                   (SELECT COALESCE(SUM(amount), 0) FROM silver.payments WHERE status = 'CAPTURED'),
                   (SELECT COUNT(*) FROM gold.daily_revenue),
                   (SELECT COUNT(*) FROM gold.settlement_reconciliation WHERE recon_status = 'MISMATCH')""").fetchone()
        if gold_amount != silver_amount:
            raise ValueError(f"gold revenue {gold_amount} does not match silver {silver_amount}")
        return f"{days} dates, gross revenue {gold_amount}; settlement recon: {mismatch_days} date(s) MISMATCH"

    gold_amount, silver_amount = conn.execute("""
        SELECT (SELECT gross_revenue FROM gold.daily_revenue WHERE business_date = %(d)s),
               (SELECT COALESCE(SUM(amount), 0) FROM silver.payments
                WHERE business_date = %(d)s AND status = 'CAPTURED')""", {"d": business_date}).fetchone()
    if gold_amount is None:
        raise ValueError(f"gold.daily_revenue has no row for {business_date}")
    if gold_amount != silver_amount:
        raise ValueError(f"gold revenue {gold_amount} does not match silver {silver_amount}")

    recon = conn.execute("SELECT recon_status, payment_difference, amount_difference "
                         "FROM gold.settlement_reconciliation WHERE business_date = %s", (business_date,)).fetchone()
    recon_text = f"settlement recon {recon[0]} (payments diff {recon[1]}, amount diff {recon[2]})" if recon else "no settlement"
    return f"gross revenue {gold_amount}; {recon_text}"


def log_recon_warnings(conn, batch_id: str, business_date: str = None) -> None:
    """One WARNING row in audit.error_log per date where the settlement does not match silver. Never raises."""
    try:
        rows = conn.execute("""
            SELECT business_date::text, settled_payments, silver_payments, payment_difference,
                   settled_amount, silver_amount, amount_difference, rejected_payments
            FROM   gold.settlement_reconciliation
            WHERE  recon_status = 'MISMATCH' AND (%(d)s::date IS NULL OR business_date = %(d)s::date)
            ORDER  BY business_date""", {"d": business_date}).fetchall()
        for day, settled, silver, diff, s_amt, p_amt, amt_diff, rejected in rows:
            db_utils.log_warning(batch_id, day, "GOLD", "PL_GOLD_SETTLEMENT_RECON", "DQ_SETTLEMENT_MISMATCH",
                                 f"settlement {settled} payments / {s_amt} vs silver {silver} payments / {p_amt}: "
                                 f"difference {diff} payments, {amt_diff} amount; {rejected} payment(s) rejected in silver")
    except Exception as e:
        db_utils.log_error(batch_id, business_date, "GOLD", "PL_GOLD_SETTLEMENT_RECON", e)


def run_gold(batch_id: str, business_date: str = None) -> bool:
    started_at = datetime.now()
    current = db_utils.pipeline_id("GOLD", "GATE")
    conn = None
    try:                                                                  # EVERYTHING inside, incl. gating
        def step(name):                                                   # every step -> audit.step_log
            nonlocal current
            current = db_utils.pipeline_id("GOLD", name)
            return db_utils.track_step(batch_id, business_date, "GOLD", current)

        with step("GATE") as info:
            silver_ok = db_utils.layer_succeeded(batch_id, "SILVER")      # layer gating
            info.update(status="SUCCESS" if silver_ok else "SKIPPED",
                        message="silver succeeded for this batch" if silver_ok
                        else "SKIPPED: silver did not succeed for this batch")
        if not silver_ok:
            db_utils.log_step(batch_id, business_date, "GOLD", "SKIPPED", started_at,
                              message="silver did not succeed for this batch")
            print("  gold    SKIPPED (silver did not succeed)")
            return False

        with step("VIEWS") as info:
            conn = db_utils.get_connection()
            conn.execute((config.PROJECT_DIR / "sql" / "02_gold_views.sql").read_text(encoding="utf-8"))
            info["message"] = "gold views (re)created from sql/02_gold_views.sql"

        with step("VALIDATE") as info:
            message = validate_gold(conn, business_date)
            conn.commit()
            info["message"] = message
        db_utils.log_step(batch_id, business_date, "GOLD", "SUCCESS", started_at, message=message)
        print(f"  gold    views refreshed: {message}")

        with step("SETTLEMENT_RECON") as info:
            log_recon_warnings(conn, batch_id, business_date)
            info["message"] = "settlement mismatches (if any) written to audit.error_log as WARNING"
        return True

    except BaseException as e:
        try:
            if conn is not None and not conn.closed:
                conn.rollback()
        except Exception as rollback_error:
            db_utils.log_error(batch_id, business_date, "GOLD", "PL_GOLD_ROLLBACK", rollback_error)
        status = "CANCELLED" if db_utils.is_cancelled(e) else "FAILED"
        db_utils.log_error(batch_id, business_date, "GOLD", current, e, status)
        db_utils.log_step(batch_id, business_date, "GOLD", status, started_at, message=f"{current}: {e}"[:500])
        db_utils.stop_after_logging(e)                      # Ctrl+C: stop the whole run
        return False
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    # python gold_layer.py             -> uses the latest successful SILVER batch
    # python gold_layer.py <batch_id>  -> uses that batch
    try:
        with db_utils.capture_errors("GOLD", "PL_GOLD_FIND_BATCH"):
            if len(sys.argv) > 1:
                batch, day = sys.argv[1], db_utils.batch_date(sys.argv[1])
            else:
                latest = db_utils.latest_batch("SILVER")
                if latest is None:
                    raise RuntimeError("no silver batch found - run python silver_layer.py first")
                batch, day = latest
        print(f"gold for batch {batch} ({day or 'ALL DATES'})")
        ok = run_gold(batch, day)
    except BaseException as e:
        sys.exit(f"gold stopped: {type(e).__name__}: {e}\ndetails: audit.error_log and {db_utils.ERROR_FILE}")
    sys.exit(0 if ok else 1)