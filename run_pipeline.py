"""
Run the Medallion pipeline: BRONZE -> SILVER -> GOLD for one or more business dates.
Each layer starts only if the previous layer of the same batch succeeded.
Every error, wherever it happens, is stored in audit.error_log and logs/pipeline_errors.log.

    python run_pipeline.py --setup                    create schemas and tables (once)
    python run_pipeline.py --full                     FULL load: every row of every file (all dates) at once
    python run_pipeline.py 2026-10-01                 one business date
    python run_pipeline.py 2026-10-01 2026-10-08      every date in the range, in order
    python run_pipeline.py --status                   show the run log
    python run_pipeline.py --errors                   show the latest errors and warnings
    python run_pipeline.py --steps [batch_id]         every step of a run (default: latest batch)

Exit code: 0 = every layer succeeded, 1 = something failed (useful for schedulers).
"""
import json
import sys
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path

try:
    import db_utils
    from bronze_layer import run_bronze
    from db_utils import capture_errors
    from gold_layer import run_gold
    from silver_layer import run_silver
    from spark_utils import get_spark
except Exception as import_error:
    # A package is missing or a file is broken: the database logger is not available yet,
    # so write straight to the log file, then stop.
    log_file = Path(__file__).resolve().parent / "logs" / "pipeline_errors.log"
    log_file.parent.mkdir(exist_ok=True)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps({"logged_at": datetime.now().isoformat(timespec="seconds"), "layer": "RUN",
                            "pipeline_id": "PL_RUN_IMPORT", "status": "FAILED",
                            "error_type": type(import_error).__name__, "error_message": str(import_error),
                            "stack_trace": traceback.format_exc()}) + "\n")
    sys.exit(f"cannot start: {type(import_error).__name__}: {import_error}\n"
             f"(did you activate .venv and run pip install -r requirements.txt?)  details: {log_file}")


def run_day(spark, business_date: str = None) -> bool:
    """business_date=None -> full load of all rows (all dates) in one batch."""
    label = business_date.replace("-", "") if business_date else "FULL"
    batch_id = f"B{label}_{datetime.now():%H%M%S}"
    print(f"\n=== {business_date or 'FULL LOAD (all dates)'}   batch {batch_id}")
    with capture_errors("RUN", "PL_RUN_DAY", batch_id, business_date):
        bronze_ok = run_bronze(spark, batch_id, business_date)
        silver_ok = run_silver(spark, batch_id, business_date)      # skips itself if bronze failed
        gold_ok = run_gold(batch_id, business_date)                 # skips itself if silver failed
    return bronze_ok and silver_ok and gold_ok


def show_status() -> None:
    with db_utils.get_connection() as conn:
        rows = conn.execute("""SELECT business_date, batch_id, bronze_load_mode, bronze_status, silver_status,
                                      gold_status, rows_rejected FROM gold.pipeline_status ORDER BY 1""").fetchall()
    print(f"{'date':<12}{'batch':<18}{'mode':<8}{'bronze':<10}{'silver':<10}{'gold':<10}rejected")
    for r in rows:
        print(f"{str(r[0] or 'ALL DATES'):<12}{r[1]:<18}{r[2] or '':<8}{r[3] or '':<10}{r[4] or '':<10}{r[5] or '':<10}{r[6] or 0}")


def show_errors(limit: int = 10) -> None:
    with db_utils.get_connection() as conn:
        rows = conn.execute("""SELECT logged_at, batch_id, layer, pipeline_id, status, error_type, sql_state,
                                      left(error_message, 90)
                               FROM audit.error_log ORDER BY error_id DESC LIMIT %s""", (limit,)).fetchall()
    for r in rows:
        print(f"{r[0]:%Y-%m-%d %H:%M:%S}  {r[1] or '-':<17} {r[2]:<7} {r[3]:<26} {r[4]:<9} {r[5]} {r[6] or ''}\n"
              f"{'':<21}{r[7]}")


def show_steps(batch_id: str = None) -> None:
    with db_utils.get_connection() as conn:
        if batch_id is None:
            row = conn.execute("SELECT batch_id FROM audit.step_log ORDER BY step_id DESC LIMIT 1").fetchone()
            if row is None:
                print("no steps logged yet")
                return
            batch_id = row[0]
        rows = conn.execute("""SELECT layer, pipeline_id, status, rows_in, rows_out, rows_rejected, duration_sec,
                                      COALESCE(error_type || ': ' || error_message, message)
                               FROM audit.step_log WHERE batch_id = %s ORDER BY step_id""", (batch_id,)).fetchall()
    print(f"steps of batch {batch_id}")
    print(f"{'layer':<7} {'pipeline_id':<30} {'status':<10} {'in':>7} {'out':>7} {'rej':>4} {'sec':>6}  message")
    for r in rows:
        print(f"{r[0]:<7} {r[1]:<30} {r[2]:<10} {r[3] if r[3] is not None else '':>7} "
              f"{r[4] if r[4] is not None else '':>7} {r[5] if r[5] is not None else '':>4} {r[6]:>6}  {(r[7] or '')[:110]}")


def parse_dates(args: list):
    start = date.fromisoformat(args[0])                  # a bad date raises ValueError -> logged
    end = date.fromisoformat(args[1]) if len(args) > 1 else start
    if end < start:
        raise ValueError(f"end date {end} is before start date {start}")
    return start, end


def main(args: list) -> int:
    if not args:
        print(__doc__)
        return 0
    if args[0] == "--setup":
        with capture_errors("SETUP", "PL_SETUP"):
            db_utils.run_sql_file("01_create_schemas_and_tables.sql")
            db_utils.run_sql_file("02_gold_views.sql")
        return 0
    if args[0] == "--status":
        with capture_errors("RUN", "PL_STATUS"):
            show_status()
        return 0
    if args[0] == "--steps":
        with capture_errors("RUN", "PL_STEPS"):
            show_steps(args[1] if len(args) > 1 else None)
        return 0
    if args[0] == "--errors":
        with capture_errors("RUN", "PL_ERRORS"):
            show_errors()
        return 0

    full_load = args[0] == "--full"
    if not full_load:
        with capture_errors("RUN", "PL_RUN_ARGUMENTS"):
            start, end = parse_dates(args)
    with capture_errors("RUN", "PL_SPARK_START"):
        spark = get_spark()
        db_utils.install_stop_handler()                 # Ctrl+C -> CANCELLED and the run stops

    all_ok = True
    try:
        if full_load:
            all_ok = run_day(spark, None)
        else:
            day = start
            while day <= end:
                all_ok = run_day(spark, day.isoformat()) and all_ok
                day += timedelta(days=1)
    finally:
        with capture_errors("RUN", "PL_SPARK_STOP"):
            spark.stop()
    print()
    with capture_errors("RUN", "PL_STATUS"):
        show_status()
    return 0 if all_ok else 1


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        print("\nstopped (Ctrl+C) - logged in audit.error_log as CANCELLED")
        sys.exit(130)
    except Exception as e:
        print(f"\npipeline stopped: {type(e).__name__}: {e}\n"
              f"details: audit.error_log and {db_utils.ERROR_FILE}")
        sys.exit(1)