"""
BRONZE layer: land the source CSV rows exactly as delivered.

  * One bronze table per source file, all columns TEXT (raw), plus load metadata:
    batch_id, source_file, ingested_at, load_mode.
  * First successful load = FULL  (bronze tables emptied, then loaded)
    Every later load      = APPEND (new rows added with the new batch_id, nothing deleted)
  * FULL LOAD (no business date given): every row of every file is loaded, all dates at once.
  * DAILY LOAD (business date given): daily files are filtered to that date; snapshot files are loaded whole.
  * Everything runs in ONE transaction: if any file fails, nothing of this batch is kept.

Run alone:  python bronze_layer.py               full load (all rows, all dates)
            python bronze_layer.py 2026-10-09    daily load (only that date)
"""
import re
import sys
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import adls_utils
import config
import db_utils
from spark_utils import get_spark


def to_snake_case(name: str) -> str:
    """creationAt -> creation_at, BUSINESS_DATE -> business_date"""
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name.strip()).lower()


def get_source_folder():
    """Local folder with the CSV files. In adls mode the files are first downloaded from ADLS."""
    if config.SOURCE_MODE == "adls":
        print("  downloading source files from ADLS ...")
        return adls_utils.download_folder()
    return config.LOCAL_SOURCE_DIR


def read_source_csv(spark, path, date_column, business_date) -> DataFrame:
    """Read a CSV with every column as text (bronze keeps the raw values)."""
    df = spark.read.option("header", True).option("inferSchema", False).csv(path.as_uri())
    df = df.toDF(*[to_snake_case(c) for c in df.columns])
    if date_column and business_date:                      # daily load -> only this business date
        df = df.filter(F.col(date_column) == business_date)
    return df


def get_load_mode(conn) -> str:
    """FULL if bronze was never loaded successfully, otherwise APPEND."""
    loaded_before = conn.execute(
        "SELECT COUNT(*) > 0 FROM audit.batch_control WHERE layer = 'BRONZE' AND status = 'SUCCESS'").fetchone()[0]
    return "APPEND" if loaded_before else "FULL"


def run_bronze(spark, batch_id: str, business_date: str = None) -> bool:
    """business_date=None -> FULL load of all rows; a date -> daily load (FULL the very first time, then APPEND)."""
    started_at = datetime.now()
    total_rows = 0
    load_mode = None
    current = db_utils.pipeline_id("BRONZE", "START")        # which step is running (goes to the error log)
    conn = None
    try:                                                     # EVERYTHING inside: connection, checks, loads
        def step(name):                                      # every step -> audit.step_log (SUCCESS / FAILED)
            nonlocal current
            current = db_utils.pipeline_id("BRONZE", name)
            return db_utils.track_step(batch_id, business_date, "BRONZE", current)

        with step("CONNECT") as info:
            conn = db_utils.get_connection()
            info["message"] = f"connected to {config.PG_DATABASE}"

        with step("LOAD_MODE") as info:
            load_mode = "FULL" if business_date is None else get_load_mode(conn)
            info["message"] = f"load mode {load_mode} ({'all dates' if business_date is None else business_date})"

        with step("SOURCE_FILES") as info:
            source_folder = get_source_folder()
            info["message"] = f"{config.SOURCE_MODE} source: {source_folder}"

        if load_mode == "FULL":                              # first load: overwrite
            with step("TRUNCATE") as info:
                emptied = []
                for src in config.SOURCES:
                    if db_utils.table_exists(conn, src["bronze"]):
                        conn.execute(f"TRUNCATE TABLE {src['bronze']}")
                        emptied.append(src["bronze"])
                info["message"] = f"emptied {len(emptied)} table(s): {', '.join(emptied) or '-'}"

        for src in config.SOURCES:
            with step(src["file"]) as info:
                df = read_source_csv(spark, source_folder / src["file"], src["date_column"], business_date)
                df = (df.withColumn("batch_id", F.lit(batch_id))
                        .withColumn("source_file", F.lit(src["file"]))
                        .withColumn("ingested_at", F.localtimestamp())
                        .withColumn("load_mode", F.lit(load_mode)))

                db_utils.create_table_like_df(conn, src["bronze"], df)
                if config.WRITE_MODE == "jdbc":
                    conn.commit()                            # Spark's own connection must see the table
                rows = db_utils.write_df(df, src["bronze"], conn)
                total_rows += rows
                info.update(rows_in=rows, rows_out=rows, message=f"{src['file']} -> {src['bronze']} ({load_mode})")
                print(f"  bronze  {src['bronze']:<24} +{rows:>6} rows  ({load_mode})")

        with step("COMMIT") as info:
            conn.commit()                                    # COMMIT: the whole batch at once
            info.update(rows_out=total_rows, message=f"{len(config.SOURCES)} files committed")
        db_utils.log_step(batch_id, business_date, "BRONZE", "SUCCESS", started_at, load_mode,
                          rows_read=total_rows, rows_written=total_rows)
        return True

    except BaseException as e:                               # BaseException: also Ctrl+C / stop
        rollback_and_clean(conn, batch_id)
        status = "CANCELLED" if db_utils.is_cancelled(e) else "FAILED"
        db_utils.log_error(batch_id, business_date, "BRONZE", current, e, status)
        db_utils.log_step(batch_id, business_date, "BRONZE", status, started_at, load_mode,
                          message=f"{current}: {e}"[:500])
        db_utils.stop_after_logging(e)                      # Ctrl+C: stop the whole run
        return False
    finally:
        if conn is not None:
            conn.close()


def rollback_and_clean(conn, batch_id: str) -> None:
    """ROLLBACK: nothing of this batch is kept. Never raises (a cleanup problem is logged, not thrown)."""
    try:
        if conn is not None and not conn.closed:
            conn.rollback()
            if config.WRITE_MODE == "jdbc":                  # JDBC rows were committed by Spark: remove them
                for src in config.SOURCES:
                    if db_utils.table_exists(conn, src["bronze"]):
                        conn.execute(f"DELETE FROM {src['bronze']} WHERE batch_id = %s", (batch_id,))
                conn.commit()
    except Exception as cleanup_error:
        db_utils.log_error(batch_id, None, "BRONZE", "PL_BRONZE_ROLLBACK", cleanup_error)


if __name__ == "__main__":
    day = sys.argv[1] if len(sys.argv) > 1 else None          # no date -> full load of all rows
    label = day.replace("-", "") if day else "FULL"
    batch = f"B{label}_{datetime.now():%H%M%S}"
    try:
        with db_utils.capture_errors("BRONZE", "PL_BRONZE_SPARK_START", batch, day):   # e.g. Java / path problems
            spark = get_spark("bronze")
            db_utils.install_stop_handler()
        ok = run_bronze(spark, batch, day)
    except BaseException as e:
        sys.exit(f"bronze stopped: {type(e).__name__}: {e}\ndetails: audit.error_log and {db_utils.ERROR_FILE}")
    sys.exit(0 if ok else 1)