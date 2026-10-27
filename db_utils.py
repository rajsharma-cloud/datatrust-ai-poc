"""
PostgreSQL helper functions: connection, running SQL files, reading and writing Spark
DataFrames, and the audit log (batch control + error log).
"""
import json
import signal
import traceback
from contextlib import contextmanager
from datetime import datetime

import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

import config

JDBC_URL = f"jdbc:postgresql://{config.PG_HOST}:{config.PG_PORT}/{config.PG_DATABASE}"
JDBC_PROPS = {"user": config.PG_USER, "password": config.PG_PASSWORD, "driver": "org.postgresql.Driver"}


# ----------------------------------------------------------------------------- connection
def get_connection() -> psycopg.Connection:
    """Open a connection. Nothing is saved until conn.commit() (one transaction)."""
    return psycopg.connect(host=config.PG_HOST, port=config.PG_PORT, dbname=config.PG_DATABASE,
                           user=config.PG_USER, password=config.PG_PASSWORD)


def run_sql_file(file_name: str) -> None:
    """Run a .sql file from the sql folder."""
    sql = (config.PROJECT_DIR / "sql" / file_name).read_text(encoding="utf-8")
    with get_connection() as conn:
        conn.execute(sql)
        conn.commit()
    print(f"ran sql/{file_name}")


def table_exists(conn, table: str) -> bool:
    return conn.execute("SELECT to_regclass(%s) IS NOT NULL", (table,)).fetchone()[0]


# ----------------------------------------------------------------------------- Spark <-> PostgreSQL
def pg_type(spark_type) -> str:
    """Spark data type -> PostgreSQL column type."""
    if isinstance(spark_type, T.DecimalType):
        return f"NUMERIC({spark_type.precision},{spark_type.scale})"
    return {
        T.StringType: "TEXT", T.IntegerType: "INTEGER", T.LongType: "BIGINT", T.DoubleType: "DOUBLE PRECISION",
        T.BooleanType: "BOOLEAN", T.DateType: "DATE", T.TimestampNTZType: "TIMESTAMP", T.TimestampType: "TIMESTAMP",
    }.get(type(spark_type), "TEXT")


def create_table_like_df(conn, table: str, df: DataFrame) -> None:
    """CREATE TABLE IF NOT EXISTS with the DataFrame's columns.
    If the table exists but the DataFrame has new columns (source added a column = schema drift),
    the new columns are added, so bronze never rejects a file because of a new column."""
    if not table_exists(conn, table):
        cols = ", ".join(f"{f.name} {pg_type(f.dataType)}" for f in df.schema.fields)
        conn.execute(f"CREATE TABLE {table} ({cols})")
        return
    schema, name = table.split(".")
    existing = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
        (schema, name)).fetchall()}
    for f in df.schema.fields:
        if f.name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {f.name} {pg_type(f.dataType)}")
            print(f"  schema drift: new column {f.name} added to {table}")


def write_df(df: DataFrame, table: str, conn) -> int:
    """Append a Spark DataFrame to a PostgreSQL table and return the row count.

    WRITE_MODE=python : PostgreSQL COPY on `conn` -> part of the caller's transaction.
    WRITE_MODE=jdbc   : Spark JDBC writer (its own connection, commits by itself).
    """
    if config.WRITE_MODE == "jdbc":
        df.write.jdbc(JDBC_URL, table, mode="append", properties=JDBC_PROPS)
        return df.count()

    columns = ", ".join(df.columns)
    rows = 0
    with conn.cursor() as cur:
        with cur.copy(f"COPY {table} ({columns}) FROM STDIN") as copy:
            for row in df.toLocalIterator():
                copy.write_row(tuple(row))
                rows += 1
    return rows


def write_staging(df: DataFrame, table: str, conn) -> int:
    """(Re)create a staging table from the DataFrame and load it.
    Spark JDBC cannot MERGE, so silver rows go to a staging table first and SQL merges them."""
    if config.WRITE_MODE == "jdbc":
        df.write.jdbc(JDBC_URL, table, mode="overwrite", properties=JDBC_PROPS)
        return df.count()
    conn.execute(f"DROP TABLE IF EXISTS {table}")
    create_table_like_df(conn, table, df)
    return write_df(df, table, conn)


def read_df(spark: SparkSession, query: str) -> DataFrame:
    """Run a query on PostgreSQL and return the result as a Spark DataFrame."""
    if config.WRITE_MODE == "jdbc":
        return (spark.read.option("preferTimestampNTZ", "true")
                .jdbc(JDBC_URL, f"({query}) AS q", properties=JDBC_PROPS))

    # PostgreSQL type id -> Spark type (anything else is read as text)
    type_map = {16: T.BooleanType(), 20: T.LongType(), 21: T.IntegerType(), 23: T.IntegerType(),
                700: T.DoubleType(), 701: T.DoubleType(), 1082: T.DateType(), 1114: T.TimestampNTZType(),
                1700: T.DecimalType(38, 6)}
    with get_connection() as conn:
        cur = conn.execute(query)
        fields = [T.StructField(d.name, type_map.get(d.type_code, T.StringType())) for d in cur.description]
        text_cols = [i for i, f in enumerate(fields) if isinstance(f.dataType, T.StringType)]
        data = [tuple(str(v) if (i in text_cols and v is not None) else v for i, v in enumerate(row))
                for row in cur.fetchall()]
    return spark.createDataFrame(data, T.StructType(fields))


# ----------------------------------------------------------------------------- audit log
# Logging must never stop the pipeline and must never lose an error:
#   * every error goes to audit.error_log (own connection, so it survives a ROLLBACK)
#   * it is ALWAYS also appended to logs/pipeline_errors.log (works even when PostgreSQL is down)
#   * if writing to the database fails, the reason is added to the file as well
ERROR_FILE = config.PROJECT_DIR / "logs" / "pipeline_errors.log"

# Set when the user presses Ctrl+C. PySpark turns Ctrl+C inside a Spark call into a Py4JError,
# so this flag is how we still know the run was CANCELLED (not FAILED) and must stop.
STOP_REQUESTED = False


def install_stop_handler() -> None:
    """Call after the Spark session is created (Spark installs its own Ctrl+C handler)."""
    original = signal.getsignal(signal.SIGINT)

    def on_ctrl_c(signum, frame):
        global STOP_REQUESTED
        STOP_REQUESTED = True
        if callable(original):
            original(signum, frame)                      # Spark: cancel running jobs, raise KeyboardInterrupt
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, on_ctrl_c)


def is_cancelled(error: BaseException) -> bool:
    return isinstance(error, KeyboardInterrupt) or STOP_REQUESTED


def stop_after_logging(error: BaseException) -> None:
    """After a cancel was logged: stop the whole run (raise a KeyboardInterrupt marked as logged)."""
    if is_cancelled(error):
        stop = KeyboardInterrupt("run cancelled")
        stop._datatrust_logged = True
        raise stop from error


def _write_error_file(record: dict) -> None:
    try:
        ERROR_FILE.parent.mkdir(exist_ok=True)
        with open(ERROR_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")
    except Exception as file_error:                      # last resort: the console
        print(f"!! could not write {ERROR_FILE}: {file_error}\n{record}")


def log_error(batch_id, business_date, layer, pipeline_id, error, status="FAILED") -> None:
    """Store an error with batch ID, pipeline ID, SQL error code and full stack trace. Never raises."""
    record = {
        "logged_at": datetime.now().isoformat(timespec="seconds"),
        "batch_id": batch_id, "business_date": business_date, "layer": layer, "pipeline_id": pipeline_id,
        "status": status, "error_type": type(error).__name__,
        "sql_state": getattr(error, "sqlstate", None),               # PostgreSQL error code, e.g. 42703
        "error_message": str(error)[:4000],
        "stack_trace": "".join(traceback.format_exception(type(error), error, error.__traceback__))[-8000:],
    }
    try:
        error._datatrust_logged = True                   # so an outer handler does not log it twice
    except Exception:
        pass
    print(f"  ERROR logged: {layer} / {pipeline_id}: {record['error_type']}: {record['error_message'][:150]}")
    _write_error_file(record)
    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO audit.error_log (batch_id, business_date, layer, pipeline_id, status, error_type,
                       sql_state, error_message, stack_trace)
                VALUES (%(batch_id)s, %(business_date)s, %(layer)s, %(pipeline_id)s, %(status)s, %(error_type)s,
                        %(sql_state)s, %(error_message)s, %(stack_trace)s)""", record)
            conn.commit()
    except Exception as db_error:
        _write_error_file({"logged_at": record["logged_at"], "note": "could not write audit.error_log",
                           "db_error": str(db_error)[:1000], "for_pipeline_id": pipeline_id})


def log_warning(batch_id, business_date, layer, pipeline_id, issue_type, message) -> None:
    """Data quality issue (the run continues): rejected rows, duplicates, reconciliation mismatch.
    Stored in audit.error_log with status WARNING. Never raises."""
    record = {
        "logged_at": datetime.now().isoformat(timespec="seconds"),
        "batch_id": batch_id, "business_date": business_date, "layer": layer, "pipeline_id": pipeline_id,
        "status": "WARNING", "error_type": issue_type, "sql_state": None,
        "error_message": str(message)[:4000], "stack_trace": None,
    }
    print(f"  WARNING logged: {layer} / {pipeline_id}: {issue_type}: {record['error_message'][:150]}")
    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO audit.error_log (batch_id, business_date, layer, pipeline_id, status, error_type,
                       sql_state, error_message, stack_trace)
                VALUES (%(batch_id)s, %(business_date)s, %(layer)s, %(pipeline_id)s, %(status)s, %(error_type)s,
                        %(sql_state)s, %(error_message)s, %(stack_trace)s)""", record)
            conn.commit()
    except Exception as db_error:
        _write_error_file({**record, "note": "could not write audit.error_log", "db_error": str(db_error)[:1000]})


def log_step(batch_id, business_date, layer, status, started_at, load_mode=None,
             rows_read=None, rows_written=None, rows_rejected=None, message=None) -> None:
    """One row per batch and layer in audit.batch_control. Never raises (a failure is itself logged)."""
    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO audit.batch_control (batch_id, business_date, layer, status, load_mode,
                       rows_read, rows_written, rows_rejected, message, started_at, ended_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (batch_id, business_date, layer, status, load_mode, rows_read, rows_written, rows_rejected,
                 message, started_at, datetime.now()))
            conn.commit()
    except Exception as e:
        log_error(batch_id, business_date, layer, f"PL_{layer}_LOG_STEP", e)


def log_pipeline_step(batch_id, business_date, layer, pipeline_id, status, started_at, rows_in=None,
                      rows_out=None, rows_rejected=None, message=None, error=None) -> None:
    """One row in audit.step_log for a step (SUCCESS / FAILED / CANCELLED / SKIPPED). Never raises.
    Own connection, so the row is kept even when the layer's transaction is rolled back."""
    ended_at = datetime.now()
    record = {
        "batch_id": batch_id, "business_date": business_date, "layer": layer, "pipeline_id": pipeline_id,
        "status": status, "rows_in": rows_in, "rows_out": rows_out, "rows_rejected": rows_rejected,
        "message": None if message is None else str(message)[:2000],
        "error_type": type(error).__name__ if error is not None else None,
        "error_message": str(error)[:4000] if error is not None else None,
        "started_at": started_at, "ended_at": ended_at,
        "duration_sec": round((ended_at - started_at).total_seconds(), 2),
    }
    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO audit.step_log (batch_id, business_date, layer, pipeline_id, status, rows_in, rows_out,
                       rows_rejected, message, error_type, error_message, started_at, ended_at, duration_sec)
                VALUES (%(batch_id)s, %(business_date)s, %(layer)s, %(pipeline_id)s, %(status)s, %(rows_in)s,
                        %(rows_out)s, %(rows_rejected)s, %(message)s, %(error_type)s, %(error_message)s,
                        %(started_at)s, %(ended_at)s, %(duration_sec)s)""", record)
            conn.commit()
    except Exception as db_error:
        _write_error_file({**record, "note": "could not write audit.step_log", "db_error": str(db_error)[:1000]})


class StepInfo(dict):
    """Filled in by the step: info['rows_in'] = ..., info['message'] = ..."""


@contextmanager
def track_step(batch_id, business_date, layer: str, step_pipeline_id: str):
    """Log one step to audit.step_log: SUCCESS when the block finishes, FAILED / CANCELLED (with the exact
    error message) when it raises. The error is raised again, so the layer still rolls back and
    writes the full stack trace to audit.error_log.

        with db_utils.track_step(batch_id, day, "BRONZE", "PL_BRONZE_PAYMENTS") as info:
            info["rows_out"] = write(...)
    """
    started_at = datetime.now()
    info = StepInfo()
    try:
        yield info
    except BaseException as e:
        status = "CANCELLED" if is_cancelled(e) else "FAILED"
        log_pipeline_step(batch_id, business_date, layer, step_pipeline_id, status, started_at,
                          info.get("rows_in"), info.get("rows_out"), info.get("rows_rejected"),
                          info.get("message"), error=e)
        raise
    log_pipeline_step(batch_id, business_date, layer, step_pipeline_id, info.get("status", "SUCCESS"), started_at,
                      info.get("rows_in"), info.get("rows_out"), info.get("rows_rejected"), info.get("message"))


def layer_succeeded(batch_id: str, layer: str) -> bool:
    """Layer gating: did this layer finish successfully for this batch? (errors are raised to the caller)"""
    with get_connection() as conn:
        return conn.execute("""SELECT COUNT(*) > 0 FROM audit.batch_control
                               WHERE batch_id = %s AND layer = %s AND status = 'SUCCESS'""",
                            (batch_id, layer)).fetchone()[0]


def latest_batch(layer: str):
    """(batch_id, business_date) of the most recent run of a layer (any status), or None.
    If that run failed, the next layer is SKIPPED by gating (it never silently uses an older batch)."""
    with get_connection() as conn:
        row = conn.execute("""SELECT batch_id, business_date::text FROM audit.batch_control
                              WHERE layer = %s ORDER BY ended_at DESC LIMIT 1""", (layer,)).fetchone()
    return (row[0], row[1]) if row else None


def batch_date(batch_id: str):
    """business_date of a batch (None = full load batch)."""
    with get_connection() as conn:
        row = conn.execute("SELECT business_date::text FROM audit.batch_control WHERE batch_id = %s LIMIT 1",
                           (batch_id,)).fetchone()
    return row[0] if row else None


def pipeline_id(layer: str, name: str = "") -> str:
    """PL_BRONZE_PAYMENTS, PL_SILVER_CUSTOMERS, PL_GOLD_VIEWS ..."""
    name = name.replace(".csv", "").upper()
    return f"PL_{layer.upper()}" + (f"_{name}" if name else "")


@contextmanager
def capture_errors(layer: str, pipeline_id: str, batch_id=None, business_date=None):
    """Wrap any code: an error that escapes it is logged (once) and then raised again.

        with capture_errors("RUN", "PL_RUN_START"):
            spark = get_spark()
    """
    try:
        yield
    except SystemExit:
        raise                                            # normal exit / usage message: not an error
    except BaseException as e:
        if not getattr(e, "_datatrust_logged", False):
            log_error(batch_id, business_date, layer, pipeline_id, e, "CANCELLED" if is_cancelled(e) else "FAILED")
        raise