"""
SILVER layer: clean, typed, de-duplicated data built from ONE bronze batch.

For every entity:
  1. read this batch's rows from bronze
  2. PySpark: cast types (bad values become NULL instead of failing), trim/standardise,
     keep the latest row per business key (removes duplicates)
  3. PySpark: data quality rules -> failing rows go to silver.rejected_records with the rule name
  4. PostgreSQL: good rows -> staging table -> MERGE into silver
        SCD1 (payments, orders, refunds, settlements, categories): insert new, update existing
        SCD2 (customers, products): changed record -> old version closed, new version inserted

All entities are saved in ONE transaction: COMMIT at the end, ROLLBACK on any error.
Silver runs only if bronze of the same batch succeeded.

Run alone:  python silver_layer.py <batch_id>
"""
import sys
from datetime import datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import db_utils
from spark_utils import get_spark


# =============================================================================== small helpers
def to_decimal(col: str, precision: int = 14, scale: int = 2):
    return F.expr(f"try_cast({col} AS DECIMAL({precision},{scale}))")       # 'abc' -> NULL, no failure


def to_int(col: str):
    return F.expr(f"try_cast({col} AS INT)")


def to_date(col: str):
    return F.expr(f"try_cast({col} AS DATE)")


def to_timestamp(col: str):
    # handles '2026-10-01 07:00:07' and '2026-10-12T13:58:35.000Z'
    return F.expr(f"try_to_timestamp(trim(regexp_replace({col}, '[TZ]', ' ')))")


def clean_text(col: str, upper: bool = False):
    c = F.trim(F.col(col))
    c = F.when(c == "", None).otherwise(c)                                   # '' -> NULL
    return F.upper(c) if upper else c


def keep_latest(df: DataFrame, key: str, order_by: str) -> DataFrame:
    """Remove duplicates: keep one row per key (the latest by order_by)."""
    w = Window.partitionBy(key).orderBy(F.col(order_by).desc_nulls_last(), F.col("ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def apply_rules(df: DataFrame, entity: str, key: str, rules: dict, raw_cols: list):
    """rules = {rule_name: condition that marks a BAD row}. First failing rule wins.
    Returns (good rows, rejected rows)."""
    failed = F.lit(None)
    for rule_name, is_bad in reversed(list(rules.items())):
        failed = F.when(is_bad, F.lit(rule_name)).otherwise(failed)
    checked = df.withColumn("_failed_rule", failed)

    good = checked.filter("_failed_rule IS NULL").drop("_failed_rule")
    rejected = checked.filter("_failed_rule IS NOT NULL").select(
        F.lit(entity).alias("entity"),
        F.col(key).cast("string").alias("record_key"),
        F.col("business_date"),
        F.col("_failed_rule").alias("rule_name"),
        F.to_json(F.struct(*[F.col(f"raw_{c}").alias(c) for c in raw_cols])).alias("raw_record"),
        F.col("batch_id"),
        F.localtimestamp().alias("rejected_at"))
    return good, rejected


def add_record_hash(df: DataFrame, columns: list) -> DataFrame:
    """SCD2: a hash of the tracked columns. Different hash = the record changed."""
    return df.withColumn("record_hash", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit(""))
                                                                    for c in columns]), 256))


def with_raw_copy(df: DataFrame) -> DataFrame:
    """Keep the original bronze values (raw_*) so a rejected row can be stored exactly as received."""
    return df.select("*", *[F.col(c).alias(f"raw_{c}") for c in df.columns])


# =============================================================================== transformations (PySpark)
def transform_payments(bronze: DataFrame, business_date: str):
    raw_cols = bronze.columns
    df = with_raw_copy(bronze).select(
        clean_text("payment_id").alias("payment_id"),
        clean_text("order_id").alias("order_id"),
        clean_text("customer_id").alias("customer_id"),
        to_date("business_date").alias("business_date"),
        to_timestamp("created_at").alias("created_at"),
        to_timestamp("captured_at").alias("captured_at"),
        to_decimal("amount").alias("amount"),
        clean_text("currency", upper=True).alias("currency"),
        clean_text("status", upper=True).alias("status"),
        clean_text("method", upper=True).alias("method"),
        clean_text("card_network").alias("card_network"),
        clean_text("bank").alias("bank"),
        clean_text("wallet").alias("wallet"),
        F.lower(clean_text("email")).alias("email"),
        F.concat(F.lit("******"), F.substring(clean_text("contact"), -4, 4)).alias("contact_masked"),   # PII
        to_decimal("fee").alias("fee"),
        to_decimal("tax").alias("tax"),
        to_decimal("amount_refunded").alias("amount_refunded"),
        clean_text("error_code").alias("error_code"),
        clean_text("error_reason").alias("error_reason"),
        "batch_id", "ingested_at", *[f"raw_{c}" for c in raw_cols])

    df = keep_latest(df, "payment_id", "created_at")
    good, rejected = apply_rules(df, "payments", "payment_id", {
        "PAYMENT_ID_REQUIRED": F.col("payment_id").isNull(),
        "CURRENCY_REQUIRED":   F.col("currency").isNull(),
        "AMOUNT_POSITIVE":     F.col("amount").isNull() | (F.col("amount") <= 0),
        "STATUS_VALID":        F.col("status").isNull() | ~F.col("status").isin("CAPTURED", "FAILED", "REFUNDED", "AUTHORIZED"),
    }, raw_cols)
    return finish(good), rejected


def transform_payment_orders(bronze: DataFrame, business_date: str):
    raw_cols = bronze.columns
    df = with_raw_copy(bronze).select(
        clean_text("order_id").alias("order_id"),
        clean_text("receipt").alias("receipt"),
        clean_text("customer_id").alias("customer_id"),
        to_date("business_date").alias("business_date"),
        to_timestamp("created_at").alias("created_at"),
        to_decimal("amount").alias("amount"),
        to_decimal("amount_paid").alias("amount_paid"),
        to_decimal("amount_due").alias("amount_due"),
        clean_text("currency", upper=True).alias("currency"),
        clean_text("status", upper=True).alias("payment_status"),
        to_int("attempts").alias("attempts"),
        clean_text("channel", upper=True).alias("channel"),
        clean_text("order_status", upper=True).alias("order_status"),
        "batch_id", "ingested_at", *[f"raw_{c}" for c in raw_cols])

    df = keep_latest(df, "order_id", "created_at")
    good, rejected = apply_rules(df, "payment_orders", "order_id", {
        "ORDER_ID_REQUIRED": F.col("order_id").isNull(),
        "CURRENCY_REQUIRED": F.col("currency").isNull(),
        "AMOUNT_POSITIVE":   F.col("amount").isNull() | (F.col("amount") <= 0),
    }, raw_cols)
    return finish(good), rejected


def transform_refunds(bronze: DataFrame, business_date: str):
    raw_cols = bronze.columns
    df = with_raw_copy(bronze).select(
        clean_text("refund_id").alias("refund_id"),
        clean_text("payment_id").alias("payment_id"),
        clean_text("order_id").alias("order_id"),
        to_date("business_date").alias("business_date"),
        to_timestamp("created_at").alias("created_at"),
        to_timestamp("processed_at").alias("processed_at"),
        to_decimal("amount").alias("amount"),
        clean_text("currency", upper=True).alias("currency"),
        clean_text("status", upper=True).alias("status"),
        clean_text("speed").alias("speed"),
        clean_text("reason", upper=True).alias("reason"),
        "batch_id", "ingested_at", *[f"raw_{c}" for c in raw_cols])

    df = keep_latest(df, "refund_id", "created_at")
    good, rejected = apply_rules(df, "refunds", "refund_id", {
        "REFUND_ID_REQUIRED": F.col("refund_id").isNull(),
        "AMOUNT_POSITIVE":    F.col("amount").isNull() | (F.col("amount") <= 0),
    }, raw_cols)
    return finish(good), rejected


def transform_settlements(bronze: DataFrame, business_date: str):
    raw_cols = bronze.columns
    df = with_raw_copy(bronze).select(
        clean_text("settlement_id").alias("settlement_id"),
        to_date("business_date").alias("business_date"),
        to_timestamp("settled_at").alias("settled_at"),
        to_int("payments_count").alias("payments_count"),
        to_decimal("gross_amount").alias("gross_amount"),
        to_decimal("refunds").alias("refunds"),
        to_decimal("fees").alias("fees"),
        to_decimal("tax").alias("tax"),
        to_decimal("net_settled_amount").alias("net_settled_amount"),
        clean_text("currency", upper=True).alias("currency"),
        clean_text("status", upper=True).alias("status"),
        clean_text("utr").alias("utr"),
        "batch_id", "ingested_at", *[f"raw_{c}" for c in raw_cols])

    df = keep_latest(df, "settlement_id", "settled_at")
    good, rejected = apply_rules(df, "settlements", "settlement_id", {
        "SETTLEMENT_ID_REQUIRED": F.col("settlement_id").isNull(),
        "AMOUNT_VALID":           F.col("gross_amount").isNull(),
    }, raw_cols)
    return finish(good), rejected


def transform_customers(bronze: DataFrame, business_date: str):
    """Customer change events (CDC) -> latest change per customer -> SCD2."""
    raw_cols = bronze.columns
    df = with_raw_copy(bronze).select(
        clean_text("customer_id").alias("customer_id"),
        clean_text("customer_name").alias("customer_name"),
        F.lower(clean_text("email")).alias("email"),
        clean_text("city").alias("city"),
        clean_text("state").alias("state"),
        clean_text("region", upper=True).alias("region"),
        clean_text("segment", upper=True).alias("segment"),
        to_timestamp("changed_at").alias("valid_from"),
        to_date("business_date").alias("business_date"),
        "batch_id", "ingested_at", *[f"raw_{c}" for c in raw_cols])

    df = keep_latest(df, "customer_id", "valid_from")
    good, rejected = apply_rules(df, "customers", "customer_id", {
        "CUSTOMER_ID_REQUIRED": F.col("customer_id").isNull(),
        "EMAIL_VALID":          F.col("email").isNull() | ~F.col("email").contains("@"),
        "CHANGE_TIME_REQUIRED": F.col("valid_from").isNull(),
    }, raw_cols)
    good = add_record_hash(good, ["customer_name", "email", "city", "state", "region", "segment"])
    return finish(good.drop("business_date")), rejected


def transform_products(bronze: DataFrame, business_date: str):
    """Product catalog snapshot (Platzi API) -> SCD2 (price / title / category changes kept as history)."""
    raw_cols = bronze.columns
    df = with_raw_copy(bronze).select(
        to_int("id").alias("product_id"),
        clean_text("title").alias("title"),
        clean_text("slug").alias("slug"),
        to_decimal("price", 12, 2).alias("price"),
        to_int("category_id").alias("category_id"),
        clean_text("image_1").alias("image_url"),
        to_timestamp("updated_at").alias("valid_from"),
        F.lit(business_date).cast("date").alias("business_date"),
        "batch_id", "ingested_at", *[f"raw_{c}" for c in raw_cols])

    df = keep_latest(df, "product_id", "valid_from")
    good, rejected = apply_rules(df, "products", "product_id", {
        "PRODUCT_ID_REQUIRED": F.col("product_id").isNull(),
        "PRICE_POSITIVE":      F.col("price").isNull() | (F.col("price") <= 0),
    }, raw_cols)
    good = add_record_hash(good, ["title", "slug", "price", "category_id", "image_url"])
    return finish(good.drop("business_date")), rejected


def transform_categories(bronze: DataFrame, business_date: str):
    raw_cols = bronze.columns
    df = with_raw_copy(bronze).select(
        to_int("id").alias("category_id"),
        clean_text("name").alias("category_name"),
        clean_text("slug").alias("slug"),
        clean_text("image").alias("image_url"),
        to_timestamp("updated_at").alias("updated_at"),
        F.lit(business_date).cast("date").alias("business_date"),
        "batch_id", "ingested_at", *[f"raw_{c}" for c in raw_cols])

    df = keep_latest(df, "category_id", "updated_at")
    good, rejected = apply_rules(df, "categories", "category_id", {
        "CATEGORY_ID_REQUIRED": F.col("category_id").isNull(),
    }, raw_cols)
    return finish(good.drop("business_date")), rejected


def finish(good: DataFrame) -> DataFrame:
    """Drop helper columns and stamp the load time."""
    helper = [c for c in good.columns if c.startswith("raw_")] + ["ingested_at"]
    return good.drop(*helper).withColumn("loaded_at", F.localtimestamp())


# =============================================================================== load into silver (SQL)
def merge_scd1(conn, target: str, staging: str, key: str, columns: list) -> int:
    """Insert new keys, update existing ones (latest version only)."""
    updates = ", ".join(f"{c} = s.{c}" for c in columns if c != key)
    sql = f"""
        MERGE INTO {target} t
        USING {staging} s ON t.{key} = s.{key}
        WHEN MATCHED THEN UPDATE SET {updates}
        WHEN NOT MATCHED THEN INSERT ({", ".join(columns)}) VALUES ({", ".join("s." + c for c in columns)})"""
    return conn.execute(sql).rowcount


def merge_scd2(conn, target: str, staging: str, key: str, columns: list):
    """SCD Type 2.
    Step 1: a key whose tracked columns changed -> close the current version (valid_to, is_current = FALSE).
    Step 2: every key without a current version (new key, or just closed) -> insert the new current version."""
    closed = conn.execute(f"""
        UPDATE {target} t
        SET    valid_to = s.valid_from, is_current = FALSE
        FROM   {staging} s
        WHERE  t.{key} = s.{key}
          AND  t.is_current
          AND  t.record_hash <> s.record_hash
          AND  s.valid_from > t.valid_from""").rowcount

    inserted = conn.execute(f"""
        INSERT INTO {target} ({", ".join(columns)}, valid_to, is_current)
        SELECT {", ".join("s." + c for c in columns)}, NULL, TRUE
        FROM   {staging} s
        WHERE  NOT EXISTS (SELECT 1 FROM {target} t WHERE t.{key} = s.{key} AND t.is_current)""").rowcount
    return closed, inserted


# entity: (bronze table, transform function, silver table, business key, SCD type)
ENTITIES = [
    ("categories",     "bronze.categories",     transform_categories,     "silver.categories",     "category_id",   1),
    ("products",       "bronze.products",       transform_products,       "silver.products",       "product_id",    2),
    ("customers",      "bronze.customers",      transform_customers,      "silver.customers",      "customer_id",   2),
    ("payment_orders", "bronze.payment_orders", transform_payment_orders, "silver.payment_orders", "order_id",      1),
    ("payments",       "bronze.payments",       transform_payments,       "silver.payments",       "payment_id",    1),
    ("refunds",        "bronze.refunds",        transform_refunds,        "silver.refunds",        "refund_id",     1),
    ("settlements",    "bronze.settlements",    transform_settlements,    "silver.settlements",    "settlement_id", 1),
]


def run_silver(spark, batch_id: str, business_date: str = None) -> bool:
    """business_date=None -> the batch is a full load (all dates)."""
    started_at = datetime.now()
    rows_read = rows_written = rows_rejected = 0
    current = db_utils.pipeline_id("SILVER", "GATE")
    conn = None
    try:                                                                  # EVERYTHING inside, incl. gating
        def step(name):                                                   # every step -> audit.step_log
            nonlocal current
            current = db_utils.pipeline_id("SILVER", name)
            return db_utils.track_step(batch_id, business_date, "SILVER", current)

        with step("GATE") as info:
            bronze_ok = db_utils.layer_succeeded(batch_id, "BRONZE")      # layer gating
            info.update(status="SUCCESS" if bronze_ok else "SKIPPED",
                        message="bronze succeeded for this batch" if bronze_ok
                        else "SKIPPED: bronze did not succeed for this batch")
        if not bronze_ok:
            db_utils.log_step(batch_id, business_date, "SILVER", "SKIPPED", started_at,
                              message="bronze did not succeed for this batch")
            print("  silver  SKIPPED (bronze did not succeed)")
            return False

        with step("CONNECT") as info:
            conn = db_utils.get_connection()
            info["message"] = "connected"
        duplicates = []                                                   # (entity, removed, read) for the log
        for name, bronze_table, transform, silver_table, key, scd in ENTITIES:
            with step(name) as info:
                bronze = db_utils.read_df(spark, f"SELECT * FROM {bronze_table} WHERE batch_id = '{batch_id}'")

                # rejected rows of this load are replaced (no duplicates on re-runs)
                if business_date is None:                                     # full load: all dates
                    conn.execute("DELETE FROM silver.rejected_records WHERE entity = %s", (name,))
                else:
                    conn.execute("DELETE FROM silver.rejected_records WHERE entity = %s AND business_date = %s",
                                 (name, business_date))

                # SCD2 on a full load: apply the changes day by day, in order, so the full history is kept
                if scd == 2 and business_date is None and "business_date" in bronze.columns:
                    days = [r[0] for r in bronze.select("business_date").distinct().orderBy("business_date").collect()]
                    parts = [(d, bronze.filter(F.col("business_date") == d)) for d in days]
                else:
                    parts = [(business_date, bronze)]

                n_good = n_rejected = closed = inserted = 0
                staging = f"silver.stg_{name}"
                for part_date, part in parts:
                    good, rejected = transform(part, part_date)
                    good.cache()
                    n_rejected += db_utils.write_df(rejected, "silver.rejected_records", conn)
                    n_good += db_utils.write_staging(good, staging, conn)    # staging -> MERGE -> drop staging
                    if scd == 1:
                        merge_scd1(conn, silver_table, staging, key, good.columns)
                    else:
                        c, i = merge_scd2(conn, silver_table, staging, key, good.columns)
                        closed += c
                        inserted += i
                    conn.execute(f"DROP TABLE {staging}")
                    good.unpersist()
                result = f"{n_good} merged" if scd == 1 else f"SCD2: {closed} closed, {inserted} new versions"

                n_read = bronze.count()
                rows_read += n_read
                rows_written += n_good
                rows_rejected += n_rejected
                dupes = n_read - n_good - n_rejected
                if scd == 2:
                    unchanged = n_good - inserted
                    result += f", {unchanged} unchanged (no new version)" if unchanged else ""
                print(f"  silver  {silver_table:<24} {n_read:>6} read, {result}, {n_rejected} rejected"
                      + (f", {dupes} duplicates removed" if dupes else ""))
                info.update(rows_in=n_read, rows_out=n_good, rows_rejected=n_rejected,
                            message=f"{bronze_table} -> {silver_table}: {result}"
                                    + (f", {dupes} duplicates removed" if dupes else ""))
                if dupes and scd == 1:
                    duplicates.append((name, dupes, n_read))

        with step("COMMIT") as info:
            conn.commit()                                                 # COMMIT all entities together
            info.update(rows_in=rows_read, rows_out=rows_written, rows_rejected=rows_rejected,
                        message=f"{len(ENTITIES)} entities committed")
        db_utils.log_step(batch_id, business_date, "SILVER", "SUCCESS", started_at, rows_read=rows_read,
                          rows_written=rows_written, rows_rejected=rows_rejected)

        with step("DQ_WARNINGS") as info:
            log_quality_warnings(conn, batch_id, business_date, duplicates)
            info["message"] = "data quality warnings written to audit.error_log"
        return True

    except BaseException as e:                                            # also Ctrl+C / stop
        safe_rollback(conn, batch_id, business_date, "SILVER")            # ROLLBACK: silver unchanged
        status = "CANCELLED" if db_utils.is_cancelled(e) else "FAILED"
        db_utils.log_error(batch_id, business_date, "SILVER", current, e, status)
        db_utils.log_step(batch_id, business_date, "SILVER", status, started_at, message=f"{current}: {e}"[:500])
        db_utils.stop_after_logging(e)                      # Ctrl+C: stop the whole run
        return False
    finally:
        if conn is not None:
            conn.close()


def log_quality_warnings(conn, batch_id, business_date, duplicates) -> None:
    """After a successful silver load: one WARNING row in audit.error_log per entity / date / rule
    with rejected rows, and per entity with removed duplicates. Never raises."""
    try:
        rows = conn.execute("""
            SELECT entity, business_date::text, rule_name, COUNT(*), string_agg(record_key, ', ' ORDER BY record_key)
            FROM   silver.rejected_records
            WHERE  batch_id = %s
            GROUP  BY entity, business_date, rule_name
            ORDER  BY entity, business_date, rule_name""", (batch_id,)).fetchall()
        for entity, day, rule, count, keys in rows:
            db_utils.log_warning(batch_id, day, "SILVER", db_utils.pipeline_id("SILVER", entity), "DQ_REJECTED_ROWS",
                                 f"{count} {entity} row(s) rejected by rule {rule}; keys: {keys[:1500]} "
                                 f"(details in silver.rejected_records)")
        for entity, removed, read in duplicates:
            db_utils.log_warning(batch_id, business_date, "SILVER", db_utils.pipeline_id("SILVER", entity),
                                 "DQ_DUPLICATES_REMOVED", f"{removed} duplicate {entity} row(s) removed ({read} read)")
    except Exception as e:
        db_utils.log_error(batch_id, business_date, "SILVER", "PL_SILVER_DQ_WARNINGS", e)


def safe_rollback(conn, batch_id, business_date, layer) -> None:
    """ROLLBACK without ever raising (a rollback problem is logged too)."""
    try:
        if conn is not None and not conn.closed:
            conn.rollback()
    except Exception as e:
        db_utils.log_error(batch_id, business_date, layer, f"PL_{layer}_ROLLBACK", e)


if __name__ == "__main__":
    # python silver_layer.py             -> uses the latest successful BRONZE batch
    # python silver_layer.py <batch_id>  -> uses that batch
    try:
        with db_utils.capture_errors("SILVER", "PL_SILVER_FIND_BATCH"):
            if len(sys.argv) > 1:
                batch, day = sys.argv[1], db_utils.batch_date(sys.argv[1])
            else:
                latest = db_utils.latest_batch("BRONZE")
                if latest is None:
                    raise RuntimeError("no bronze batch found - run python bronze_layer.py first")
                batch, day = latest
        print(f"silver for batch {batch} ({day or 'ALL DATES'})")
        with db_utils.capture_errors("SILVER", "PL_SILVER_SPARK_START", batch, day):
            spark = get_spark("silver")
            db_utils.install_stop_handler()
        ok = run_silver(spark, batch, day)
    except BaseException as e:
        sys.exit(f"silver stopped: {type(e).__name__}: {e}\ndetails: audit.error_log and {db_utils.ERROR_FILE}")
    sys.exit(0 if ok else 1)