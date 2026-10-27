
from pyspark.sql import functions as F

import adls_utils
import config
from db_utils import read_df
from spark_utils import get_spark

spark = get_spark("explore")
print("Spark", spark.version)

#  ADLS: test the connection and list the landing files (needs ADLS settings in .env)
if adls_utils.test_connection():
    for path in adls_utils.list_files():
        print(path)

#  SOURCE: a CSV exactly as delivered
raw = spark.read.option("header", True).csv((config.LOCAL_SOURCE_DIR / "payments.csv").as_uri())
raw.printSchema()
raw.groupBy("BUSINESS_DATE").count().orderBy("BUSINESS_DATE").show()

#  BRONZE: every delivery is kept (FULL first, APPEND after; a re-run adds a new batch)
read_df(spark, """SELECT business_date, batch_id, load_mode, COUNT(*) AS rows
                  FROM bronze.payments GROUP BY 1, 2, 3 ORDER BY 1, 2""").show(truncate=False)

# SILVER: rejected rows and why
read_df(spark, """SELECT entity, business_date, rule_name, COUNT(*) AS rows
                  FROM silver.rejected_records GROUP BY 1, 2, 3""").show(truncate=False)

#  SILVER SCD2: full history of one customer
read_df(spark, """SELECT customer_id, city, segment, valid_from, valid_to, is_current
                  FROM silver.customers WHERE customer_id = 'C00781' ORDER BY valid_from""").show(truncate=False)

#  GOLD: daily revenue KPI (as pandas -> opens nicely in the Data Viewer)
read_df(spark, "SELECT * FROM gold.daily_revenue ORDER BY business_date").toPandas()

# GOLD: gateway settlement vs silver -> where data went missing
read_df(spark, "SELECT * FROM gold.settlement_reconciliation ORDER BY business_date").toPandas()

#  Your own PySpark on silver data: success rate by payment method
payments = read_df(spark, "SELECT * FROM silver.payments")
(payments.groupBy("method")
         .agg(F.count("*").alias("attempts"),
              F.sum(F.when(F.col("status") == "CAPTURED", 1).otherwise(0)).alias("captured"))
         .withColumn("success_rate_pct", F.round(100 * F.col("captured") / F.col("attempts"), 2))
         .orderBy(F.desc("success_rate_pct"))
         .show())

#  Pipeline run log
read_df(spark, "SELECT * FROM gold.pipeline_status ORDER BY business_date").toPandas()

#  Stop Spark when finished
spark.stop()
