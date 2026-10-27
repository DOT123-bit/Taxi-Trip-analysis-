"""Spark (PySpark) engine: reads, cleans and analyses the taxi trip data."""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

NAME = "Apache Spark (PySpark)"
REQUIRED = ["trip_id", "pickup_datetime", "dropoff_datetime", "pickup_zone", "dropoff_zone",
            "passenger_count", "distance_km", "fare_amount", "payment_type"]
DOW = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}  # Spark dayofweek: 1 = Sunday

_spark = None


def get_spark():
    global _spark
    if _spark is None:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        _spark = (
            SparkSession.builder.appName("TaxiTripAnalytics")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "512m")
            .config("spark.sql.ansi.enabled", "false")  # bad values become NULL instead of errors
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def load_and_clean(path):
    """Returns (cleaned_spark_df, stats_dict)."""
    spark = get_spark()
    raw = spark.read.option("header", True).csv(path)          # everything as string first
    raw = raw.toDF(*[c.strip().lower() for c in raw.columns])
    missing = [c for c in REQUIRED if c not in raw.columns]
    if missing:
        raise ValueError("CSV is missing columns: " + ", ".join(missing))

    rows_in = raw.count()
    df = raw.select(
        F.col("trip_id"),
        F.to_timestamp(F.trim("pickup_datetime")).alias("pickup_ts"),
        F.to_timestamp(F.trim("dropoff_datetime")).alias("dropoff_ts"),
        F.initcap(F.trim("pickup_zone")).alias("pickup_zone"),
        F.initcap(F.trim("dropoff_zone")).alias("dropoff_zone"),
        F.trim("passenger_count").cast("double").alias("passenger_count"),
        F.trim("distance_km").cast("double").alias("distance_km"),
        F.trim("fare_amount").cast("double").alias("fare_amount"),
        F.trim("payment_type").alias("payment_type"),
    )
    df = df.withColumn(
        "duration_min", (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0
    ).withColumn("speed_kmph", F.col("distance_km") / F.col("duration_min") * 60.0)

    df = df.filter(
        F.col("pickup_ts").isNotNull() & F.col("dropoff_ts").isNotNull()
        & F.col("pickup_zone").isNotNull() & (F.col("pickup_zone") != "")
        & F.col("dropoff_zone").isNotNull() & (F.col("dropoff_zone") != "")
        & F.col("payment_type").isNotNull() & (F.col("payment_type") != "")
        & F.col("passenger_count").between(1, 6)
        & (F.col("distance_km") > 0) & (F.col("distance_km") <= 200)
        & (F.col("fare_amount") > 0) & (F.col("fare_amount") <= 50000)
        & (F.col("duration_min") >= 1) & (F.col("duration_min") <= 300)
        & (F.col("speed_kmph") <= 120)
    )
    df = (df.withColumn("date", F.to_date("pickup_ts"))
            .withColumn("hour", F.hour("pickup_ts"))
            .withColumn("dow", F.dayofweek("pickup_ts"))
            .cache())
    rows_out = df.count()
    return df, {"rows_in": rows_in, "rows_out": rows_out, "rows_dropped": rows_in - rows_out}


def get_options(df):
    zones = sorted(r[0] for r in df.select("pickup_zone").distinct().collect())
    pays = sorted(r[0] for r in df.select("payment_type").distinct().collect())
    b = df.agg(F.min("date"), F.max("date")).collect()[0]
    return zones, pays, b[0], b[1]


def analyze(df, start, end, zones, pays, hmin, hmax):
    f = df.filter(
        (F.col("date") >= F.lit(start)) & (F.col("date") <= F.lit(end))
        & F.col("pickup_zone").isin(zones) & F.col("payment_type").isin(pays)
        & F.col("hour").between(hmin, hmax)
    )
    k = f.agg(
        F.count("*").alias("trips"), F.sum("fare_amount").alias("revenue"),
        F.avg("fare_amount").alias("avg_fare"), F.avg("distance_km").alias("avg_distance"),
        F.avg("duration_min").alias("avg_duration"),
        F.sum("distance_km").alias("sum_km"), F.sum("duration_min").alias("sum_min"),
    ).collect()[0]
    trips = int(k["trips"] or 0)
    kpi = {"trips": trips,
           "revenue": float(k["revenue"] or 0),
           "avg_fare": float(k["avg_fare"] or 0),
           "avg_distance": float(k["avg_distance"] or 0),
           "avg_duration": float(k["avg_duration"] or 0),
           "avg_speed": float(k["sum_km"] / k["sum_min"] * 60) if trips and k["sum_min"] else 0.0}

    by_hour = (f.groupBy("hour").agg(F.count("*").alias("trips"), F.sum("fare_amount").alias("revenue"),
                                     F.avg("fare_amount").alias("avg_fare")).orderBy("hour").toPandas())
    by_zone = (f.groupBy("pickup_zone").agg(F.count("*").alias("trips"), F.sum("fare_amount").alias("revenue"),
                                            F.avg("fare_amount").alias("avg_fare"),
                                            F.avg("distance_km").alias("avg_distance"))
               .orderBy(F.desc("trips")).toPandas())
    routes = (f.groupBy("pickup_zone", "dropoff_zone")
              .agg(F.count("*").alias("trips"), F.avg("fare_amount").alias("avg_fare"),
                   F.avg("distance_km").alias("avg_distance"))
              .withColumn("route", F.concat_ws(" → ", "pickup_zone", "dropoff_zone"))
              .orderBy(F.desc("trips")).limit(10)
              .select("route", "trips", "avg_fare", "avg_distance").toPandas())
    by_payment = (f.groupBy("payment_type").agg(F.count("*").alias("trips"), F.sum("fare_amount").alias("revenue"))
                  .orderBy(F.desc("trips")).toPandas())
    by_date = (f.groupBy("date").agg(F.count("*").alias("trips"), F.sum("fare_amount").alias("revenue"))
               .orderBy("date").toPandas())
    heat = f.groupBy("dow", "hour").agg(F.count("*").alias("trips")).toPandas()
    heat["day"] = heat["dow"].map(DOW)
    heat = heat[["day", "hour", "trips"]]
    return {"kpi": kpi, "by_hour": by_hour, "by_zone": by_zone, "routes": routes,
            "by_payment": by_payment, "by_date": by_date, "heat": heat}
