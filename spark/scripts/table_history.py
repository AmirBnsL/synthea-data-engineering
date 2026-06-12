from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
for t in ['patients','encounters','conditions','observations','procedures']:
    df = spark.sql(f"SELECT * FROM raw.raw_landing.{t}.snapshots")
    print(f"\n=== {t} snapshots ===")
    df.select("committed_at", "snapshot_id", "parent_id", "operation").show(truncate=False)
