from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
for t in ['patients','encounters','conditions','observations','procedures']:
    try:
        df = spark.sql(f"SELECT committed_at, snapshot_id, operation FROM raw.raw_landing.{t}.snapshots")
        print(f'{t}:')
        df.show(truncate=False)
    except Exception as e:
        print(f'{t}: {e}')
