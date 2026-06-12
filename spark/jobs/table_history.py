import sys

from utils import init_spark, CATALOG, NAMESPACE


def main():
    spark = init_spark(app_name="Table-History")

    tables = ["patients", "encounters", "conditions", "observations", "procedures"]

    for t in tables:
        full_name = f"{CATALOG}.{NAMESPACE}.{t}"
        try:
            df = spark.sql(
                f"SELECT committed_at, snapshot_id, operation "
                f"FROM {full_name}.snapshots ORDER BY committed_at DESC"
            )
            count = df.count()
            print(f"\n{t}: ({count} snapshots)")
            if count > 0:
                df.show(truncate=False)
        except Exception as e:
            print(f"\n{t}: Error — {e}")

    spark.stop()


if __name__ == "__main__":
    main()
