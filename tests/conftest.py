import pytest

try:
    from pyspark.sql import SparkSession

    @pytest.fixture(scope="session")
    def spark():
        session = (
            SparkSession.builder.appName("test")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        yield session
        session.stop()

except ImportError:
    pass
