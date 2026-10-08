import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder \
        .appName("Pytest-Spark") \
        .master("local[1]") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

def test_spark_session_initialized(spark):
    """Test if Spark session is created successfully."""
    assert spark is not None
    assert spark.sparkContext.appName == "Pytest-Spark"

def test_dummy_ci_pass():
    """Dummy test to ensure CI/CD pipeline runs and passes."""
    assert True
