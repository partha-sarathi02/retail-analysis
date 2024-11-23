import pytest
from lib.Utils import get_spark_session

@pytest.fixture
def spark():
    "create spark session"
    spark_session=  get_spark_session('LOCAL')
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_result(spark):
    "Gives expected results"
    state_schema= 'state string, count integer'
    return spark.read \
        .format("csv") \
        .schema(state_schema) \
        .load("data/test_results/aggregated_results.csv")