from cow_bff.heatmap import calculate_time_overlap, compute_heatmap
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from pandas.testing import assert_frame_equal
import pytest
import datetime

@pytest.fixture(scope="session")
def spark_session():
    global spark
    try:
        spark
    except NameError:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()

    yield spark


@pytest.fixture(scope="session")
def cow_bff(spark_session):
    schema = StructType([ \
        StructField("cow_name", StringType(),True), \
        StructField("meal_start", IntegerType(),True), \
        StructField("meal_end", IntegerType(),True), \
        StructField("date", DateType(), True) \
    ])
    day = 86400
    date1 = datetime.datetime.fromtimestamp(1684540800)
    date2 = datetime.datetime.fromtimestamp(1684540800 + day)
    df = spark_session.createDataFrame([
        ("cow1", 1, 2, date1),
        ("cow2", 1, 3, date1),
        ("cow3", 0, 3, date1),
        ("cow1", 2, 4, date2),
        ("cow2", 5, 7, date2),
        ("cow3", 5, 8, date2),
    ], schema)
    yield df


def test_calculate_time_overlap():
    assert calculate_time_overlap(0, 10, 20, 30) == 0
    assert calculate_time_overlap(0, 20, 10, 30) == 10
    assert calculate_time_overlap(0, 20, 5, 15) == 10


def test_compute_heatmap(spark_session, cow_bff):
    result = compute_heatmap(cow_bff)
    schema = StructType([ \
        StructField("cow1", StringType(),True), \
        StructField("cow2", StringType(),True), \
        StructField("distance", DoubleType(),True)
    ])
    expected = spark_session.createDataFrame([
        ("cow1", "cow2", 0.5),
        ("cow1", "cow3", 0.5),
        ("cow2", "cow1", 0.5),
        ("cow2", "cow3", 2.0),
        ("cow3", "cow1", 0.5),
        ("cow3", "cow2", 2.0),        
    ], schema)
    assert_frame_equal(result.toPandas(), expected.toPandas())
