import pytest

from food_pipeline.spark_session import get_spark

# A row that passes every Silver DQ rule. Tests copy it and change one field.
VALID_ROW = {
    "age": 23, "gender": "Female", "marital_status": "Single", "occupation": "Student",
    "monthly_income": "No Income", "educational_qualifications": "Graduate", "family_size": 3,
    "customer_type": "Regular", "latitude": 12.97, "longitude": 77.59, "pin_code": "560001",
    "feedback": "Positive", "raw_extra": "Yes", "income_band_order": 0, "ordered_again": True,
    "_batch_id": "batch_01",
}

VALID_ROW_SCHEMA = (
    "age int, gender string, marital_status string, occupation string, monthly_income string, "
    "educational_qualifications string, family_size int, customer_type string, latitude double, "
    "longitude double, pin_code string, feedback string, raw_extra string, income_band_order int, "
    "ordered_again boolean, _batch_id string"
)


@pytest.fixture(scope="session")
def spark():
    """One SparkSession for the whole test run: starting Spark takes seconds, so tests share it."""
    session = get_spark("food-pipeline-tests", master="local[1]", shuffle_partitions=1)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def make_rows(spark):
    """Build a DataFrame of Silver-shaped rows: make_rows({"age": 150}, {}) -> one bad row, one valid row."""
    def _make(*overrides: dict):
        rows = [{**VALID_ROW, **o} for o in overrides]
        return spark.createDataFrame([tuple(r[k] for k in VALID_ROW) for r in rows], VALID_ROW_SCHEMA)
    return _make
