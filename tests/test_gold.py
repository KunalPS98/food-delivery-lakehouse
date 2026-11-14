import pytest

from food_pipeline import gold


@pytest.mark.parametrize("age, band", [(18, "18-22"), (22, "18-22"), (23, "23-26"), (30, "27-30"), (31, "31+"), (60, "31+")])
def test_add_age_band_boundaries(spark, age, band):
    df = spark.createDataFrame([(age,)], "age int")
    assert gold.add_age_band(df).first()["age_band"] == band


def test_dim_location_has_one_stable_key_per_pin_code(spark):
    silver = spark.createDataFrame(
        [("560001", 12.97, 77.59), ("560001", 12.97, 77.59), ("560002", 12.96, 77.58)],
        "pin_code string, latitude double, longitude double",
    )
    first = {r["pin_code"]: r["location_key"] for r in gold.build_dim_location(silver).collect()}
    second = {r["pin_code"]: r["location_key"] for r in gold.build_dim_location(silver).collect()}
    assert len(first) == 2
    assert first == second, "location_key must not change between rebuilds"


def test_rate_by_counts_respondents_and_rates(spark):
    fact = spark.createDataFrame(
        [("Student", True, "Positive")] * 3 + [("Student", False, "Negative")],
        "occupation string, ordered_again boolean, feedback string",
    )
    row = gold._rate_by(fact, "occupation", None).first()
    assert (row["respondents"], row["ordered_again_yes"], row["order_rate"], row["positive_feedback_rate"]) == (4, 3, 0.75, 0.75)
