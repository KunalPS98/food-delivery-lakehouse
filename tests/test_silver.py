from chispa import assert_df_equality

from food_pipeline import silver


def test_trim_strings_removes_spaces_and_leaves_other_types(spark):
    src = spark.createDataFrame([(" Negative ", 22), ("Positive", 30)], "feedback string, age int")
    expected = spark.createDataFrame([("Negative", 22), ("Positive", 30)], "feedback string, age int")
    assert_df_equality(silver.trim_strings(src), expected, ignore_nullable=True)


def test_standardise_categories_fixes_known_typos_only(spark):
    src = spark.createDataFrame([("Self Employeed",), ("House wife",), ("Student",)], "occupation string")
    expected = spark.createDataFrame([("Self Employed",), ("Homemaker",), ("Student",)], "occupation string")
    assert_df_equality(silver.standardise_categories(src), expected, ignore_nullable=True)


def test_cast_types_turns_bad_numbers_into_null(spark):
    src = spark.createDataFrame(
        [("25", "3", "12.97", "77.59", "560001"), ("abc", "3", "12.97", "77.59", "560001")],
        "age string, family_size string, latitude string, longitude string, pin_code string",
    )
    out = silver.cast_types(src)
    assert dict(out.dtypes) == {"age": "int", "family_size": "int", "latitude": "double",
                                "longitude": "double", "pin_code": "string"}
    assert [r["age"] for r in out.collect()] == [25, None]


def test_map_income_maps_every_band_and_unknown_to_null(spark):
    labels = list(silver.INCOME_BANDS) + ["Unknown band"]
    src = spark.createDataFrame([(label,) for label in labels], "monthly_income string")
    out = {r["monthly_income"]: (r["income_band_order"], r["income_min_inr"], r["income_max_inr"])
           for r in silver.map_income(src).collect()}
    assert out == {**silver.INCOME_BANDS, "Unknown band": (None, None, None)}


def test_clean_outcome_maps_yes_no_and_flags_anything_else_as_null(spark):
    src = spark.createDataFrame([("Yes",), ("No",), ("Maybe",)], "output string")
    out = silver.clean_outcome(src)
    assert "output" not in out.columns
    assert [r["ordered_again"] for r in out.collect()] == [True, False, None]


def test_record_hash_ignores_lineage_but_not_answers(make_rows):
    df = make_rows({}, {"_batch_id": "batch_02"}, {"age": 24})
    hashes = [r["record_hash"] for r in silver.add_record_hash(df).collect()]
    assert hashes[0] == hashes[1], "same answers from a different batch must hash the same"
    assert hashes[0] != hashes[2], "different answers must hash differently"
    assert len(hashes[0]) == 64


def test_record_hash_distinguishes_null_positions(make_rows):
    df = make_rows({"gender": None, "marital_status": "Single"}, {"gender": "Single", "marital_status": None})
    hashes = [r["record_hash"] for r in silver.add_record_hash(df).collect()]
    assert hashes[0] != hashes[1]


def test_dedupe_keeps_one_row_per_hash(make_rows):
    df = silver.add_record_hash(make_rows({}, {}, {"age": 30}))
    assert silver.dedupe(df).count() == 2


def test_merge_into_silver_is_idempotent(spark, make_rows, tmp_path):
    target = (tmp_path / "silver").as_posix()
    batch = silver.add_record_hash(make_rows({}, {"age": 30}))

    silver.merge_into_silver(spark, batch, target)
    silver.merge_into_silver(spark, batch, target)

    assert spark.read.format("delta").load(target).count() == 2
