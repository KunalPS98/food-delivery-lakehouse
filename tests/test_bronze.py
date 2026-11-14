from food_pipeline import bronze

HEADER = ("Age,Gender,Marital Status,Occupation,Monthly Income,Educational Qualifications,Family size,"
          "Customer Type,latitude,longitude,Pin code,Output,Feedback,\n")
ROW = "20,Female,Single,Student,No Income,Post Graduate,4,Frequent,12.9766,77.5993,560001,Yes,Positive,Yes\n"


def test_batch_guard_skips_reruns_and_retries_of_the_same_file(spark, tmp_path):
    csv = tmp_path / "batch_01.csv"
    csv.write_text(HEADER + ROW + ROW, encoding="utf-8")
    target = (tmp_path / "bronze").as_posix()

    assert bronze.ingest_batch(spark, csv.as_posix(), "batch_01", target) == 2
    assert bronze.ingest_batch(spark, csv.as_posix(), "batch_01", target) == 0
    assert bronze.ingest_batch(spark, csv.as_posix(), "batch_01_retry", target) == 0

    stored = spark.read.format("delta").load(target)
    assert stored.count() == 2, "Bronze keeps duplicates within a file, but never loads a file twice"
    assert stored.columns[-3:] == ["_ingest_ts", "_source_file", "_batch_id"]
