from food_pipeline import quality


def failed_rules(make_rows, **override):
    _, quarantine = quality.split_valid(make_rows(override))
    rows = quarantine.collect()
    return rows[0]["failed_rules"] if rows else []


def test_valid_row_passes_and_loses_raw_extra(make_rows):
    valid, quarantine = quality.split_valid(make_rows({}))
    assert valid.count() == 1
    assert quarantine.count() == 0
    assert "raw_extra" not in valid.columns


def test_bad_age_is_quarantined_with_its_rule(make_rows):
    assert failed_rules(make_rows, age=150) == ["age_18_to_80"]


def test_row_outside_bengaluru_fails_every_location_rule(make_rows):
    assert failed_rules(make_rows, pin_code="400001", latitude=19.07, longitude=72.87) == [
        "pin_code_bengaluru", "latitude_in_bengaluru", "longitude_in_bengaluru"]


def test_null_counts_as_a_failure_not_a_pass(make_rows):
    assert failed_rules(make_rows, feedback=None) == ["feedback_allowed"]
    assert failed_rules(make_rows, age=None) == ["age_18_to_80"]


def test_unexpected_category_is_quarantined(make_rows):
    assert failed_rules(make_rows, occupation="Self-Employed") == ["occupation_allowed"]


def test_raw_extra_must_match_ordered_again(make_rows):
    assert failed_rules(make_rows, raw_extra="No") == ["raw_extra_matches_output"]


def test_failure_counts_reports_every_rule_including_zeros(make_rows):
    counts = quality.failure_counts(make_rows({}, {"age": 150}, {"age": 99, "feedback": None}))
    assert counts["age_18_to_80"] == 2
    assert counts["feedback_allowed"] == 1
    assert counts["gender_allowed"] == 0
    assert len(counts) == len(quality.dq_rules())
