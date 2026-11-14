# Design decisions

Each entry records a choice, the evidence behind it, and its trade-off.

## 1. Bronze keeps every row; Silver deduplicates

**Context.** The raw file has 388 rows but only 285 distinct answer sets: 103 rows are exact
duplicates (profiled in `notebooks/00_explore.ipynb`). The survey has no respondent ID or timestamp.

**Decision.**
- Bronze stores all 388 rows unchanged. It is the record of what arrived, so Silver can always be
  rebuilt from it if a cleaning rule turns out to be wrong.
- Silver keeps one row per `record_hash`, a SHA-256 of the respondent's cleaned answers
  (`HASH_COLUMNS` in `src/food_pipeline/silver.py`). Lineage columns (`_batch_id`, `_source_file`,
  `_ingest_ts`) and derived income columns are excluded from the hash.
- Silver is written with a Delta `MERGE ... WHEN NOT MATCHED INSERT` on `record_hash`, so reloading a
  batch inserts nothing.

**Trade-off.** Without an ID we can't tell a repeat submission from two people who gave identical
answers. Both are treated as one respondent. With 13 mostly categorical answers from a young,
student-heavy sample, some real respondents are probably merged, so Silver may slightly
under-count people. If a respondent ID becomes available, it should replace `record_hash` as the key.

**Result.** 388 Bronze rows = 285 Silver rows + 103 duplicates + 0 quarantined
(`notebooks/02_silver_transform.ipynb`, reconciliation cell). Rerunning both batches leaves Silver at 285.

## 2. Hash after cleaning, not on raw values

The hash is computed after trimming and standardising, so `"Negative "` and `"Negative"` give the same
key. Nulls are replaced by an explicit `<null>` token before hashing so that `(null, "a")` and
`("a", null)` can't collide.

## 3. snake_case column names already in Bronze

Delta Lake rejects column names containing spaces (`Marital Status`, `Pin code`). Bronze applies
snake_case names **by position** through an explicit all-string schema (`src/food_pipeline/schemas.py`).
Only the names change; every value stays exactly as received. The alternative, Delta column mapping,
allows spaces but adds table-protocol requirements for every reader, which isn't worth it here.

## 4. Bronze batch guard checks batch id *and* source file

A rerun or retry must not load the same data twice. Checking only `_batch_id` would miss a retry of the
same file under a new id (`batch_01_retry`), so Bronze skips a batch when either its id or its source
file is already present. Limitation: the check and the append are separate steps, so this assumes one
pipeline run at a time.

## 5. Quarantine instead of failing, with a threshold

Rows failing a DQ rule go to a quarantine table with the names of the rules they broke, so one bad row
doesn't block the rest and nothing is silently dropped. A condition that evaluates to null counts as a
failure (SQL three-valued logic would otherwise let it pass). If more than 5% of a batch fails
(`quality.max_quarantine_ratio` in `config/pipeline.yaml`), the run stops before Silver: that many bad
rows usually means the source changed, and loading the rest would hide the problem.

## 6. Gold is rebuilt in full

Gold tables are derived entirely from Silver and are small, so every run overwrites them. A full rebuild
is simple, always consistent with Silver and naturally idempotent. At much larger scale Gold would be
updated incrementally instead.

## 7. Stable surrogate key for `dim_location`

`location_key = xxhash64(pin_code)`. A hash gives the same key on every rebuild, unlike
`monotonically_increasing_id()` (changes every run) or `row_number()` over pin codes (shifts when a new
pin code appears). The trade-off is a theoretical hash collision, which the uniqueness check in
`notebooks/03_gold_model.ipynb` would reveal.
