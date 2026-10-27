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
