# Data dictionary

All tables are Delta tables under `lakehouse/` (paths set in `config/pipeline.yaml`).
Columns starting with `_` are pipeline metadata, not survey answers.

## Bronze · `bronze/survey_raw`

Raw survey rows exactly as they arrived, one row per CSV line (duplicates included). **Grain:** one landing-file row.
All 14 survey columns are `string`; names are snake_case by position because Delta doesn't allow spaces.

| Column | Type | Source column | Meaning |
|---|---|---|---|
| `age` | string | Age | Respondent's age in years |
| `gender` | string | Gender | `Male` / `Female` |
| `marital_status` | string | Marital Status | `Single` / `Married` / `Prefer not to say` |
| `occupation` | string | Occupation | Raw label, including source typos |
| `monthly_income` | string | Monthly Income | Income band as text |
| `educational_qualifications` | string | Educational Qualifications | Highest education |
| `family_size` | string | Family size | Number of people in the household |
| `customer_type` | string | Customer Type | `New` / `Regular` / `Frequent` |
| `latitude` | string | latitude | Location latitude |
| `longitude` | string | longitude | Location longitude |
| `pin_code` | string | Pin code | Indian postal code |
| `output` | string | Output | `Yes` / `No`, undefined in the source |
| `feedback` | string | Feedback | `Positive` / `Negative ` (raw, with trailing space) |
| `raw_extra` | string | *(unnamed 14th column)* | Copy of `Output` caused by a trailing comma in the header |
| `_ingest_ts` | timestamp | — | When the batch was loaded (one value per load) |
| `_source_file` | string | — | Full path of the landing file |
| `_batch_id` | string | — | Batch name, from the landing file name (`batch_01`) |

## Silver · `silver/survey`

Cleaned, typed and validated. **Grain:** one distinct set of answers (`record_hash`).

| Column | Type | Meaning / allowed values |
|---|---|---|
| `age` | int | 18–80 (data: 18–33) |
| `gender` | string | `Male`, `Female` |
| `marital_status` | string | `Single`, `Married`, `Prefer not to say` |
| `occupation` | string | `Student`, `Employee`, `Self Employed`, `Homemaker` |
| `monthly_income` | string | `No Income`, `Below Rs.10000`, `10001 to 25000`, `25001 to 50000`, `More than 50000` |
| `educational_qualifications` | string | `School`, `Graduate`, `Post Graduate`, `Ph.D`, `Uneducated` |
| `family_size` | int | 1–15 (data: 1–6) |
| `customer_type` | string | `New`, `Regular`, `Frequent` |
| `latitude` | double | Inside 12.7–13.3 (Bengaluru) |
| `longitude` | double | Inside 77.3–77.9 (Bengaluru) |
| `pin_code` | string | `560` followed by 3 digits; kept as text because it is an identifier |
| `feedback` | string | `Positive`, `Negative` (trimmed) |
| `_ingest_ts`, `_source_file`, `_batch_id` | — | Lineage of the **first** batch these answers arrived in |
| `income_band_order` | int | 0 = No Income … 4 = More than 50000; use for sorting and range filters |
| `income_min_inr` | int | Lower bound of the band in rupees |
| `income_max_inr` | int | Upper bound of the band; null for `More than 50000` (no upper bound) |
| `ordered_again` | boolean | From `Output`: Yes → true, No → false. Assumed to mean *would order again* |
| `record_hash` | string | SHA-256 of the 13 cleaned answers; the Silver key |

## Quarantine · `quarantine/survey`

Rows that failed at least one DQ rule. Same columns as Silver before de-duplication (including `raw_extra`,
without `record_hash`), plus:

| Column | Type | Meaning |
|---|---|---|
| `failed_rules` | array&lt;string&gt; | Names of every rule the row broke (see `dq_rules()` in `src/food_pipeline/quality.py`) |
| `_dq_checked_ts` | timestamp | When the rules were run |

## Gold

Rebuilt in full from Silver on every run.

### `gold/dim_location`
**Grain:** one pin code.

| Column | Type | Meaning |
|---|---|---|
| `location_key` | bigint | Surrogate key: `xxhash64(pin_code)`, stable across rebuilds |
| `pin_code` | string | Natural key |
| `latitude`, `longitude` | double | Coordinates of the pin code |

### `gold/fact_survey_response`
**Grain:** one respondent (`record_hash`). Joins to `dim_location` on `location_key`.

| Column | Type | Meaning |
|---|---|---|
| `record_hash` | string | Respondent key, from Silver |
| `location_key` | bigint | Foreign key to `dim_location` |
| `age_band` | string | `18-22`, `23-26`, `27-30`, `31+` |
| `age`, `gender`, `marital_status`, `occupation`, `monthly_income`, `income_band_order`, `income_min_inr`, `income_max_inr`, `educational_qualifications`, `family_size`, `customer_type`, `feedback`, `ordered_again`, `_batch_id` | — | As in Silver |

### `gold/agg_order_rate_by_segment`
**Grain:** one value of one segment (long format).

| Column | Type | Meaning |
|---|---|---|
| `segment_type` | string | Column grouped by: `occupation`, `monthly_income`, `customer_type`, `age_band`, `gender`, `marital_status`, `educational_qualifications` |
| `segment_value` | string | Value of that column |
| `segment_order` | int | Sort order where it matters (income band 0–4), otherwise null |
| `respondents` | bigint | Respondents in the segment |
| `ordered_again_yes` | bigint | Respondents with `ordered_again = true` |
| `order_rate` | double | `ordered_again_yes / respondents`, 3 decimals |
| `positive_feedback_rate` | double | Share with `feedback = Positive`, 3 decimals |
| `is_small_sample` | boolean | True when `respondents < 20`: treat the rates as unreliable |

### `gold/agg_area_summary`
**Grain:** one pin code. Same metrics as above, plus `location_key`, `pin_code`, `latitude`, `longitude`.
