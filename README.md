# Food Delivery Lakehouse

## Data
Source: Online Food Ordering survey dataset (Kaggle, Bengaluru), 388 rows.

The original file is a single snapshot. To practise incremental loading, it is split into
two simulated landing batches by `scripts/make_landing_batches.py`:
`batch_01.csv` (rows 1–200) and `batch_02.csv` (rows 201–388).
