Completed milestone

**Silver layer with PySpark**

- [x] Convert timestamps into proper timestamp types.
- [x] Convert prices and payment values into decimals.
- [x] Normalize empty strings to nulls and preserve optional missing values.
- [x] Remove exact duplicate records.
- [x] Validate primary and foreign keys.
- [x] Write cleaned Parquet datasets.

The daily Bronze DAG now starts this Silver job after all nine Bronze tasks
succeed. Validation failures stop the batch rather than silently dropping
records.
