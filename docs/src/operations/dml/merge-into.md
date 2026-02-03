# MERGE INTO

Currently, merge into only supports for Spark 3.5+.

Lance fully supports Spark's MERGE INTO operation. For specific usage, please refer to Spark's relevant documentation.

```
MERGE INTO customers c
USING new_updates u
ON c.id = u.id
WHEN MATCHED THEN
UPDATE SET c.status = u.new_status, c.last_seen = u.timestamp;
```

Additionally, `lance-spark` introduces a column rewrite mode for `MERGE INTO` operation, which can significantly improve performance for narrow updates that only affect a few columns.

## Column Rewrite Mode

This mode allows the Lance data source to perform column-level updates by writing new data files for only the modified columns, avoiding the need to rewrite the entire data file (the "delete and insert" pattern).

!!! warning "Spark Extension Required"
This feature requires the Lance Spark SQL extension to be enabled.
See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

### Configuration

You can enable or disable this feature using the Spark SQL session configuration `spark.sql.lance.rewrite_columns`.

**Using SQL:**

To enable the feature for the current session:
```sql
SET spark.sql.lance.rewrite_columns = true;
```

To disable it:
```sql
SET spark.sql.lance.rewrite_columns = false;
```

### Behavior and Semantics

- When `spark.sql.lance.rewrite_columns` is set to `true`, `MERGE INTO ... WHEN MATCHED UPDATE` operation will attempt to perform column-level updates. Instead of deleting the matched rows and inserting new versions, the engine will only write new versions of the changed columns.
- When the configuration is set to `false` (the default behavior), the operation fall back to rewriting the affected rows (a "delete and insert" operation).

### Examples

**MERGE INTO with RewriteColumns**

When enabled, the `UPDATE` clause in a `MERGE INTO` statement will benefit from this optimization.

```sql
-- Enable column rewrite mode
SET spark.sql.lance.rewrite_columns = true;

-- This will update the 'status' and 'last_seen' columns without rewriting the whole row
MERGE INTO customers c
USING new_updates u
ON c.id = u.id
WHEN MATCHED THEN
  UPDATE SET c.status = u.new_status, c.last_seen = u.timestamp;
```

### Notes

- **Spark Version**: `MERGE INTO` operation are supported on Spark 3.5 and newer.
- **Nested Fields**: Updating nested fields follows the existing semantics of `MERGE INTO`. The entire top-level column containing the nested field will be rewritten.
- **Troubleshooting**: If you encounter any issues or unexpected behavior with this feature, you can disable it by setting `spark.sql.lance.rewrite_columns` to `false` to revert to the row-rewrite behavior.
