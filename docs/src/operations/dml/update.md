# UPDATE SET

Currently, update only supports for Spark 3.5+.

Update with condition:

```sql
UPDATE users 
SET name = 'Updated Name' 
WHERE id = 4;
```

Update with complex data types:

```sql
UPDATE events 
SET metadata = named_struct('source', 'ios', 'version', 1, 'processed_at', timestamp'2024-01-15 13:00:00') 
WHERE event_id = 1001;
```

Update struct's field:

```sql
UPDATE events 
SET metadata = named_struct('source', metadata.source, 'version', 2, 'processed_at', timestamp'2024-01-15 13:00:00') 
WHERE event_id = 1001;
```

Update array field:

```sql
UPDATE events
SET tags = ARRAY('ios', 'mobile')
WHERE event_id = 1001;
```

## Column Rewrite Mode

`lance-spark` introduces a column rewrite mode for `UPDATE` operations, which can significantly improve performance for narrow updates that only affect a few columns.

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

- When `spark.sql.lance.rewrite_columns` is set to `true`, `UPDATE` operations will attempt to perform column-level updates. Instead of deleting the matched rows and inserting new versions, the engine will only write new versions of the changed columns.
- When the configuration is set to `false` (the default behavior), the operations fall back to rewriting the affected rows (a "delete and insert" operation).

### Examples

**UPDATE with RewriteColumns**

Here is an example of enabling the mode and performing an `UPDATE`.

```sql
-- Enable column rewrite mode
SET spark.sql.lance.rewrite_columns = true;

-- Assume 'users' table has columns: id, name, address
-- This operation will only write new data for the 'name' column
UPDATE users
SET name = 'New User Name'
WHERE id > 100;
```

### Notes

- **Spark Version**: `UPDATE` operations are supported on Spark 3.5 and newer.
- **Nested Fields**: Updating nested fields follows the existing semantics of `UPDATE`. The entire top-level column containing the nested field will be rewritten.
- **Troubleshooting**: If you encounter any issues or unexpected behavior with this feature, you can disable it by setting `spark.sql.lance.rewrite_columns` to `false` to revert to the row-rewrite behavior.
