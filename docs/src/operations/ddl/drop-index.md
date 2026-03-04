# DROP INDEX

Removes a scalar index from a Lance table.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

The `DROP INDEX` command removes an existing index from a Lance table. This operation is useful when an index is no longer needed, or to remove an old index before creating a new one with different parameters.

## Basic Usage

The command uses the `ALTER TABLE` syntax to remove an index.

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users DROP INDEX user_id_idx;
    ```

## Examples

### Dropping an Index

Remove an index named `idx_id` from the `lance.db.users` table:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users DROP INDEX idx_id;
    ```

## Output

The `DROP INDEX` command returns the following information about the operation:

| Column       | Type    | Description                                       |
|--------------|---------|---------------------------------------------------|
| `index_name` | String  | The name of the dropped index.                    |
| `dropped`    | Boolean | A boolean flag indicating if the index was dropped successfully. |

## Behavior

- **Index Not Found**: If you try to drop an index that does not exist, Spark will throw an `IllegalArgumentException`.
- **Transactional Commit**: The index removal is a transactional operation. A new table version is committed without the index information. This process is atomic and ensures that concurrent reads are not affected.

## See Also

- [CREATE INDEX](./create-index.md)
- [SHOW INDEXES](./show-indexes.md)
