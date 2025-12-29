# CREATE INDEX

Creates a scalar index on a Lance table to accelerate queries.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

The `CREATE INDEX` command builds an index on one or more columns of a Lance table. Indexing can improve the performance of queries that filter on the indexed columns. This operation is performed in a distributed manner, building indexes for each data fragment in parallel.

## Basic Usage

The command uses the `ALTER TABLE` syntax to add an index.

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX user_id_idx USING btree (id);
    ```

## Options

The `CREATE INDEX` command supports options via the `WITH` clause to control index creation. These options are specific to the chosen index method.

For the `btree` method, the following options are supported:

| Option      | Type | Description                                  |
|-------------|------|----------------------------------------------|
| `zone_size` | Long | The number of rows per zone in the B-tree index. |

## Examples

### Basic Index Creation

Create a simple B-tree index on a single column:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX idx_id USING btree (id);
    ```

### Indexing Multiple Columns

Create a composite index on multiple columns.

=== "SQL"
    ```sql
    ALTER TABLE lance.db.logs CREATE INDEX idx_ts_level USING btree (timestamp, level);
    ```

### Indexing with Options

Create an index and specify the `zone_size` for the B-tree:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX idx_id_zoned USING btree (id) WITH (zone_size = 2048);
    ```

## Output

The `CREATE INDEX` command returns the following information about the operation:

| Column              | Type   | Description                            |
|---------------------|--------|----------------------------------------|
| `fragments_indexed` | Long   | The number of fragments that were indexed. |
| `index_name`        | String | The name of the created index.         |

## When to Use an Index

Consider creating an index when:

- You frequently filter a large table on a specific column.
- Your queries involve point lookups or small range scans.

## How It Works

The `CREATE INDEX` command operates as follows:

1.  **Distributed Index Building**: For each fragment in the Lance dataset, a separate task is launched to build an index on the specified column(s).
2.  **Metadata Merging**: Once all per-fragment indexes are built, their metadata is collected and merged.
3.  **Transactional Commit**: A new table version is committed with the new index information. The operation is atomic and ensures that concurrent reads are not affected.

## Notes and Limitations

- **Index Method**: Currently, only the `btree` method is supported for scalar index creation.
- **Index Replacement**: If you create an index with the same name as an existing one, the old index will be replaced by the new one. This is because the underlying implementation uses `replace(true)`.
