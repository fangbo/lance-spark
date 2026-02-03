/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.update;

import org.lance.spark.utils.SparkUtil;

import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public abstract class BaseMergeIntoTest {
  protected static final int SHUFFLE_PARTITIONS = 4;

  protected SparkSession spark;
  protected TableCatalog catalog;
  protected String catalogName = "lance_ns";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-merge-into-distribution-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .config("spark.sql.shuffle.partitions", String.valueOf(SHUFFLE_PARTITIONS))
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.default.parallelism", String.valueOf(SHUFFLE_PARTITIONS))
            .config("spark.ui.enabled", "false")
            .getOrCreate();

    spark.conf().set(SparkUtil.REWRITE_COLUMNS, "true");

    catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    // Create default namespace for multi-level namespace mode
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".default");
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private List<Row> baseRows() {
    return Arrays.asList(
        Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
        Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
        Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301)));
  }

  @Test
  public void testMergeIntoInsertDistributionOnNullSegmentId() {
    String tableName = "merge_dist_" + UUID.randomUUID().toString().replace("-", "");

    spark.sql(
        "CREATE TABLE "
            + catalogName
            + ".default."
            + tableName
            + " (id INT NOT NULL, value INT, tag STRING)");

    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES "
            + "(1, 10, 'base'), "
            + "(2, 20, 'base'), "
            + "(3, 30, 'base'), "
            + "(4, 40, 'base'), "
            + "(5, 50, 'base'), "
            + "(6, 60, 'base')");

    // Build merge source with update/delete rows plus insert-only rows (null _fragid path).
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1, 110),
                RowFactory.create(2, 120),
                RowFactory.create(3, 130),
                RowFactory.create(4, 140),
                RowFactory.create(5, null),
                RowFactory.create(6, null)),
            new org.apache.spark.sql.types.StructType().add("id", "int").add("value", "int"))
        .union(
            spark
                .range(0, 200)
                .repartition(SHUFFLE_PARTITIONS)
                .selectExpr("cast(id + 1000 as int) as id", "cast(id as int) as value"))
        .createOrReplaceTempView("merge_source");

    // MERGE triggers delete/update/insert branches in a single run.
    spark.sql(
        "MERGE INTO "
            + catalogName
            + ".default."
            + tableName
            + " t USING merge_source s ON t.id = s.id "
            + "WHEN MATCHED AND s.value IS NULL THEN DELETE "
            + "WHEN MATCHED THEN UPDATE SET value = s.value, tag = 'updated' "
            + "WHEN NOT MATCHED THEN INSERT (id, value, tag) VALUES (s.id, s.value, 'inserted')");

    long insertedRowCount =
        spark
            .sql(
                "SELECT COUNT(*) FROM "
                    + catalogName
                    + ".default."
                    + tableName
                    + " WHERE tag = 'inserted'")
            .first()
            .getLong(0);
    Assertions.assertEquals(
        200L, insertedRowCount, "Expected merge to insert 200 rows into new fragments");
    long updatedCount =
        spark
            .sql(
                "SELECT COUNT(*) FROM "
                    + catalogName
                    + ".default."
                    + tableName
                    + " WHERE tag = 'updated'")
            .first()
            .getLong(0);
    Assertions.assertEquals(4L, updatedCount, "Expected 4 updated rows");

    long deletedCount =
        spark
            .sql(
                "SELECT COUNT(*) FROM "
                    + catalogName
                    + ".default."
                    + tableName
                    + " WHERE id IN (5, 6)")
            .first()
            .getLong(0);
    Assertions.assertEquals(0L, deletedCount, "Expected rows 5 and 6 to be deleted");

    // Inserted rows should span multiple fragments to avoid skew.
    long insertFragmentCount =
        spark
            .sql(
                "SELECT COUNT(DISTINCT _fragid) FROM "
                    + catalogName
                    + ".default."
                    + tableName
                    + " WHERE tag = 'inserted'")
            .first()
            .getLong(0);
    Assertions.assertTrue(
        insertFragmentCount >= 2, "Expected inserted rows to span multiple fragments");
  }

  @Test
  public void testMergeInto() {
    String tableName = "merge_result_" + UUID.randomUUID().toString().replace("-", "");

    spark.sql(
        "CREATE TABLE " + catalogName + ".default." + tableName + " (id INT NOT NULL, value INT)");

    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES "
            + "(1, 10), "
            + "(2, 20), "
            + "(3, 30), "
            + "(4, 40), "
            + "(5, 50)");

    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1, 110),
                RowFactory.create(2, 120),
                RowFactory.create(3, null),
                RowFactory.create(100, 1000),
                RowFactory.create(101, 1010)),
            new org.apache.spark.sql.types.StructType().add("id", "int").add("value", "int"))
        .createOrReplaceTempView("merge_result_source");

    spark.sql(
        "MERGE INTO "
            + catalogName
            + ".default."
            + tableName
            + " t USING merge_result_source s ON t.id = s.id "
            + "WHEN MATCHED AND s.value IS NULL THEN DELETE "
            + "WHEN MATCHED THEN UPDATE SET value = s.value "
            + "WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)");

    List<org.apache.spark.sql.Row> actual =
        spark
            .sql("SELECT id, value FROM " + catalogName + ".default." + tableName + " ORDER BY id")
            .collectAsList();
    List<org.apache.spark.sql.Row> expected =
        Arrays.asList(
            RowFactory.create(1, 110),
            RowFactory.create(2, 120),
            RowFactory.create(4, 40),
            RowFactory.create(5, 50),
            RowFactory.create(100, 1000),
            RowFactory.create(101, 1010));
    Assertions.assertEquals(expected, actual, "Expected merged rows to match result set");
  }

  @Test
  public void testBasicMatchedUpdate() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();
    op.insert(baseRows());

    List<Row> srcRows =
        Arrays.asList(
            Row.of(1, "Alice_new", 500, "Alice_meta", 500, Arrays.asList(500, 501)),
            Row.of(2, "Bob_new", 400, "Bob_meta", 400, Arrays.asList(400, 401)),
            Row.of(3, "Charlie_new", 600, "Charlie_meta", 600, Arrays.asList(600, 601)));
    op.createOrReplaceTempView("src", srcRows);

    String mergeSql =
        "MERGE INTO "
            + op.tableRef()
            + " t "
            + "USING src s "
            + "ON t.id = s.id "
            + "WHEN MATCHED THEN UPDATE SET "
            + "t.value = s.value + 1, "
            + "t.name = s.name, "
            + "t.values = s.values";

    spark.sql(mergeSql);

    op.check(
        Arrays.asList(
            Row.of(1, "Alice_new", 501, "Alice", 100, Arrays.asList(500, 501)),
            Row.of(2, "Bob_new", 401, "Bob", 200, Arrays.asList(400, 401)),
            Row.of(3, "Charlie_new", 601, "Charlie", 300, Arrays.asList(600, 601))));
  }

  @Test
  public void testConditionalMatchedUpdate() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();
    op.insert(baseRows());

    List<Row> srcRows =
        Arrays.asList(
            Row.of(1, "Alice_src", 100, "Alice_src", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob_src", 250, "Bob_src", 250, Arrays.asList(250, 251)),
            Row.of(3, "Charlie_src", 350, "Charlie_src", 350, Arrays.asList(350, 351)));
    op.createOrReplaceTempView("src", srcRows);

    String mergeSql =
        "MERGE INTO "
            + op.tableRef()
            + " t "
            + "USING src s "
            + "ON t.id = s.id "
            + "WHEN MATCHED AND s.value >= 250 THEN UPDATE SET "
            + "t.value = s.value + 1";

    spark.sql(mergeSql);

    op.check(
        Arrays.asList(
            // id = 1 does not meet condition, unchanged
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            // id = 2 and 3 updated on value only
            Row.of(2, "Bob", 251, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 351, "Charlie", 300, Arrays.asList(300, 301))));
  }

  @Test
  public void testConditionalMatchedDelete() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();
    op.insert(baseRows());

    List<Row> srcRows =
        Arrays.asList(
            Row.of(1, "Alice_src", 100, "Alice_src", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob_src", 200, "Bob_src", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie_src", 300, "Charlie_src", 300, Arrays.asList(300, 301)));
    op.createOrReplaceTempView("src", srcRows);

    String mergeSql =
        "MERGE INTO "
            + op.tableRef()
            + " t "
            + "USING src s "
            + "ON t.id = s.id "
            + "WHEN MATCHED AND s.value >= 300 THEN DELETE";

    spark.sql(mergeSql);

    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201))));
  }

  @Test
  public void testNotMatchedInsert() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();
    op.insert(baseRows());

    List<Row> srcRows =
        Arrays.asList(
            Row.of(4, "Dave", 400, "Dave", 400, Arrays.asList(400, 401)),
            Row.of(5, "Eve", 500, "Eve", 500, Arrays.asList(500, 501)));
    op.createOrReplaceTempView("src", srcRows);

    String mergeSql =
        "MERGE INTO "
            + op.tableRef()
            + " t "
            + "USING src s "
            + "ON t.id = s.id "
            + "WHEN NOT MATCHED THEN INSERT (id, name, value, meta, values) "
            + "VALUES (s.id, s.name, s.value, s.meta, s.values)";

    spark.sql(mergeSql);

    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301)),
            Row.of(4, "Dave", 400, "Dave", 400, Arrays.asList(400, 401)),
            Row.of(5, "Eve", 500, "Eve", 500, Arrays.asList(500, 501))));
  }

  @Test
  public void testConditionalNotMatchedInsert() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();
    op.insert(baseRows());

    List<Row> srcRows =
        Arrays.asList(
            Row.of(4, "Dave", 100, "Dave", 100, Arrays.asList(100, 101)),
            Row.of(5, "Eve", 400, "Eve", 400, Arrays.asList(400, 401)));
    op.createOrReplaceTempView("src", srcRows);

    String mergeSql =
        "MERGE INTO "
            + op.tableRef()
            + " t "
            + "USING src s "
            + "ON t.id = s.id "
            + "WHEN NOT MATCHED AND s.value >= 300 THEN INSERT (id, name, value, meta, values) "
            + "VALUES (s.id, s.name, s.value, s.meta, s.values)";

    spark.sql(mergeSql);

    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301)),
            // only Eve is inserted because of the condition
            Row.of(5, "Eve", 400, "Eve", 400, Arrays.asList(400, 401))));
  }

  @Test
  public void testMatchedBranchPriority() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    List<Row> initialRows =
        Collections.singletonList(Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)));
    op.insert(initialRows);

    List<Row> srcRows =
        Collections.singletonList(Row.of(1, "Source", 100, "Source", 100, Arrays.asList(100, 101)));
    op.createOrReplaceTempView("src", srcRows);

    String mergeSql =
        "MERGE INTO "
            + op.tableRef()
            + " t "
            + "USING src s "
            + "ON t.id = s.id "
            + "WHEN MATCHED AND s.value = 100 THEN UPDATE SET t.value = 1000 "
            + "WHEN MATCHED AND s.value = 100 THEN UPDATE SET t.value = 2000";

    spark.sql(mergeSql);

    op.check(
        Collections.singletonList(
            // first WHEN MATCHED branch should win
            Row.of(1, "Alice", 1000, "Alice", 100, Arrays.asList(100, 101))));
  }

  @Test
  public void testNoMatchWithoutInsertKeepsTargetUnchanged() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();
    op.insert(baseRows());

    List<Row> srcRows =
        Arrays.asList(
            Row.of(4, "Dave", 400, "Dave", 400, Arrays.asList(400, 401)),
            Row.of(5, "Eve", 500, "Eve", 500, Arrays.asList(500, 501)));
    op.createOrReplaceTempView("src", srcRows);

    String mergeSql =
        "MERGE INTO "
            + op.tableRef()
            + " t "
            + "USING src s "
            + "ON t.id = s.id "
            + "WHEN MATCHED THEN UPDATE SET t.value = s.value + 1";

    spark.sql(mergeSql);

    op.check(baseRows());
  }

  @Test
  public void testSourceDuplicateRowsBehavior() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    List<Row> initialRows =
        Collections.singletonList(Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)));
    op.insert(initialRows);

    List<Row> srcRows =
        Arrays.asList(
            Row.of(1, "Source1", 200, "Source1", 200, Arrays.asList(200, 201)),
            Row.of(1, "Source2", 300, "Source2", 300, Arrays.asList(300, 301)));
    op.createOrReplaceTempView("src", srcRows);

    String mergeSql =
        "MERGE INTO "
            + op.tableRef()
            + " t "
            + "USING src s "
            + "ON t.id = s.id "
            + "WHEN MATCHED THEN UPDATE SET t.value = s.value";

    Exception e =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              spark.sql(mergeSql);
            });
    System.out.println(
        "Merge source duplicate rows behavior: " + e.getClass().getName() + ": " + e.getMessage());
  }

  @Test
  public void testMatchedUpdateDeleteAndNotMatchedInsertInSingleMerge() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();
    op.insert(baseRows());

    List<Row> srcRows =
        Arrays.asList(
            // matched for delete
            Row.of(3, "Charlie_src", 350, "Charlie_src", 350, Arrays.asList(350, 351)),
            // matched for update
            Row.of(1, "Alice_src", 250, "Alice_src", 250, Arrays.asList(250, 251)),
            // not matched for insert
            Row.of(4, "David", 400, "David", 400, Arrays.asList(400, 401)));
    op.createOrReplaceTempView("src", srcRows);

    String mergeSql =
        "MERGE INTO "
            + op.tableRef()
            + " t "
            + "USING src s "
            + "ON t.id = s.id "
            + "WHEN MATCHED AND s.value >= 300 THEN DELETE "
            + "WHEN MATCHED AND s.value < 300 THEN UPDATE SET "
            + "t.value = s.value + 1, "
            + "t.name = s.name, "
            + "t.values = s.values "
            + "WHEN NOT MATCHED THEN INSERT (id, name, value, meta, values) "
            + "VALUES (s.id, s.name, s.value, s.meta, s.values)";

    spark.sql(mergeSql);

    op.check(
        Arrays.asList(
            // id=1 updated from src, meta remains from target
            Row.of(1, "Alice_src", 251, "Alice", 100, Arrays.asList(250, 251)),
            // id=2 unchanged
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            // id=3 deleted
            // id=4 inserted with meta and values from src
            Row.of(4, "David", 400, "David", 400, Arrays.asList(400, 401))));
  }

  protected static class TableOperator {
    private final SparkSession spark;
    private final String catalogName;
    private final String tableName;

    public TableOperator(SparkSession spark, String catalogName) {
      this.spark = spark;
      this.catalogName = catalogName;
      String baseName = "merge_test_table";
      this.tableName = baseName + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    public void create() {
      spark.sql(
          "CREATE TABLE "
              + catalogName
              + ".default."
              + tableName
              + " (id INT NOT NULL, name STRING, value INT, meta STRUCT<name: STRING, value: INT>, values ARRAY<INT>)");
    }

    public void insert(List<Row> rows) {
      String sql =
          String.format(
              "INSERT INTO %s.default.%s VALUES %s",
              catalogName,
              tableName,
              rows.stream().map(Row::insertSql).collect(Collectors.joining(", ")));
      spark.sql(sql);
    }

    public void createOrReplaceTempView(String viewName, List<Row> rows) {
      String valuesSql = rows.stream().map(Row::insertSql).collect(Collectors.joining(", "));
      String sql =
          String.format(
              "CREATE OR REPLACE TEMP VIEW %s AS SELECT * FROM VALUES %s AS %s(id, name, value, meta, values)",
              viewName, valuesSql, viewName);
      spark.sql(sql);
    }

    public String tableRef() {
      return catalogName + ".default." + tableName;
    }

    public void check(List<Row> expected) {
      String sql = String.format("Select * from %s.default.%s order by id", catalogName, tableName);
      List<Row> actual =
          spark.sql(sql).collectAsList().stream()
              .map(
                  row ->
                      Row.of(
                          row.getInt(0),
                          row.getString(1),
                          row.getInt(2),
                          row.getStruct(3).getString(0),
                          row.getStruct(3).getInt(1),
                          row.getList(4)))
              .collect(Collectors.toList());
      Assertions.assertEquals(expected, actual);
    }
  }

  protected static class Row {
    int id;
    String name;
    int value;
    String metaName;
    int metaValue;
    List<Integer> values;

    protected static Row of(
        int id, String name, int value, String metaName, int metaValue, List<Integer> values) {
      Row row = new Row();
      row.id = id;
      row.name = name;
      row.value = value;
      row.metaName = metaName;
      row.metaValue = metaValue;
      row.values = values;
      return row;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Row row = (Row) o;
      return id == row.id
          && value == row.value
          && metaValue == row.metaValue
          && Objects.equals(name, row.name)
          && Objects.equals(metaName, row.metaName)
          && Objects.deepEquals(values, row.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, value, metaName, metaValue, values);
    }

    @Override
    public String toString() {
      return String.format(
          "Row(id=%s, name=%s, value=%s, metaName=%s, metaValue=%s, values=%s)",
          id, name, value, metaName, metaValue, values);
    }

    private String insertSql() {
      return String.format(
          "(%d, '%s', %d, NAMED_STRUCT('name', '%s', 'value', %d), ARRAY(%s))",
          id,
          name,
          value,
          metaName,
          metaValue,
          values.stream().map(String::valueOf).collect(Collectors.joining(",")));
    }
  }
}
