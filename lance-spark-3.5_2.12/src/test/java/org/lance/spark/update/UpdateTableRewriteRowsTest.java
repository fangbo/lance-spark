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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

public class UpdateTableRewriteRowsTest extends UpdateTableTest {
  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-namespace-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", getNsImpl())
            .getOrCreate();

    Map<String, String> additionalConfigs = getAdditionalNsConfigs();
    for (Map.Entry<String, String> entry : additionalConfigs.entrySet()) {
      spark.conf().set("spark.sql.catalog." + catalogName + "." + entry.getKey(), entry.getValue());
    }

    // Use RewriteRows mode to do update/merge-into
    spark.conf().set(SparkUtil.REWRITE_COLUMNS, "false");

    catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
  }

  @Test
  public void testUpdateChildSomeRows() {
    // Because Fragment.updateColumns can't accept null for struct column.
    // So update for partial rows must use RewriteRow mode.
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 100, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));

    op.updateStructValue(101, "id = 1");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100, "Alice", 101, Arrays.asList(100, 101)),
            Row.of(2, "Bob", 200, "Bob", 200, Arrays.asList(200, 201)),
            Row.of(3, "Charlie", 300, "Charlie", 300, Arrays.asList(300, 301))));
  }
}
