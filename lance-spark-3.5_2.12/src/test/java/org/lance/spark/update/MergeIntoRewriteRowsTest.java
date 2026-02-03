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

public class MergeIntoRewriteRowsTest extends BaseMergeIntoTest {
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

    // Use RewriteRows mode to do update/merge-into
    spark.conf().set(SparkUtil.REWRITE_COLUMNS, "false");

    catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
  }
}
