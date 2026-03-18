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

public class UpdateTableTest extends BaseUpdateTableTest {
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

    spark.conf().set(SparkUtil.REWRITE_COLUMNS, "true");

    catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    // Create default namespace for multi-level namespace mode
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".default");
  }
}
