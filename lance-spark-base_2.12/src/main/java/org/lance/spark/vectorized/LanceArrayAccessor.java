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
package org.lance.spark.vectorized;

import org.apache.arrow.vector.complex.ListVector;
import org.apache.spark.sql.vectorized.ColumnarArray;

public class LanceArrayAccessor {

  private final ListVector accessor;
  private final LanceArrowColumnVector arrayData;

  public LanceArrayAccessor(ListVector vector) {
    this.accessor = vector;
    this.arrayData = new LanceArrowColumnVector(vector.getDataVector());
  }

  public boolean isNullAt(int rowId) {
    return this.accessor.isNull(rowId);
  }

  public int getNullCount() {
    return this.accessor.getNullCount();
  }

  public ColumnarArray getArray(int rowId) {
    int start = accessor.getElementStartIndex(rowId);
    int end = accessor.getElementEndIndex(rowId);
    return new ColumnarArray(arrayData, start, end - start);
  }

  public void close() {
    this.accessor.close();
  }
}
