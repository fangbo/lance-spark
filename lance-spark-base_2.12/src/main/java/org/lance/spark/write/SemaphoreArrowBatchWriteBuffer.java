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
package org.lance.spark.write;

import org.lance.spark.LanceRuntime;

import com.google.common.base.Preconditions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Buffers Spark rows into Arrow batches for consumption by Lance fragment creation.
 *
 * <p>This class bridges the producer (Spark thread writing rows) and consumer (Lance native code
 * pulling batches via ArrowReader interface). It uses a lock with conditions to synchronize between
 * the two threads - the producer blocks until the consumer is ready for more data, and vice versa.
 *
 * @see QueuedArrowBatchWriteBuffer for a queue-based alternative with better pipelining
 */
public class SemaphoreArrowBatchWriteBuffer extends ArrowBatchWriteBuffer {
  private final Schema schema;
  private final StructType sparkSchema;
  private final int batchSize;

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition canWrite = lock.newCondition();
  private final Condition batchReady = lock.newCondition();

  @GuardedBy("lock")
  private volatile boolean finished = false;

  @GuardedBy("lock")
  private int remainingWrites = 0;

  @GuardedBy("lock")
  private int count = 0;

  @GuardedBy("lock")
  private boolean batchFull = false;

  private org.lance.spark.arrow.LanceArrowWriter arrowWriter = null;

  public SemaphoreArrowBatchWriteBuffer(
      BufferAllocator allocator, Schema schema, StructType sparkSchema, int batchSize) {
    super(allocator);
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(batchSize > 0);
    this.schema = schema;
    this.sparkSchema = sparkSchema;
    this.batchSize = batchSize;
  }

  /** Simplified constructor that uses LanceRuntime allocator and converts Spark schema to Arrow. */
  public SemaphoreArrowBatchWriteBuffer(StructType sparkSchema, int batchSize) {
    this(
        LanceRuntime.allocator(),
        LanceArrowUtils.toArrowSchema(sparkSchema, "UTC", false),
        sparkSchema,
        batchSize);
  }

  @Override
  public void onTaskComplete() {
    lock.lock();
    try {
      canWrite.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void write(InternalRow row) {
    Preconditions.checkNotNull(row);
    lock.lock();
    try {
      // wait until prepareLoadNextBatch signals that writes are available
      while (remainingWrites == 0) {
        checkForError();
        canWrite.await();
      }

      arrowWriter.write(row);
      count++;
      remainingWrites--;

      if (count == batchSize) {
        batchFull = true;
        batchReady.signal();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void setFinished() {
    lock.lock();
    try {
      finished = true;
      batchReady.signal();
      canWrite.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void prepareLoadNextBatch() throws IOException {
    super.prepareLoadNextBatch();
    arrowWriter =
        org.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(
            this.getVectorSchemaRoot(), sparkSchema);
    lock.lock();
    try {
      count = 0;
      batchFull = false;
      remainingWrites = batchSize;
      canWrite.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    prepareLoadNextBatch();
    lock.lock();
    try {
      if (finished && count == 0) {
        return false;
      }

      // wait until batch is full or finished
      while (!batchFull && !finished) {
        batchReady.await();
      }

      arrowWriter.finish();

      if (!finished) {
        return true;
      } else {
        return count > 0;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public long bytesRead() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized void closeReadSource() throws IOException {
    // Implement if needed
  }

  @Override
  protected Schema readSchema() {
    return this.schema;
  }
}
