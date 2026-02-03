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

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.ReadOptions;
import org.lance.RowAddress;
import org.lance.WriteParams;
import org.lance.fragment.FragmentUpdateResult;
import org.lance.io.StorageOptionsProvider;
import org.lance.operation.Update;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceDataset;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.arrow.LanceArrowWriter;
import org.lance.spark.function.LanceFragmentIdWithDefaultFunction;
import org.lance.spark.utils.SparkUtil;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.SortValue;
import org.apache.spark.sql.connector.write.DeltaBatchWrite;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

public class SparkPositionDeltaWrite implements DeltaWrite, RequiresDistributionAndOrdering {
  private static final Logger LOG = LoggerFactory.getLogger(SparkPositionDeltaWrite.class);

  private final StructType sparkSchema;
  private final List<String> updatedColumns;
  private final LanceSparkWriteOptions writeOptions;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final Map<String, String> namespaceProperties;
  private final List<String> tableId;

  public SparkPositionDeltaWrite(
      StructType sparkSchema,
      List<String> updatedColumns,
      LanceSparkWriteOptions writeOptions,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId) {
    this.sparkSchema = sparkSchema;
    this.updatedColumns = updatedColumns;
    this.writeOptions = writeOptions;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
  }

  @Override
  public Distribution requiredDistribution() {
    NamedReference segmentId = Expressions.column(LanceConstant.FRAGMENT_ID);
    // Avoid skew by spreading null segment_id rows across tasks.
    Expression clusteredExpr =
        Expressions.apply(LanceFragmentIdWithDefaultFunction.NAME, segmentId);
    return Distributions.clustered(new Expression[] {clusteredExpr});
  }

  @Override
  public SortOrder[] requiredOrdering() {
    NamedReference segmentId = Expressions.column(LanceConstant.ROW_ADDRESS);
    SortValue sortValue =
        new SortValue(segmentId, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    return new SortValue[] {sortValue};
  }

  @Override
  public DeltaBatchWrite toBatch() {
    return new PositionDeltaBatchWrite();
  }

  private class PositionDeltaBatchWrite implements DeltaBatchWrite {

    @Override
    public DeltaWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      return new PositionDeltaWriteFactory(
          sparkSchema,
          updatedColumns,
          writeOptions,
          initialStorageOptions,
          namespaceImpl,
          namespaceProperties,
          tableId);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      List<Long> removedFragmentIds = new ArrayList<>();
      List<FragmentMetadata> updatedFragments = new ArrayList<>();
      List<FragmentMetadata> newFragments = new ArrayList<>();
      Set<Long> fieldsModified = new HashSet<>();

      Arrays.stream(messages)
          .map(m -> (DeltaWriteTaskCommit) m)
          .forEach(
              m -> {
                removedFragmentIds.addAll(m.removedFragmentIds());
                updatedFragments.addAll(m.updatedFragments());
                newFragments.addAll(m.newFragments());
                fieldsModified.addAll(m.fieldsModified());
              });

      // Use SDK directly to update fragments
      try (Dataset dataset = openDataset(writeOptions)) {
        Update update =
            Update.builder()
                .removedFragmentIds(removedFragmentIds)
                .updatedFragments(updatedFragments)
                .newFragments(newFragments)
                .fieldsModified(fieldsModified.stream().mapToLong(Long::longValue).toArray())
                .updateMode(
                    Optional.of(
                        fieldsModified.isEmpty()
                            ? Update.UpdateMode.RewriteRows
                            : Update.UpdateMode.RewriteColumns))
                .build();

        dataset.newTransactionBuilder().operation(update).build().commit();
      }
    }

    private Dataset openDataset(LanceSparkWriteOptions options) {
      if (options.hasNamespace()) {
        return Dataset.open()
            .allocator(LanceRuntime.allocator())
            .namespace(options.getNamespace())
            .tableId(options.getTableId())
            .session(LanceRuntime.session())
            .build();
      } else {
        ReadOptions readOptions =
            new ReadOptions.Builder()
                .setStorageOptions(options.getStorageOptions())
                .setSession(LanceRuntime.session())
                .build();
        return Dataset.open()
            .allocator(LanceRuntime.allocator())
            .uri(options.getDatasetUri())
            .readOptions(readOptions)
            .build();
      }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {}
  }

  private static class PositionDeltaWriteFactory implements DeltaWriterFactory {
    private final StructType sparkSchema;
    private final List<String> updatedColumns;
    private final LanceSparkWriteOptions writeOptions;

    /**
     * Initial storage options fetched from namespace.describeTable() on the driver. These are
     * passed to workers so they can reuse the credentials without calling describeTable again.
     */
    private final Map<String, String> initialStorageOptions;

    /** Namespace configuration for credential refresh on workers. */
    private final String namespaceImpl;

    private final Map<String, String> namespaceProperties;
    private final List<String> tableId;

    PositionDeltaWriteFactory(
        StructType sparkSchema,
        List<String> updatedColumns,
        LanceSparkWriteOptions writeOptions,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId) {
      this.sparkSchema = sparkSchema;
      this.updatedColumns = updatedColumns;
      this.writeOptions = writeOptions;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
    }

    @Override
    public DeltaWriter<InternalRow> createWriter(int partitionId, long taskId) {
      int batchSize = writeOptions.getBatchSize();
      boolean useQueuedBuffer = writeOptions.isUseQueuedWriteBuffer();

      // Merge initial storage options with write options
      WriteParams params = buildWriteParams();

      // Select buffer type based on configuration
      ArrowBatchWriteBuffer writeBuffer;
      if (useQueuedBuffer) {
        int queueDepth = writeOptions.getQueueDepth();
        writeBuffer = new QueuedArrowBatchWriteBuffer(sparkSchema, batchSize, queueDepth);
      } else {
        writeBuffer = new SemaphoreArrowBatchWriteBuffer(sparkSchema, batchSize);
      }

      // Get storage options provider for credential refresh
      StorageOptionsProvider storageOptionsProvider = getStorageOptionsProvider();

      // Create fragment in background thread
      Callable<List<FragmentMetadata>> fragmentCreator =
          () -> {
            try (ArrowArrayStream arrowStream =
                ArrowArrayStream.allocateNew(LanceRuntime.allocator())) {
              Data.exportArrayStream(LanceRuntime.allocator(), writeBuffer, arrowStream);
              return Fragment.create(
                  writeOptions.getDatasetUri(), arrowStream, params, storageOptionsProvider);
            }
          };
      FutureTask<List<FragmentMetadata>> fragmentCreationTask = new FutureTask<>(fragmentCreator);
      Thread fragmentCreationThread = new Thread(fragmentCreationTask);
      fragmentCreationThread.start();

      return new LanceDeltaWriter(
          sparkSchema,
          updatedColumns,
          writeOptions,
          new LanceDataWriter(writeBuffer, fragmentCreationTask, fragmentCreationThread),
          initialStorageOptions);
    }

    private WriteParams buildWriteParams() {
      Map<String, String> merged =
          LanceRuntime.mergeStorageOptions(writeOptions.getStorageOptions(), initialStorageOptions);

      WriteParams.Builder builder = new WriteParams.Builder();
      builder.withMode(writeOptions.getWriteMode());
      if (writeOptions.getMaxRowsPerFile() != null) {
        builder.withMaxRowsPerFile(writeOptions.getMaxRowsPerFile());
      }
      if (writeOptions.getMaxRowsPerGroup() != null) {
        builder.withMaxRowsPerGroup(writeOptions.getMaxRowsPerGroup());
      }
      if (writeOptions.getMaxBytesPerFile() != null) {
        builder.withMaxBytesPerFile(writeOptions.getMaxBytesPerFile());
      }
      if (writeOptions.getDataStorageVersion() != null) {
        builder.withDataStorageVersion(writeOptions.getDataStorageVersion());
      }
      builder.withStorageOptions(merged);
      return builder.build();
    }

    private StorageOptionsProvider getStorageOptionsProvider() {
      return LanceRuntime.getOrCreateStorageOptionsProvider(
          namespaceImpl, namespaceProperties, tableId);
    }
  }

  private static class LanceDeltaWriter implements DeltaWriter<InternalRow> {
    private final Dataset dataset;

    private final LanceSparkWriteOptions writeOptions;
    private final LanceDataWriter writer;

    /**
     * Initial storage options fetched from namespace.describeTable() on the driver. These are
     * passed to workers so they can reuse the credentials without calling describeTable again.
     */
    private final Map<String, String> initialStorageOptions;

    // Key is fragmentId, Value is fragment's deleted row indexes
    private final Map<Integer, RoaringBitmap> deletedRows;

    // Spark schema for updated columns
    private Optional<StructType> updatedSparkSchema;

    // Data stream arrow schema for updated columns
    private Optional<Schema> updatedArrowSchema;

    // Updated column ordinals in source input row
    private Optional<int[]> updatedColumnOrdinals;

    private Optional<Set<Long>> fieldsModified;
    private Optional<Map<Integer, FragmentMetadata>> updatedFragments;

    private Optional<Integer> currentUpdateFragmentId;
    private Optional<VectorSchemaRoot> currentUpdateData;
    private Optional<LanceArrowWriter> currentUpdateWriter;

    private LanceDeltaWriter(
        StructType sparkSchema,
        List<String> updatedColumns,
        LanceSparkWriteOptions writeOptions,
        LanceDataWriter writer,
        Map<String, String> initialStorageOptions) {
      this.dataset = openDataset(writeOptions);
      this.writeOptions = writeOptions;
      this.writer = writer;
      this.initialStorageOptions = initialStorageOptions;
      this.deletedRows = new HashMap<>();

      if (updatedColumns != null && !updatedColumns.isEmpty()) {
        StructType schema =
            new StructType(
                updatedColumns.stream()
                    .map(sparkSchema::apply)
                    .collect(Collectors.toList())
                    .toArray(new StructField[0]));
        schema =
            schema.add(
                LanceDataset.ROW_ADDRESS_COLUMN.name(),
                LanceDataset.ROW_ADDRESS_COLUMN.dataType(),
                LanceDataset.ROW_ADDRESS_COLUMN.isNullable());

        updatedSparkSchema = Optional.of(schema);
        updatedArrowSchema =
            Optional.of(LanceArrowUtils.toArrowSchema(schema, "UTC", false));

        updatedColumnOrdinals =
            Optional.of(
                updatedColumns.stream()
                    .map(sparkSchema::fieldIndex)
                    .mapToInt(Integer::intValue)
                    .toArray());
      }
      fieldsModified = Optional.of(new HashSet<>());
      updatedFragments = Optional.of(new HashMap<>());
      currentUpdateFragmentId = Optional.of(-1);
    }

    @Override
    public void delete(InternalRow metadata, InternalRow id) throws IOException {
      int fragmentId = metadata.getInt(0);
      deletedRows.compute(
          fragmentId,
          (k, v) -> {
            if (v == null) {
              v = new RoaringBitmap();
            }
            // Get the row index which is low 32 bits of row address.
            // See
            // https://github.com/lance-format/lance/blob/main/rust/lance-core/src/utils/address.rs#L36
            v.add(RowAddress.rowIndex(id.getLong(0)));
            return v;
          });
    }

    @Override
    public void update(InternalRow metadata, InternalRow id, InternalRow row) throws IOException {
      if (updatedArrowSchema == null) {
        throw new UnsupportedOperationException(
            "Updated columns is empty. Maybe some bugs for updated columns extractor. "
                + "You can set "
                + SparkUtil.REWRITE_COLUMNS
                + " to false to disable this feature.");
      }

      int fragmentId = metadata.getInt(0);
      if (currentUpdateFragmentId.get() != fragmentId) {

        // A new fragment is coming, update old fragment columns.
        updateFragmentColumns();

        // Initialize a new arrow batch writee for new fragment.
        currentUpdateFragmentId = Optional.of(fragmentId);
        currentUpdateData =
            Optional.of(
                VectorSchemaRoot.create(updatedArrowSchema.get(), LanceRuntime.allocator()));
        currentUpdateWriter =
            Optional.of(
                org.lance.spark.arrow.LanceArrowWriter$.MODULE$.create(
                    currentUpdateData.get(), updatedSparkSchema.get()));
      }

      // Copy updated columns from source row to updated data row.
      for (int i = 0; i < updatedColumnOrdinals.get().length; i++) {
        currentUpdateWriter.get().field(i).write(row, updatedColumnOrdinals.get()[i]);
      }
      // Add row address column to updated data row.
      currentUpdateWriter.get().field(updatedColumnOrdinals.get().length).write(id, 0);
    }

    private void updateFragmentColumns() throws IOException {
      if (currentUpdateFragmentId.get() == -1) {
        return;
      }

      LOG.info("Update columns for fragment: {}", currentUpdateFragmentId);

      currentUpdateWriter.get().finish();

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(currentUpdateData.get(), null, out)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }

      byte[] arrowData = out.toByteArray();
      ByteArrayInputStream in = new ByteArrayInputStream(arrowData);
      BufferAllocator allocator = LanceRuntime.allocator();

      try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
          ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
        Data.exportArrayStream(allocator, reader, stream);

        // Use Dataset to get the fragment and merge columns
        Fragment fragment = new Fragment(dataset, currentUpdateFragmentId.get());
        FragmentUpdateResult result =
            fragment.updateColumns(
                stream,
                LanceDataset.ROW_ADDRESS_COLUMN.name(),
                LanceDataset.ROW_ADDRESS_COLUMN.name());

        for (long fieldId : result.getFieldsModified()) {
          fieldsModified.get().add(fieldId);
        }
        updatedFragments.get().put(currentUpdateFragmentId.get(), result.getUpdatedFragment());
      }

      currentUpdateData.get().close();
    }

    @Override
    public void insert(InternalRow row) throws IOException {
      writer.write(row);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      updateFragmentColumns();

      // Write new fragments to store new updated rows.
      LanceBatchWrite.TaskCommit append = (LanceBatchWrite.TaskCommit) writer.commit();
      List<FragmentMetadata> newFragments = append.getFragments();

      List<Long> removedFragmentIds = new ArrayList<>();

      // Deleting updated rows from old fragments.
      this.deletedRows.forEach(
          (fragmentId, rowIndexes) -> {
            FragmentMetadata updatedFragment =
                dataset.getFragment(fragmentId).deleteRows(ImmutableList.copyOf(rowIndexes));

            if (updatedFragment != null) {
              updatedFragments
                  .get()
                  .compute(
                      fragmentId,
                      (k, v) -> {
                        if (v == null) {
                          return updatedFragment;
                        } else {
                          return new FragmentMetadata(
                              v.getId(),
                              v.getFiles(),
                              v.getPhysicalRows(),
                              updatedFragment.getDeletionFile(),
                              v.getRowIdMeta());
                        }
                      });
            } else {
              removedFragmentIds.add(Long.valueOf(fragmentId));
            }
          });

      return new DeltaWriteTaskCommit(
          removedFragmentIds,
          new ArrayList<>(updatedFragments.get().values()),
          fieldsModified.get(),
          newFragments);
    }

    private Dataset openDataset(LanceSparkWriteOptions options) {
      // Note: options.hasNamespace() is false on workers (namespace is transient)
      Map<String, String> merged =
          LanceRuntime.mergeStorageOptions(options.getStorageOptions(), initialStorageOptions);
      ReadOptions readOptions =
          new ReadOptions.Builder()
              .setStorageOptions(merged)
              .setSession(LanceRuntime.session())
              .build();
      return Dataset.open()
          .allocator(LanceRuntime.allocator())
          .uri(options.getDatasetUri())
          .readOptions(readOptions)
          .build();
    }

    @Override
    public void abort() throws IOException {
      writer.abort();
      dataset.close();
    }

    @Override
    public void close() throws IOException {
      writer.close();
      dataset.close();
    }
  }

  private static class DeltaWriteTaskCommit implements WriterCommitMessage {
    private List<Long> removedFragmentIds;
    private List<FragmentMetadata> updatedFragments;
    private Set<Long> fieldsModified;
    private List<FragmentMetadata> newFragments;

    DeltaWriteTaskCommit(
        List<Long> removedFragmentIds,
        List<FragmentMetadata> updatedFragments,
        Set<Long> fieldsModified,
        List<FragmentMetadata> newFragments) {
      this.removedFragmentIds = removedFragmentIds;
      this.updatedFragments = updatedFragments;
      this.fieldsModified = fieldsModified;
      this.newFragments = newFragments;
    }

    public List<Long> removedFragmentIds() {
      return removedFragmentIds == null ? Collections.emptyList() : removedFragmentIds;
    }

    public List<FragmentMetadata> updatedFragments() {
      return updatedFragments == null ? Collections.emptyList() : updatedFragments;
    }

    public Set<Long> fieldsModified() {
      return fieldsModified;
    }

    public List<FragmentMetadata> newFragments() {
      return newFragments == null ? Collections.emptyList() : newFragments;
    }
  }
}
