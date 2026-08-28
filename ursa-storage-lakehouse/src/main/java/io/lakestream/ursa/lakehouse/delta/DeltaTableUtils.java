/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.lakestream.ursa.lakehouse.utils.LakehouseFieldNames;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import io.lakestream.ursa.lakehouse.utils.TopicNames;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.IntStream;

public class DeltaTableUtils {

    public static String normalizeStorageLocation(String location) {
        if (location != null && location.startsWith("s3://")) {
            return location.replace("s3://", "s3a://");
        }
        return location;
    }

    public static Table loadTable(Engine engine, String location) {
        return Table.forPath(engine, location);
    }

    public static boolean isTableExists(Table table, Engine engine) {
        try {
            table.getLatestSnapshot(engine);
            return true;
        } catch (TableNotFoundException e) {
            return false;
        }
    }

    public static String generateTableLocation(String storagePath, TopicName topic) {
        return TopicNames.storagePath(storagePath, topic.toString());
    }

    public static String generateTableLocation(String storagePath, String topic) {
        return TopicNames.storagePath(storagePath, topic);
    }

    private static final Map<String, Integer> ADD_COL_NAME_TO_ORDINAL =
            IntStream.range(0, AddFile.FULL_SCHEMA.length())
                    .boxed()
                    .collect(toMap(i -> AddFile.FULL_SCHEMA.at(i).getName(), i -> i));

    private static final StructType ADD_FILE_SCHEMA =
            (StructType) InternalScanFileUtils.SCAN_FILE_SCHEMA.get("add").getDataType();

    private static final int ADD_FILE_PATH_ORDINAL = ADD_FILE_SCHEMA.indexOf("path");
    private static final int ADD_FILE_PARTITION_VALUES_ORDINAL =
            ADD_FILE_SCHEMA.indexOf("partitionValues");
    private static final int ADD_FILE_SIZE_ORDINAL = ADD_FILE_SCHEMA.indexOf("size");
    private static final int ADD_FILE_MOD_TIME_ORDINAL =
            ADD_FILE_SCHEMA.indexOf("modificationTime");
    private static final int ADD_FILE_TAGS_ORDINAL =
            ADD_FILE_SCHEMA.indexOf("tags");
    private static final int ADD_FILE_DATA_CHANGE_ORDINAL = ADD_FILE_SCHEMA.indexOf("dataChange");
    private static final int ADD_FILE_DV_ORDINAL = ADD_FILE_SCHEMA.indexOf("deletionVector");
    private static final int TABLE_ROOT_ORDINAL =
            InternalScanFileUtils.SCAN_FILE_SCHEMA.indexOf(InternalScanFileUtils.TABLE_ROOT_STRUCT_FIELD.getName());
    public static final int ADD_FILE_STATS_ORDINAL = AddFile.SCHEMA_WITH_STATS.indexOf("stats");

    private static final int DV_STORAGE_TYPE_ORDINAL = DeletionVectorDescriptor.READ_SCHEMA.indexOf("storageType");
    private static final int DV_PATH_OR_INLINE_DV_ORDINAL =
            DeletionVectorDescriptor.READ_SCHEMA.indexOf("pathOrInlineDv");
    private static final int DV_OFFSET_ORDINAL = DeletionVectorDescriptor.READ_SCHEMA.indexOf("offset");
    private static final int DV_SIZE_IN_BYTES_ORDINAL = DeletionVectorDescriptor.READ_SCHEMA.indexOf("sizeInBytes");
    private static final int DV_CARDINALITY_ORDINAL = DeletionVectorDescriptor.READ_SCHEMA.indexOf("cardinality");

    public static Row buildAddFileAction(String filePath, long fileSize, long modificationTime,
                                         Map<String, String> partitionValues,
                                         boolean dataChange, String stats, Map<String, String> tags) {
        Map<Integer, Object> valueMap = new HashMap<>();
        valueMap.put(ADD_COL_NAME_TO_ORDINAL.get("path"), filePath);
        if (partitionValues != null) {
            valueMap.put(ADD_COL_NAME_TO_ORDINAL.get("partitionValues"),
                VectorUtils.stringStringMapValue(partitionValues));
        }
        valueMap.put(ADD_COL_NAME_TO_ORDINAL.get("size"), fileSize);
        valueMap.put(ADD_COL_NAME_TO_ORDINAL.get("modificationTime"), modificationTime);
        valueMap.put(ADD_COL_NAME_TO_ORDINAL.get("dataChange"), dataChange);
        valueMap.put(ADD_COL_NAME_TO_ORDINAL.get("stats"), stats);
        if (tags != null) {
            valueMap.put(ADD_COL_NAME_TO_ORDINAL.get("tags"), VectorUtils.stringStringMapValue(tags));
        }
        // any fields not present in the valueMap are considered null
        return SingleAction.createAddFileSingleAction(new GenericRow(AddFile.FULL_SCHEMA, valueMap));
    }

    private static final Map<String, Integer> REMOVE_COL_NAME_TO_ORDINAL =
            IntStream.range(0, RemoveFile.FULL_SCHEMA.length())
                    .boxed()
                    .collect(toMap(i -> RemoveFile.FULL_SCHEMA.at(i).getName(), i -> i));


    public static Row buildRemoveFileAction(String filePath, long fileSize, long deletionTimestamp,
                                            Map<String, String> partitionValues,
                                            boolean dataChange, String stats, Map<String, String> tags) {
        Map<Integer, Object> valueMap = new HashMap<>();
        valueMap.put(REMOVE_COL_NAME_TO_ORDINAL.get("path"), filePath);
        valueMap.put(REMOVE_COL_NAME_TO_ORDINAL.get("deletionTimestamp"), deletionTimestamp);
        valueMap.put(REMOVE_COL_NAME_TO_ORDINAL.get("dataChange"), dataChange);
        valueMap.put(REMOVE_COL_NAME_TO_ORDINAL.get("partitionValues"),
                VectorUtils.stringStringMapValue(partitionValues));
        valueMap.put(REMOVE_COL_NAME_TO_ORDINAL.get("size"), fileSize);
        valueMap.put(REMOVE_COL_NAME_TO_ORDINAL.get("stats"), stats);
        valueMap.put(REMOVE_COL_NAME_TO_ORDINAL.get("tags"), VectorUtils.stringStringMapValue(tags));
        // any fields not present in the valueMap are considered null
        return SingleAction.createRemoveFileSingleAction(new GenericRow(RemoveFile.FULL_SCHEMA, valueMap));

    }

    public static CloseableIterable<Row> toCloseableIterable(List<Row> actions) {
        Iterator<Row> actionIterator = actions.iterator();

        CloseableIterator<Row> actionIteratorWrapper = new CloseableIterator<>() {

            @Override
            public boolean hasNext() {
                return actionIterator.hasNext();
            }

            @Override
            public Row next() {
                return actionIterator.next();
            }

            @Override
            public void close() throws IOException {

            }
        };
        return CloseableIterable.inMemoryIterable(actionIteratorWrapper);
    }

    public static CloseableIterator<AddFileAction> getAddActionIterator(Snapshot latestSnapshot, Engine engine) {
        try {
            ScanImpl scan = (ScanImpl) latestSnapshot.getScanBuilder().build();
            return scanAddFileActions(scan, engine);
        } catch (TableNotFoundException e) {
            return CloseableIterators.empty();
        }
    }

    public static CloseableIterator<AddFileAction> scanAddFileActions(ScanImpl scan, Engine engine) {
        CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(engine, true);

        return new CloseableIterator<AddFileAction>() {
            private CloseableIterator<Row> currentRowIter = null;
            private boolean closed = false;

            @Override
            public boolean hasNext() {
                if (closed) {
                    return false;
                }

                try {
                    if (currentRowIter != null && currentRowIter.hasNext()) {
                        return true;
                    }

                    while (scanFileIter.hasNext()) {
                        if (currentRowIter != null) {
                            currentRowIter.close();
                        }

                        FilteredColumnarBatch batch = scanFileIter.next();
                        currentRowIter = batch.getRows();

                        if (currentRowIter.hasNext()) {
                            return true;
                        }
                    }

                    return false;
                } catch (Exception e) {
                    closeQuietly();
                    throw new RuntimeException("Failed to check for next element", e);
                }
            }

            @Override
            public AddFileAction next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                try {
                    Row row = currentRowIter.next();
                    return parseAddFileAction(row);
                } catch (Exception e) {
                    closeQuietly();
                    throw new RuntimeException("Failed to parse AddFileAction", e);
                }
            }

            @Override
            public void close() throws IOException {
                closed = true;
                if (currentRowIter != null) {
                    currentRowIter.close();
                }
                scanFileIter.close();
            }

            private void closeQuietly() {
                closed = true;
                try {
                    if (currentRowIter != null) {
                        currentRowIter.close();
                    }
                } catch (Exception ignored) {}
                try {
                    scanFileIter.close();
                } catch (Exception ignored) {}
            }
        };
    }

    private static AddFileAction parseAddFileAction(Row scanFileInfo) {
        Row addFile = getAddFileEntry(scanFileInfo);
        String path = addFile.getString(ADD_FILE_PATH_ORDINAL);
        long size = addFile.getLong(ADD_FILE_SIZE_ORDINAL);
        Map<String, String> partitionValues = VectorUtils.toJavaMap(addFile.getMap(ADD_FILE_PARTITION_VALUES_ORDINAL));
        long modificationTime = addFile.getLong(ADD_FILE_MOD_TIME_ORDINAL);
        String stats = addFile.getString(ADD_FILE_STATS_ORDINAL);
        boolean dataChange = addFile.getBoolean(ADD_FILE_DATA_CHANGE_ORDINAL);
        Row dv = addFile.getStruct(ADD_FILE_DV_ORDINAL);
        AddFileAction.DeletionVector deletionVector = null;
        if (dv != null) {
            String storageType = dv.getString(DV_STORAGE_TYPE_ORDINAL);
            String pathOrInlineDv = dv.getString(DV_PATH_OR_INLINE_DV_ORDINAL);
            int offset = dv.getInt(DV_OFFSET_ORDINAL);
            int sizeInBytes = dv.getInt(DV_SIZE_IN_BYTES_ORDINAL);
            long cardinality = dv.getLong(DV_CARDINALITY_ORDINAL);
            deletionVector  =
                    new AddFileAction.DeletionVector(storageType, pathOrInlineDv, offset, sizeInBytes, cardinality);
        }
        Map<String, String> tags = VectorUtils.toJavaMap(addFile.getMap(ADD_FILE_TAGS_ORDINAL));

        return new AddFileAction(path, size, partitionValues, modificationTime, stats, dataChange, deletionVector,
                tags);
    }

    private static Row getAddFileEntry(Row scanFileInfo) {
        if (scanFileInfo.isNullAt(InternalScanFileUtils.ADD_FILE_ORDINAL)) {
            throw new IllegalArgumentException("There is no `add` entry in the scan file row");
        }
        return scanFileInfo.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL);
    }

    public static List<Row> loadData(Engine engine, Snapshot snapshot) throws IOException {
        List<Row> records = new ArrayList<>();
        Scan scan = snapshot.getScanBuilder().withReadSchema(removeMetaColumn(snapshot.getSchema())).build();
        Row scanState = scan.getScanState(engine);
        CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(engine);
        try {
            StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(scanState);
            while (scanFileIter.hasNext()) {
                FilteredColumnarBatch scanFilesBatch = scanFileIter.next();
                try (CloseableIterator<Row> scanFileRows = scanFilesBatch.getRows()) {
                    while (scanFileRows.hasNext()) {
                        Row scanFileRow = scanFileRows.next();
                        io.delta.kernel.utils.FileStatus fileStatus =
                                InternalScanFileUtils.getAddFileStatus(scanFileRow);

                        CloseableIterator<ColumnarBatch> physicalDataIter =
                            engine.getParquetHandler().readParquetFiles(
                                singletonCloseableIterator(fileStatus),
                                physicalReadSchema,
                                Optional.empty()).map(FileReadResult::getData);
                        try (
                                CloseableIterator<FilteredColumnarBatch> transformedData =
                                        Scan.transformPhysicalData(
                                                engine,
                                                scanState,
                                                scanFileRow,
                                                physicalDataIter)) {
                            while (transformedData.hasNext()) {
                                FilteredColumnarBatch data = transformedData.next();
                                try (CloseableIterator<Row> rows = data.getRows()) {
                                    while (rows.hasNext()) {
                                        records.add(rows.next());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            scanFileIter.close();
        }
        return records;
    }

    static StructType removeMetaColumn(StructType physicalReadSchema) {
        if (!physicalReadSchema.fieldNames().contains(LakehouseFieldNames.META)) {
            return physicalReadSchema;
        }

        List<StructField> fields = new ArrayList<>(physicalReadSchema.length());
        for (StructField field : physicalReadSchema.fields()) {
            if (!LakehouseFieldNames.META.equals(field.getName())) {
                fields.add(field);
            }
        }
        return new StructType(fields);
    }
}
