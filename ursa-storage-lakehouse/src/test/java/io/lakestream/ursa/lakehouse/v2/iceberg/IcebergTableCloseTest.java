/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.MessageSerDeException;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.iceberg.IcebergCatalogBackendType;
import io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import io.lakestream.ursa.lakehouse.iceberg.Operation;
import io.lakestream.ursa.lakehouse.iceberg.RecordWrapper;
import io.lakestream.ursa.lakehouse.iceberg.ReferencedCatalog;
import io.lakestream.ursa.lakehouse.iceberg.TableOptions;
import io.lakestream.ursa.lakehouse.iceberg.Utilities;
import io.lakestream.ursa.lakehouse.v2.AbstractLakehouseWriter;
import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.Unpooled;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Tests that IcebergTable.close() is properly called in all writer lifecycle paths,
 * preventing resource leaks of ReferencedCatalog references.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class IcebergTableCloseTest {

    @Mock
    private IcebergTable icebergTable;

    @Mock
    private Table table;

    @Mock
    private TaskWriter<Record> taskWriter;

    @Mock
    private WriteResult writeResult;

    @Mock
    private LakehouseConfiguration config;

    @Mock
    private EntrySerdeFactory entrySerdeFactory;

    private MockedStatic<CatalogUtil> catalogUtilMock;

    private final String topic = "namespace/topic";

    @BeforeEach
    void setUp() {
        lenient().when(config.getProperties()).thenReturn(new Properties());
        lenient().when(config.getIcebergTableProperties()).thenReturn(Map.of());
        lenient().when(config.isSchemaEvolutionEnabled()).thenReturn(false);
        lenient().when(config.getCatalogMaxOpenTime()).thenReturn(Duration.ofDays(365));
        lenient().when(config.getCatalogName()).thenReturn(Optional.empty());
        lenient().when(config.getIcebergCatalogType(any())).thenReturn("hadoop");
        lenient().when(config.getIcebergCatalogBackendType(any())).thenReturn(IcebergCatalogBackendType.HADOOP);
        lenient().when(config.getIcebergProperties(any())).thenReturn(Map.of("type", "hadoop"));

        catalogUtilMock = mockStatic(CatalogUtil.class);
        catalogUtilMock.when(() -> CatalogUtil.buildIcebergCatalog(any(), any(), any()))
            .thenReturn(mock(Catalog.class));
    }

    @AfterEach
    void tearDown() {
        if (catalogUtilMock != null) {
            catalogUtilMock.close();
        }
    }

    // ======== IcebergExternalTableWriter close() tests ========

    @Test
    void testExternalWriter_CloseReleasesIcebergTable() throws Exception {
        var writer = new IcebergExternalTableWriter(topic, entrySerdeFactory, config);
        injectField(writer, IcebergExternalTableWriter.class, "icebergTable", icebergTable);

        writer.close();

        verify(icebergTable, times(1)).close();
    }

    @Test
    void testExternalWriter_CloseReleasesIcebergTableEvenOnWriteError() throws Exception {
        var writer = new IcebergExternalTableWriter(topic, entrySerdeFactory, config);
        injectField(writer, IcebergExternalTableWriter.class, "icebergTable", icebergTable);
        injectField(writer, IcebergExternalTableWriter.class, "taskWriters",
            new java.util.concurrent.ConcurrentHashMap<>(Map.of(1, taskWriter)));

        when(taskWriter.complete()).thenThrow(new IOException("Write failed"));

        try {
            writer.close();
        } catch (Exception ignored) {
            // Expected
        }

        // IcebergTable.close() must be called even when taskWriter fails
        verify(icebergTable, times(1)).close();
    }

    @Test
    void testExternalWriter_CloseReleasesIcebergTableOnSuccessfulWrite() throws Exception {
        var writer = new IcebergExternalTableWriter(topic, entrySerdeFactory, config);
        injectField(writer, IcebergExternalTableWriter.class, "icebergTable", icebergTable);
        injectField(writer, IcebergExternalTableWriter.class, "taskWriters",
            new java.util.concurrent.ConcurrentHashMap<>(Map.of(1, taskWriter)));

        when(taskWriter.complete()).thenReturn(writeResult);
        when(writeResult.dataFiles()).thenReturn(new DataFile[]{mock(DataFile.class)});
        lenient().when(writeResult.deleteFiles()).thenReturn(new DeleteFile[0]);

        List<IWriteResult> results = writer.close();

        assertNotNull(results);
        assertEquals(1, results.size());
        verify(icebergTable, times(1)).close();
    }

    @Test
    void testExternalWriter_CreateTableReusesExistingIcebergTable() throws Exception {
        var writer = new IcebergExternalTableWriter(topic, entrySerdeFactory, config);
        injectField(writer, IcebergExternalTableWriter.class, "icebergTable", icebergTable);
        doNothing().when(icebergTable).create(any(TableOptions.class));

        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
        java.lang.reflect.Method createMethod =
            IcebergExternalTableWriter.class.getDeclaredMethod("createIcebergTable", Schema.class);
        createMethod.setAccessible(true);
        createMethod.invoke(writer, schema);

        // Should call create() on the EXISTING icebergTable, not construct a new one
        verify(icebergTable).create(any(TableOptions.class));
    }

    @Test
    void testExternalWriterWrapsRecordWithCdcOperation() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("iceberg.table.cdc-field", "_op");
        when(config.getProperties()).thenReturn(properties);

        var writer = new IcebergExternalTableWriter(topic, entrySerdeFactory, config);
        injectField(writer, IcebergExternalTableWriter.class, "icebergTable", icebergTable);

        Schema schema = new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "_op", Types.StringType.get()));
        when(table.schema()).thenReturn(schema);
        when(icebergTable.getTable()).thenReturn(table);

        Record record = GenericRecord.create(schema);
        record.setField("id", 7);
        record.setField("_op", "delete");

        injectField(writer, IcebergExternalTableWriter.class, "taskWriters",
            new ConcurrentHashMap<>(Map.of(record.struct().hashCode(), taskWriter)));

        Method doWrite = AbstractLakehouseWriter.class.getDeclaredMethod(
            "doWrite", AtomicBoolean.class, GenericEntry.class, MaterializationRecord.class, long.class);
        doWrite.setAccessible(true);

        Entry entry = new Entry(EntryHeader.NOT_FOUND, Unpooled.buffer(0));
        try {
            doWrite.invoke(
                writer,
                new AtomicBoolean(false),
                new GenericEntry(entry),
                new MaterializationRecord<>(record, Optional.empty()),
                System.nanoTime());
        } finally {
            entry.payload().release();
        }

        ArgumentCaptor<Record> captor = ArgumentCaptor.forClass(Record.class);
        verify(taskWriter, times(1)).write(captor.capture());
        assertTrue(captor.getValue() instanceof RecordWrapper);
        assertEquals(Operation.DELETE, ((RecordWrapper) captor.getValue()).op());
        assertEquals(7, captor.getValue().getField("id"));
    }

    // ======== IcebergManagedTableWriter close() tests ========

    @Test
    void testManagedWriter_CloseReleasesIcebergTable() throws Exception {
        var writer = new IcebergManagedTableWriter(topic, entrySerdeFactory, config, InstrumentProvider.NOOP);
        injectField(writer, IcebergManagedTableWriter.class, "icebergTable", icebergTable);

        writer.close();

        verify(icebergTable, times(1)).close();
    }

    @Test
    void testManagedWriter_CloseNullsIcebergTableToPreventDoubleClose() throws Exception {
        var writer = new IcebergManagedTableWriter(topic, entrySerdeFactory, config, InstrumentProvider.NOOP);
        injectField(writer, IcebergManagedTableWriter.class, "icebergTable", icebergTable);

        writer.close();
        writer.close(); // second close should be a no-op

        verify(icebergTable, times(1)).close();
    }

    @Test
    void testExternalWriter_DoWriteRoutesStructLikeNullAsMessageSerDeException() throws Exception {
        var writer = new IcebergExternalTableWriter(topic, entrySerdeFactory, config);
        injectField(writer, IcebergExternalTableWriter.class, "icebergTable", icebergTable);

        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Record record = org.apache.iceberg.data.GenericRecord.create(schema);
        record.setField("id", 1);
        when(icebergTable.getTable()).thenReturn(table);
        when(table.schema()).thenReturn(schema);

        @SuppressWarnings("unchecked")
        TaskWriter<Record> recordTaskWriter = mock(TaskWriter.class);
        var taskWriters = new java.util.concurrent.ConcurrentHashMap<Integer, TaskWriter<Record>>();
        taskWriters.put(record.struct().hashCode(), recordTaskWriter);
        injectField(writer, IcebergExternalTableWriter.class, "taskWriters", taskWriters);

        NullPointerException npe = new NullPointerException(
            "Cannot invoke \"org.apache.iceberg.StructLike.get(int, java.lang.Class)\" because \"struct\" is null"
        );
        npe.setStackTrace(new StackTraceElement[] {
            new StackTraceElement("org.apache.iceberg.parquet.ParquetValueWriters$RecordWriter",
                "get", "ParquetValueWriters.java", 402)
        });
        doThrow(npe).when(recordTaskWriter).write(any());

        MessageSerDeException error = assertThrows(
            MessageSerDeException.class,
            () -> writer.doWrite(new AtomicBoolean(false), mock(GenericEntry.class),
                new MaterializationRecord<>(record, Optional.empty()), 0L)
        );

        assertEquals(ExceptionCode.MESSAGE_SERIALIZE_TO_LAKEHOUSE_ERROR, error.getExceptionCode());
    }

    @Test
    void testExternalWriter_DoWriteRoutesNullValueAsMessageSerDeException() throws Exception {
        var writer = new IcebergExternalTableWriter(topic, entrySerdeFactory, config);
        injectField(writer, IcebergExternalTableWriter.class, "icebergTable", icebergTable);

        Schema schema = new Schema(
            Types.NestedField.required(1, "Metadata",
                Types.MapType.ofRequired(2, 3, Types.StringType.get(), Types.LongType.get())));
        Record record = org.apache.iceberg.data.GenericRecord.create(schema);
        Map<String, Long> metadata = new HashMap<>();
        metadata.put("SequenceId", 123L);
        metadata.put("ActorId", null);
        record.setField("Metadata", metadata);
        when(icebergTable.getTable()).thenReturn(table);
        when(table.schema()).thenReturn(schema);

        @SuppressWarnings("unchecked")
        TaskWriter<Record> recordTaskWriter = mock(TaskWriter.class);
        var taskWriters = new java.util.concurrent.ConcurrentHashMap<Integer, TaskWriter<Record>>();
        taskWriters.put(record.struct().hashCode(), recordTaskWriter);
        injectField(writer, IcebergExternalTableWriter.class, "taskWriters", taskWriters);

        NullPointerException npe = new NullPointerException(
            "Cannot invoke \"java.lang.Long.longValue()\" because \"value\" is null"
        );
        npe.setStackTrace(new StackTraceElement[] {
            new StackTraceElement("org.apache.iceberg.parquet.ParquetValueWriters$StringWriter",
                "write", "ParquetValueWriters.java", 370),
            new StackTraceElement("org.apache.iceberg.parquet.ParquetValueWriters$RepeatedKeyValueWriter",
                "write", "ParquetValueWriters.java", 592)
        });
        doThrow(npe).when(recordTaskWriter).write(any());

        MessageSerDeException error = assertThrows(
            MessageSerDeException.class,
            () -> writer.doWrite(new AtomicBoolean(false), mock(GenericEntry.class),
                new MaterializationRecord<>(record, Optional.empty()), 0L)
        );

        assertEquals(ExceptionCode.MESSAGE_SERIALIZE_TO_LAKEHOUSE_ERROR, error.getExceptionCode());
    }

    @Test
    void testExternalWriter_DoWriteRoutesRequiredNullValueAsMessageSerDeException() throws Exception {
        var writer = new IcebergExternalTableWriter(topic, entrySerdeFactory, config);
        injectField(writer, IcebergExternalTableWriter.class, "icebergTable", icebergTable);

        Schema schema = new Schema(Types.NestedField.required(1, "SequenceId", Types.LongType.get()));
        Record record = org.apache.iceberg.data.GenericRecord.create(schema);
        record.setField("SequenceId", null);

        Table localTable = createLocalIcebergTable(schema);
        when(icebergTable.getTable()).thenReturn(localTable);

        MessageSerDeException error = assertThrows(
            MessageSerDeException.class,
            () -> writer.doWrite(new AtomicBoolean(false), mock(GenericEntry.class),
                new MaterializationRecord<>(record, Optional.empty()), 0L)
        );

        assertEquals(ExceptionCode.MESSAGE_SERIALIZE_TO_LAKEHOUSE_ERROR, error.getExceptionCode());
    }

    @Test
    void testExternalWriter_DoWriteRoutesMapNullKeyAsMessageSerDeException() throws Exception {
        var writer = new IcebergExternalTableWriter(topic, entrySerdeFactory, config);
        injectField(writer, IcebergExternalTableWriter.class, "icebergTable", icebergTable);

        Schema schema = new Schema(
            Types.NestedField.required(1, "Metadata",
                Types.MapType.ofRequired(2, 3, Types.StringType.get(), Types.LongType.get())));
        Record record = org.apache.iceberg.data.GenericRecord.create(schema);
        Map<String, Long> metadata = new HashMap<>();
        metadata.put(null, 123L);
        record.setField("Metadata", metadata);

        Table localTable = createLocalIcebergTable(schema);
        when(icebergTable.getTable()).thenReturn(localTable);

        MessageSerDeException error = assertThrows(
            MessageSerDeException.class,
            () -> writer.doWrite(new AtomicBoolean(false), mock(GenericEntry.class),
                new MaterializationRecord<>(record, Optional.empty()), 0L)
        );

        assertEquals(ExceptionCode.MESSAGE_SERIALIZE_TO_LAKEHOUSE_ERROR, error.getExceptionCode());
    }

    @Test
    void testIcebergWriter_ThrowsWhenMapKeyIsNull() throws Exception {
        Schema schema = new Schema(
            Types.NestedField.required(1, "Metadata",
                Types.MapType.ofRequired(2, 3, Types.StringType.get(), Types.LongType.get())));
        Record record = org.apache.iceberg.data.GenericRecord.create(schema);
        Map<String, Long> metadata = new HashMap<>();
        metadata.put(null, 123L);
        record.setField("Metadata", metadata);

        Table localTable = createLocalIcebergTable(schema);

        try (TaskWriter<Record> localTaskWriter = Utilities.createTableWriter(
            localTable, schema, 0, new IcebergSinkConfig(new Properties()))) {
            Throwable error = assertThrows(Throwable.class, () -> localTaskWriter.write(record));
            assertTrue(error instanceof NullPointerException);
            assertTrue(error.getMessage().contains("CharSequence.toString()"));
            assertTrue(error.getMessage().contains("\"value\" is null"));
        }
    }

    @Test
    void testIcebergWriter_ThrowsWhenRequiredFieldValueIsNull() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "SequenceId", Types.LongType.get()));
        Record record = org.apache.iceberg.data.GenericRecord.create(schema);
        record.setField("SequenceId", null);

        Table localTable = createLocalIcebergTable(schema);

        try (TaskWriter<Record> localTaskWriter = Utilities.createTableWriter(
            localTable, schema, 0, new IcebergSinkConfig(new Properties()))) {
            Throwable error = assertThrows(Throwable.class, () -> localTaskWriter.write(record));
            assertTrue(error instanceof NullPointerException);
            assertTrue(error.getMessage().contains("Long.longValue()"));
            assertTrue(error.getMessage().contains("\"value\" is null"));
        }
    }

    private Table createLocalIcebergTable(Schema schema) {
        Table localTable = mock(Table.class);
        when(localTable.schema()).thenReturn(schema);
        when(localTable.spec()).thenReturn(PartitionSpec.unpartitioned());
        when(localTable.io()).thenReturn(new InMemoryFileIO());
        when(localTable.locationProvider())
            .thenReturn(LocationProviders.locationsFor("file", ImmutableMap.of()));
        when(localTable.encryption()).thenReturn(PlaintextEncryptionManager.instance());
        when(localTable.properties()).thenReturn(Map.of());
        return localTable;
    }

    // ======== ReferencedCatalog refcount leak verification ========

    /**
     * Demonstrates the resource leak: if IcebergTable.close() is never called,
     * the refcount stays positive and the catalog is never released.
     */
    @Test
    void testReferencedCatalog_LeaksWhenIcebergTableNotClosed() throws IOException {
        Catalog mockCatalog = mock(Catalog.class, withSettings().extraInterfaces(Closeable.class));
        when(mockCatalog.name()).thenReturn("leak-test");
        ReferencedCatalog referencedCatalog = new ReferencedCatalog(mockCatalog, Duration.ofMillis(50));

        referencedCatalog.retain();
        assertEquals(1, referencedCatalog.getRefCount());

        // Without IcebergTable.close(), refcount stays at 1 — catalog can never be closed
        assertFalse(referencedCatalog.isClosed());
        verify((Closeable) mockCatalog, never()).close();
    }

    @Test
    void testReferencedCatalog_ProperlyReleasedWhenClosed() throws Exception {
        Catalog mockCatalog = mock(Catalog.class, withSettings().extraInterfaces(Closeable.class));
        when(mockCatalog.name()).thenReturn("release-test");
        ReferencedCatalog referencedCatalog = new ReferencedCatalog(mockCatalog, Duration.ofMillis(50));

        referencedCatalog.retain();
        Thread.sleep(100);
        assertTrue(referencedCatalog.isExpired());

        referencedCatalog.release();
        assertEquals(0, referencedCatalog.getRefCount());
        assertTrue(referencedCatalog.isClosed());
        verify((Closeable) mockCatalog, times(1)).close();
    }

    /**
     * Reproduces the old bug in IcebergExternalTableWriter.getIcebergTable():
     * it created a SECOND IcebergTable (retain #2) without closing the first,
     * leaking one reference and preventing the catalog from ever being closed.
     */
    @Test
    void testReferencedCatalog_LeakedReferencePreventsCatalogClose() throws Exception {
        Catalog mockCatalog = mock(Catalog.class, withSettings().extraInterfaces(Closeable.class));
        when(mockCatalog.name()).thenReturn("double-ref-test");
        ReferencedCatalog referencedCatalog = new ReferencedCatalog(mockCatalog, Duration.ofMillis(50));

        // Old bug: two IcebergTable instances retain the same catalog
        referencedCatalog.retain(); // First IcebergTable (from constructor)
        referencedCatalog.retain(); // Second IcebergTable (from old getIcebergTable())
        assertEquals(2, referencedCatalog.getRefCount());

        Thread.sleep(100);
        assertTrue(referencedCatalog.isExpired());

        // Only one close() is called — the other reference was leaked
        referencedCatalog.release(); // refCnt: 2→1
        assertEquals(1, referencedCatalog.getRefCount());
        assertFalse(referencedCatalog.isClosed());
        verify((Closeable) mockCatalog, never()).close();
    }

    @Test
    void testReferencedCatalog_FixedSingleRetainAndRelease() throws Exception {
        Catalog mockCatalog = mock(Catalog.class, withSettings().extraInterfaces(Closeable.class));
        when(mockCatalog.name()).thenReturn("fixed-test");
        ReferencedCatalog referencedCatalog = new ReferencedCatalog(mockCatalog, Duration.ofMillis(50));

        // Fixed: only one retain, properly released
        referencedCatalog.retain();
        Thread.sleep(100);
        referencedCatalog.release();

        assertEquals(0, referencedCatalog.getRefCount());
        assertTrue(referencedCatalog.isClosed());
        verify((Closeable) mockCatalog, times(1)).close();
    }

    private static void injectField(Object target, Class<?> clazz, String fieldName, Object value) throws Exception {
        java.lang.reflect.Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
