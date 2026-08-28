/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.LakehouseOptException;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.delta.GenericRow;
import io.lakestream.ursa.lakehouse.delta.ParquetRowWriter;
import io.lakestream.ursa.lakehouse.delta.UCExternalTable;
import io.lakestream.ursa.materialization.serde.EntryEncoder;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DeltaExternalTableWriterSchemaServiceTest {

    @Mock
    private LakehouseConfiguration configuration;

    @Mock
    private EntrySerdeFactory entrySerdeFactory;

    @Mock
    private EntryEncoder<Object> encoder;

    @Mock
    private UCExternalTable deltaExternalTable;

    @Mock
    private ParquetRowWriter parquetRowWriter;

    private Map<Integer, ParquetRowWriter> parquetRowWriterMap;

    private DeltaExternalTableWriter writer;
    private final String topic = "namespace/topic-partition-0";
    private final String parentTopic = "namespace/topic";

    @BeforeEach
    void setUp() {
        lenient().when(configuration.getProperties()).thenReturn(new Properties());
        lenient().when(configuration.getPartitionKey()).thenReturn("");
        lenient().when(configuration.getUnityCatalogName()).thenReturn("test-catalog");
        lenient().when(configuration.isMockUnityCatalog()).thenReturn(true);
        lenient().when(configuration.getStoragePath()).thenReturn("/tmp");
        lenient().when(configuration.getHadoopConfiguration()).thenReturn(new Configuration());
        lenient().when(configuration.getDeltaKernelWriteBatchSize()).thenReturn(1000);

        lenient().when(entrySerdeFactory.getEncoder(EntrySerdeFactory.SerdeType.KAFKA_DELTA)).thenReturn(encoder);

        writer = new DeltaExternalTableWriter(topic, entrySerdeFactory, configuration, InstrumentProvider.NOOP);
        parquetRowWriterMap = new HashMap<>();
    }

    @Test
    void testWriteCallsEncoderWithLakehouseTableSchemaService() throws Exception {
        // Given
        ByteBuf payload = Unpooled.copiedBuffer("test payload", StandardCharsets.UTF_8);
        Entry entry = new Entry(EntryHeader.NOT_FOUND, payload);
        GenericEntry genericEntry = new GenericEntry(entry, Optional.empty());

        StructType schema = new StructType()
                .add(new StructField("id", io.delta.kernel.types.LongType.LONG, false))
                .add(new StructField("name", io.delta.kernel.types.StringType.STRING, false));

        Map<Integer, Object> values = new HashMap<>();
        values.put(1, 1L);
        values.put(2, "test");
        GenericRow record = new GenericRow(schema, values);

        MaterializationRecord<Object> lakehouseEntry = new MaterializationRecord<>(record, Optional.empty());

        // Mock the encoder to call onResult
        doAnswer(invocation -> {
            ResultConsumer<MaterializationRecord<Object>> consumer = invocation.getArgument(2);
            consumer.onResult(lakehouseEntry);
            return null;
        }).when(encoder).encode(anyString(), any(GenericEntry.class), any(ResultConsumer.class), any(),
            any(EntryEncoderContext.class));

        // Set up writer with mocked delta committer
        setPrivateField(writer, "deltaTable", deltaExternalTable);
        setPrivateField(writer, "parquetRowWriterMap", parquetRowWriterMap);
        parquetRowWriterMap.put(schema.hashCode(), parquetRowWriter);

        // When
        writer.write(genericEntry);

        // Then
        ArgumentCaptor<TableSchemaService> schemaServiceCaptor =
                ArgumentCaptor.forClass(TableSchemaService.class);
        verify(encoder).encode(eq(topic), eq(genericEntry), any(ResultConsumer.class), schemaServiceCaptor.capture(),
            any(
            EntryEncoderContext.class));

        // Verify null is passed as the schema service (Delta doesn't override getLakehouseTableSchemaService)
        assertNull(schemaServiceCaptor.getValue());
    }

    @Test
    void testGetLakehouseTableSchemaServiceReturnsNull() {
        // When
        TableSchemaService result = writer.getLakehouseTableSchemaService();

        // Then
        assertNull(result);
    }

    @Test
    void testInitializeDeltaWriterWithoutUnityCatalogThrowsException() throws Exception {
        // Given
        lenient().when(configuration.isMockUnityCatalog()).thenReturn(false);
        lenient().when(configuration.getStreamTableMode())
            .thenReturn(LakehouseConfiguration.StreamTableMode.MANAGED);

        try {
            writer = new DeltaExternalTableWriter(topic, entrySerdeFactory, configuration, InstrumentProvider.NOOP);
            fail();
        } catch (Exception e) {
            assertEquals("Delta uc table must enable Unity catalog.", e.getMessage());
        }
    }

    @Test
    void testDoWriteWithParquetRowWriter() throws Exception {
        // Given
        StructType schema = new StructType()
                .add(new StructField("id", io.delta.kernel.types.LongType.LONG, false));

        GenericRow record = new GenericRow(schema, Map.of(1, 1L));
        MaterializationRecord<Object> lakehouseEntry = new MaterializationRecord<>(record, Optional.empty());

        ByteBuf payload = Unpooled.copiedBuffer("test", StandardCharsets.UTF_8);
        Entry entry = new Entry(EntryHeader.NOT_FOUND, payload);
        GenericEntry genericEntry = new GenericEntry(entry, Optional.empty());

        // Set up writer with mocked parquet writer
        setPrivateField(writer, "parquetRowWriterMap", parquetRowWriterMap);
        parquetRowWriterMap.put(schema.hashCode(), parquetRowWriter);

        // When
        writer.doWrite(new AtomicBoolean(), genericEntry, lakehouseEntry, System.nanoTime());

        // Then
        verify(parquetRowWriter).write(record);
    }

    @Test
    void testDoWriteThrowsExceptionOnWriteFailure() throws Exception {
        // Given
        StructType schema = new StructType()
                .add(new StructField("id", io.delta.kernel.types.LongType.LONG, false));

        GenericRow record = new GenericRow(schema, Map.of(1, 1L));
        MaterializationRecord<Object> lakehouseEntry = new MaterializationRecord<>(record, Optional.empty());

        ByteBuf payload = Unpooled.copiedBuffer("test", StandardCharsets.UTF_8);
        Entry entry = new Entry(EntryHeader.NOT_FOUND, payload);
        GenericEntry genericEntry = new GenericEntry(entry, Optional.empty());

        // Set up writer with mocked parquet writer that throws exception
        setPrivateField(writer, "parquetRowWriterMap", parquetRowWriterMap);
        doThrow(new java.io.IOException("Write failed")).when(parquetRowWriter).write(any());
        parquetRowWriterMap.put(schema.hashCode(), parquetRowWriter);

        // When & Then
        LakehouseOptException exception = assertThrows(LakehouseOptException.class,
                () -> writer.doWrite(new AtomicBoolean(), genericEntry, lakehouseEntry, System.nanoTime()));

        assertEquals(ExceptionCode.LAKEHOUSE_WRITE_ERROR, exception.getExceptionCode());
    }

    private void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
