/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.LakehouseOptException;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.compact.FailureMessage;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class IcebergExternalDLTTableWriterTest {

    @Mock
    private LakehouseConfiguration config;

    @Mock
    private InstrumentProvider instrumentProvider;

    @Mock
    private IcebergTable icebergTable;

    @Mock
    private Table table;

    @Mock
    private TaskWriter<Record> taskWriter;

    @Mock
    private WriteResult writeResult;

    private IcebergExternalDLTTableWriter writer;

    private final String topic = "namespace/topic";

    @BeforeEach
    void setUp() {
        lenient().when(config.getProperties()).thenReturn(new Properties());
        lenient().when(config.getIcebergTableProperties()).thenReturn(Map.of());
        writer = new IcebergExternalDLTTableWriter(topic, config, instrumentProvider);
    }

    @Test
    void testToRecordWithPayload() {
        // Given
        ByteBuf payload = Unpooled.copiedBuffer("test payload", StandardCharsets.UTF_8);
        FailureMessage msg = FailureMessage.builder()
                .topic(topic)
                .messageId("123:456:0")
                .payload(payload)
                .failureReason("Test failure")
                .build();

        // When
        Record record = IcebergExternalDLTTableWriter.toRecord(msg);

        // Then
        assertNotNull(record);
        assertEquals("123:456:0", record.getField("messageId"));
        assertNotNull(record.getField("payload")); // Base64 encoded
        assertEquals("Test failure", record.getField("failureReason"));

        // Cleanup
        msg.release();
    }

    @Test
    void testToRecordWithoutPayload() {
        // Given
        FailureMessage msg = FailureMessage.builder()
                .topic(topic)
                .messageId("123:456:0")
                .payload(null)
                .failureReason("Test failure")
                .build();

        // When
        Record record = IcebergExternalDLTTableWriter.toRecord(msg);

        // Then
        assertNotNull(record);
        assertEquals("123:456:0", record.getField("messageId"));
        assertNull(record.getField("payload"));
        assertEquals("Test failure", record.getField("failureReason"));
    }

    @Test
    void testWriteSuccess() throws Exception {
        // Given
        FailureMessage msg = FailureMessage.builder()
                .topic(topic)
                .messageId("123:456:0")
                .payload(null)
                .failureReason("Test failure")
                .build();

        IcebergExternalDLTTableWriter spyWriter = spy(writer);

        // Set up mocks
        java.lang.reflect.Field icebergTableField = IcebergExternalDLTTableWriter.class.getDeclaredField("icebergTable");
        icebergTableField.setAccessible(true);
        icebergTableField.set(spyWriter, icebergTable);

        java.lang.reflect.Field taskWriterField = IcebergExternalDLTTableWriter.class.getDeclaredField("taskWriter");
        taskWriterField.setAccessible(true);
        taskWriterField.set(spyWriter, taskWriter);

        lenient().when(icebergTable.getTable()).thenReturn(table);
        doNothing().when(icebergTable).createIfAbsent();

        // When
        spyWriter.write(msg);

        // Then
        verify(taskWriter, times(1)).write(any(Record.class));
    }

    @Test
    void testWriteThrowsExceptionOnTableCreation() throws Exception {
        // Given
        FailureMessage msg = FailureMessage.builder()
                .topic(topic)
                .messageId("123:456:0")
                .payload(null)
                .failureReason("Test failure")
                .build();

        IcebergExternalDLTTableWriter freshWriter = new IcebergExternalDLTTableWriter(topic, config, instrumentProvider) {
            @Override
            protected void beforeWrite() throws LakehouseOptException {
                throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_CREATE_TABLE_ERROR,
                        new RuntimeException("Table creation failed"));
            }
        };

        // When & Then
        LakehouseOptException exception = assertThrows(LakehouseOptException.class, () -> freshWriter.write(msg));
        assertEquals(ExceptionCode.LAKEHOUSE_CREATE_TABLE_ERROR, exception.getExceptionCode());
    }

    @Test
    void testCloseWithPendingWrites() throws Exception {
        // Given
        IcebergExternalDLTTableWriter spyWriter = spy(writer);

        // Set up mocks
        java.lang.reflect.Field icebergTableField = IcebergExternalDLTTableWriter.class.getDeclaredField("icebergTable");
        icebergTableField.setAccessible(true);
        icebergTableField.set(spyWriter, icebergTable);

        java.lang.reflect.Field taskWriterField = IcebergExternalDLTTableWriter.class.getDeclaredField("taskWriter");
        taskWriterField.setAccessible(true);
        taskWriterField.set(spyWriter, taskWriter);

        DataFile[] dataFiles = new DataFile[]{mock(DataFile.class)};
        DeleteFile[] deleteFiles = new DeleteFile[0];
        when(taskWriter.complete()).thenReturn(writeResult);
        when(writeResult.dataFiles()).thenReturn(dataFiles);
        lenient().when(writeResult.deleteFiles()).thenReturn(deleteFiles);

        // When
        List<IWriteResult> results = spyWriter.close();

        // Then
        assertNotNull(results);
        assertEquals(1, results.size());
        assertTrue(results.get(0) instanceof IcebergWriteResult);
        verify(taskWriter, times(1)).complete();
        verify(icebergTable, times(1)).close();
    }

    @Test
    void testCloseWithoutWrites() throws Exception {
        // When
        List<IWriteResult> results = writer.close();

        // Then
        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    void testCloseWithEmptyWriteResult() throws Exception {
        // Given
        IcebergExternalDLTTableWriter spyWriter = spy(writer);

        // Set up mocks
        java.lang.reflect.Field taskWriterField = IcebergExternalDLTTableWriter.class.getDeclaredField("taskWriter");
        taskWriterField.setAccessible(true);
        taskWriterField.set(spyWriter, taskWriter);

        DataFile[] dataFiles = new DataFile[0];
        DeleteFile[] deleteFiles = new DeleteFile[0];
        when(taskWriter.complete()).thenReturn(writeResult);
        when(writeResult.dataFiles()).thenReturn(dataFiles);
        when(writeResult.deleteFiles()).thenReturn(deleteFiles);

        // When
        List<IWriteResult> results = spyWriter.close();

        // Then
        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    void testCloseWithException() throws Exception {
        // Given
        IcebergExternalDLTTableWriter spyWriter = spy(writer);

        // Set up mocks
        java.lang.reflect.Field taskWriterField = IcebergExternalDLTTableWriter.class.getDeclaredField("taskWriter");
        taskWriterField.setAccessible(true);
        taskWriterField.set(spyWriter, taskWriter);

        when(taskWriter.complete()).thenThrow(new IOException("Write failed"));

        // When & Then
        LakehouseOptException exception = assertThrows(LakehouseOptException.class, spyWriter::close);
        assertEquals(ExceptionCode.LAKEHOUSE_WRITE_ERROR, exception.getExceptionCode());
    }

    @Test
    void testMultipleWrites() throws Exception {
        // Given
        FailureMessage msg1 = FailureMessage.builder()
                .topic(topic)
                .messageId("1:1:0")
                .payload(null)
                .failureReason("Failure 1")
                .build();

        FailureMessage msg2 = FailureMessage.builder()
                .topic(topic)
                .messageId("2:2:0")
                .payload(Unpooled.copiedBuffer("payload2", StandardCharsets.UTF_8))
                .failureReason("Failure 2")
                .build();

        IcebergExternalDLTTableWriter spyWriter = spy(writer);

        // Set up mocks
        java.lang.reflect.Field icebergTableField = IcebergExternalDLTTableWriter.class.getDeclaredField("icebergTable");
        icebergTableField.setAccessible(true);
        icebergTableField.set(spyWriter, icebergTable);

        java.lang.reflect.Field taskWriterField = IcebergExternalDLTTableWriter.class.getDeclaredField("taskWriter");
        taskWriterField.setAccessible(true);
        taskWriterField.set(spyWriter, taskWriter);

        lenient().when(icebergTable.getTable()).thenReturn(table);
        doNothing().when(icebergTable).createIfAbsent();

        // When
        spyWriter.write(msg1);
        spyWriter.write(msg2);

        // Then
        verify(taskWriter, times(2)).write(any(Record.class));

        // Cleanup
        msg2.release();
    }
}
