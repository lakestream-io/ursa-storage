/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import io.lakestream.ursa.lakehouse.delta.DirectExternalTable;
import io.lakestream.ursa.lakehouse.delta.ExternalDeltaTable;
import io.lakestream.ursa.lakehouse.delta.GenericRow;
import io.lakestream.ursa.lakehouse.delta.ParquetRowWriter;
import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DeltaExternalDLTTableWriterTest {

    @Mock
    private LakehouseConfiguration config;

    @Mock
    private InstrumentProvider instrumentProvider;

    @Mock
    private ParquetRowWriter parquetRowWriter;

    private DeltaExternalDLTTableWriter writer;

    private final String topic = "namespace/topic";

    @BeforeEach
    void setUp() {
        lenient().when(config.getDeltaKernelWriteBatchSize()).thenReturn(1000);
        lenient().when(config.getStoragePath()).thenReturn("/tmp");
        lenient().when(config.getHadoopConfiguration()).thenReturn(new Configuration());
        lenient().when(config.getDirectExternalStoragePath()).thenReturn("/tmp/direct-external");
        lenient().when(config.getDltSuffix()).thenReturn(LakehouseConfiguration.DEFAULT_DLT_SUFFIX);
        lenient().when(config.getProperties()).thenReturn(new Properties());
        lenient().when(config.getStreamTableMode()).thenReturn(LakehouseConfiguration.StreamTableMode.EXTERNAL);
        writer = new DeltaExternalDLTTableWriter(topic, config, instrumentProvider);
    }

    @Test
    void testToGenericRowWithPayload() {
        ByteBuf payload = Unpooled.copiedBuffer("test payload", StandardCharsets.UTF_8);
        FailureMessage msg = FailureMessage.builder()
                .topic(topic)
                .messageId("123:456:0")
                .payload(payload)
                .failureReason("Test failure")
                .build();

        GenericRow row = DeltaExternalDLTTableWriter.toGenericRow(msg);

        assertNotNull(row);
        assertEquals("123:456:0", row.getString(0));
        assertNotNull(row.getString(1));
        assertEquals("Test failure", row.getString(2));

        msg.release();
    }

    @Test
    void testToGenericRowWithoutPayload() {
        FailureMessage msg = FailureMessage.builder()
                .topic(topic)
                .messageId("123:456:0")
                .payload(null)
                .failureReason("Test failure")
                .build();

        GenericRow row = DeltaExternalDLTTableWriter.toGenericRow(msg);

        assertNotNull(row);
        assertEquals("123:456:0", row.getString(0));
        assertNull(row.getString(1));
        assertEquals("Test failure", row.getString(2));
    }

//    @Test
//    void testWriteSuccess() throws Exception {
//        FailureMessage msg = FailureMessage.builder()
//                .topic(topic)
//                .messageId("123:456:0")
//                .payload(null)
//                .failureReason("Test failure")
//                .build();
//
//        try (MockedStatic<UnityCatalogApi> mockedStatic = Mockito.mockStatic(UnityCatalogApi.class)) {
//            mockedStatic.when(() -> UnityCatalogApi.getInstance(config)).thenReturn(unityCatalogApi);
//
//            when(unityCatalogApi.getTable(anyString())).thenReturn(Optional.empty());
//            when(unityCatalogApi.getCatalog(anyString())).thenReturn(Optional.of(catalogInfo));
//            when(catalogInfo.getStorageRoot()).thenReturn("s3://bucket/root");
//            when(unityCatalogApi.createTable(anyString(), anyString(), any(StructType.class))).thenReturn(tableInfo);
//            when(tableInfo.getStorageLocation()).thenReturn("s3://bucket/root/table");
//            when(tableInfo.getTableId()).thenReturn("table-id");
//            when(unityCatalogApi.getTemporaryTableCredentials(anyString(), any(TableOperation.class)))
//                    .thenReturn(credentials);
//
//            DeltaExternalDLTTableWriter spyWriter = spy(writer);
//            java.lang.reflect.Field field = DeltaExternalDLTTableWriter.class.getDeclaredField("parquetRowWriter");
//            field.setAccessible(true);
//            field.set(spyWriter, parquetRowWriter);
//
//            spyWriter.write(msg);
//
//            verify(parquetRowWriter, times(1)).write(any(GenericRow.class));
//        }
//    }

    @Test
    void testWriteThrowsExceptionOnTableCreationFailure() throws Exception {
        FailureMessage msg = FailureMessage.builder()
                .topic(topic)
                .messageId("123:456:0")
                .payload(null)
                .failureReason("Test failure")
                .build();

        ExternalDeltaTable externalDeltaTable = mock(DirectExternalTable.class);
        when(externalDeltaTable.tableExists()).thenThrow(new RuntimeException("Table lookup failed"));
        Field field = DeltaExternalDLTTableWriter.class.getDeclaredField("deltaTable");
        field.setAccessible(true);
        field.set(writer, externalDeltaTable);

        LakehouseOptException exception = assertThrows(LakehouseOptException.class, () -> writer.write(msg));
        assertEquals(ExceptionCode.LAKEHOUSE_CREATE_TABLE_ERROR, exception.getExceptionCode());
    }

    @Test
    void testCloseWithPendingWrites() throws Exception {
//        ParquetFileStat fileStat = ParquetFileStat.builder()
//                .filePath("test.parquet")
//                .recordCount(100)
//                .build();

        ParquetFileStat fileStat = new ParquetFileStat("test.parquet", "/tmp/test.parquet", 1000L, "",
                Collections.emptyMap(), Collections.emptyMap());

        DeltaExternalDLTTableWriter spyWriter = spy(writer);
        java.lang.reflect.Field field = DeltaExternalDLTTableWriter.class.getDeclaredField("parquetRowWriter");
        field.setAccessible(true);
        field.set(spyWriter, parquetRowWriter);

        when(parquetRowWriter.close()).thenReturn(List.of(fileStat));

        List<IWriteResult> results = spyWriter.close();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertTrue(results.get(0) instanceof DeltaWriteResult);
        verify(parquetRowWriter, times(1)).close();
    }

    @Test
    void testCloseWithoutWrites() throws Exception {
        List<IWriteResult> results = writer.close();
        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    void testCloseWithException() throws Exception {
        DeltaExternalDLTTableWriter spyWriter = spy(writer);
        java.lang.reflect.Field field = DeltaExternalDLTTableWriter.class.getDeclaredField("parquetRowWriter");
        field.setAccessible(true);
        field.set(spyWriter, parquetRowWriter);

        when(parquetRowWriter.close()).thenThrow(new RuntimeException("Close failed"));

        LakehouseOptException exception = assertThrows(LakehouseOptException.class, spyWriter::close);
        assertEquals(ExceptionCode.LAKEHOUSE_WRITE_ERROR, exception.getExceptionCode());
    }
}
