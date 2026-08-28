/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.io.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.v2.serde.iceberg.test.TestProtoMessages;
import io.lakestream.ursa.metrics.InstrumentProvider;
import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;

public class ParquetFileWriterTest {


    @Test
    public void testAvroParquetWriterConfiguration() throws Exception {
        Properties props = new Properties();
        props.setProperty("parquetRowGroupSize", "16777216");
        props.setProperty("parquetRowGroupRowCountLimit", "20000");
        props.setProperty("parquetPageSize", "2097152");
        props.setProperty("parquetPageRowCountLimit", "15000");
        props.setProperty("parquetMinRowCountForPageSizeCheck", "20");
        props.setProperty("parquetMaxRowCountForPageSizeCheck", "20000");

        LakehouseConfiguration config = new LakehouseConfiguration(props);

        long expectedRowGroupSize = 16777216L;
        int expectedRowGroupRowCountLimit = 20000;
        int expectedPageSize = 2097152;
        int expectedPageRowCountLimit = 15000;
        int expectedMinRowCountForPageSizeCheck = 20;
        int expectedMaxRowCountForPageSizeCheck = 20000;

        String path = "/tmp/test_delta-" + UUID.randomUUID();
        List<Schema.Field> fields = new ArrayList<>();
        Schema schema = Schema.createRecord("test", "", "", false);
        fields.add(new Schema.Field("name", Schema.create(Schema.Type.STRING)));
        schema.setFields(fields);

        ParquetFileWriter writer = new ParquetFileWriter(new File(path).toURI(), config, InstrumentProvider.NOOP);

        GenericData.Record record = new GenericData.Record(schema);
        record.put("name", "test");
        writer.write(record, new HashMap<>());

        ParquetWriter<?> parquetWriter = getParquetWriter(writer);
        Object internalWriter = getInternalParquetRecordWriter(parquetWriter);
        Object parquetProperties = getParquetProperties(parquetWriter);

        assertEquals(expectedRowGroupSize, getRowGroupSizeThreshold(internalWriter));
        assertEquals(expectedRowGroupRowCountLimit, getFieldValue(parquetProperties, "rowGroupRowCountLimit"));
        assertEquals(expectedPageSize, getFieldValue(parquetProperties, "pageSizeThreshold"));
        assertEquals(expectedPageRowCountLimit, getFieldValue(parquetProperties, "pageRowCountLimit"));
        assertEquals(expectedMinRowCountForPageSizeCheck,
            getFieldValue(parquetProperties, "minRowCountForPageSizeCheck"));
        assertEquals(expectedMaxRowCountForPageSizeCheck,
            getFieldValue(parquetProperties, "maxRowCountForPageSizeCheck"));

        writer.close();

        new File(path).deleteOnExit();
    }

    @Test
    public void testProtoParquetWriterConfiguration() throws Exception {
        Properties props = new Properties();
        props.setProperty("parquetRowGroupSize", "16777216");
        props.setProperty("parquetRowGroupRowCountLimit", "20000");
        props.setProperty("parquetPageSize", "2097152");
        props.setProperty("parquetPageRowCountLimit", "15000");
        props.setProperty("parquetMinRowCountForPageSizeCheck", "20");
        props.setProperty("parquetMaxRowCountForPageSizeCheck", "20000");

        LakehouseConfiguration config = new LakehouseConfiguration(props);

        long expectedRowGroupSize = 16777216L;
        int expectedRowGroupRowCountLimit = 20000;
        int expectedPageSize = 2097152;
        int expectedPageRowCountLimit = 15000;
        int expectedMinRowCountForPageSizeCheck = 20;
        int expectedMaxRowCountForPageSizeCheck = 20000;

        String path = "/tmp/test_delta-" + UUID.randomUUID();

        ParquetFileWriter writer = new ParquetFileWriter(new File(path).toURI(), config, InstrumentProvider.NOOP);

        var protoMessage = TestProtoMessages.Address.newBuilder()
            .setStreet("street")
            .build();
        writer.write(protoMessage, new HashMap<>());

        ParquetWriter<?> parquetWriter = getParquetWriter(writer);
        Object internalWriter = getInternalParquetRecordWriter(parquetWriter);
        Object parquetProperties = getParquetProperties(parquetWriter);

        assertEquals(expectedRowGroupSize, getRowGroupSizeThreshold(internalWriter));
        assertEquals(expectedRowGroupRowCountLimit, getFieldValue(parquetProperties, "rowGroupRowCountLimit"));
        assertEquals(expectedPageSize, getFieldValue(parquetProperties, "pageSizeThreshold"));
        assertEquals(expectedPageRowCountLimit, getFieldValue(parquetProperties, "pageRowCountLimit"));
        assertEquals(expectedMinRowCountForPageSizeCheck,
            getFieldValue(parquetProperties, "minRowCountForPageSizeCheck"));
        assertEquals(expectedMaxRowCountForPageSizeCheck,
            getFieldValue(parquetProperties, "maxRowCountForPageSizeCheck"));

        writer.close();

        new File(path).deleteOnExit();
    }

    @Test
    public void testDefaultParquetWriterConfiguration() throws Exception {
        Properties props = new Properties();
        LakehouseConfiguration config = new LakehouseConfiguration(props);

        String path = "/tmp/test_delta-" + UUID.randomUUID();
        List<Schema.Field> fields = new ArrayList<>();
        Schema schema = Schema.createRecord("test", "", "", false);
        fields.add(new Schema.Field("name", Schema.create(Schema.Type.STRING)));
        schema.setFields(fields);

        ParquetFileWriter writer = new ParquetFileWriter(new File(path).toURI(), config, InstrumentProvider.NOOP);

        GenericData.Record record = new GenericData.Record(schema);
        record.put("name", "test");
        writer.write(record, new HashMap<>());

        ParquetWriter<?> parquetWriter = getParquetWriter(writer);
        Object internalWriter = getInternalParquetRecordWriter(parquetWriter);
        Object parquetProperties = getParquetProperties(parquetWriter);

        long defaultRowGroupSize = 8 * 1024 * 1024L;
        int defaultRowGroupRowCountLimit = Integer.MAX_VALUE;
        int defaultPageSize = 1048576;
        int defaultPageRowCountLimit = 20000;
        int defaultMinRowCountForPageSizeCheck = 10;
        int defaultMaxRowCountForPageSizeCheck = 10000;

        assertEquals(defaultRowGroupSize, getRowGroupSizeThreshold(internalWriter));
        assertEquals(defaultRowGroupRowCountLimit, getFieldValue(parquetProperties, "rowGroupRowCountLimit"));
        assertEquals(defaultPageSize, getFieldValue(parquetProperties, "pageSizeThreshold"));
        assertEquals(defaultPageRowCountLimit, getFieldValue(parquetProperties, "pageRowCountLimit"));
        assertEquals(defaultMinRowCountForPageSizeCheck,
            getFieldValue(parquetProperties, "minRowCountForPageSizeCheck"));
        assertEquals(defaultMaxRowCountForPageSizeCheck,
            getFieldValue(parquetProperties, "maxRowCountForPageSizeCheck"));

        writer.close();

        new File(path).deleteOnExit();
    }

    private ParquetWriter<?> getParquetWriter(
        io.lakestream.ursa.lakehouse.v2.io.parquet.ParquetFileWriter<?> writer) throws Exception {
        Field field = io.lakestream.ursa.lakehouse.v2.io.parquet.ParquetFileWriter.class.getDeclaredField("parquetWriter");
        field.setAccessible(true);
        return (ParquetWriter<?>) field.get(writer);
    }

    private Object getInternalParquetRecordWriter(ParquetWriter<?> writer) throws Exception {
        Field writerField = writer.getClass().getDeclaredField("writer");
        writerField.setAccessible(true);
        return writerField.get(writer);
    }

    private Object getParquetProperties(ParquetWriter<?> writer) throws Exception {
        Object internalWriter = getInternalParquetRecordWriter(writer);
        Field propsField = internalWriter.getClass().getDeclaredField("props");
        propsField.setAccessible(true);
        return propsField.get(internalWriter);
    }

    private long getRowGroupSizeThreshold(Object internalWriter) throws Exception {
        Field field = internalWriter.getClass().getDeclaredField("rowGroupSizeThreshold");
        field.setAccessible(true);
        return (long) field.get(internalWriter);
    }

    private Object getFieldValue(Object obj, String fieldName) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(obj);
    }
}
