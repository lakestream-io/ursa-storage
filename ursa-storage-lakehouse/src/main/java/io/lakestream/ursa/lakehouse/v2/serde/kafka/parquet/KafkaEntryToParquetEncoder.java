/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.serde.kafka.parquet;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.MessageSerDeException;
import io.lakestream.ursa.materialization.serde.EntryEncoder;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.SchemaKey;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaEntryEncoder;
import io.lakestream.ursa.materialization.serde.utils.json.JsonAvroConverter;
import io.lakestream.ursa.materialization.serde.utils.json.schema.JsonSchema;
import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class KafkaEntryToParquetEncoder extends KafkaEntryEncoder<GenericRecord>
    implements EntryEncoder<GenericRecord> {

    static final String PRIMITIVE_RECORD_NAME = "KafkaMessage";
    static final String PRIMITIVE_RECORD_FIELD_NAME = "payload";

    public KafkaEntryToParquetEncoder(SchemaService schemaService) {
        super(schemaService);
    }

    @Override
    protected GenericRecord transform(Object object, SchemaMetadata schemaMetadata, SchemaKey schemaKey,
                                      TableSchemaService tableSchemaService,
                                      EntryEncoderContext context)
        throws MessageSerDeException {
        try {
            switch (schemaMetadata.getSchemaType()) {
                case "AVRO":
                    if (object instanceof GenericRecord genericRecord) {
                        return (GenericRecord) genericRecord;
                    } else {
                        return transformAvroPrimitive(schemaMetadata, object);
                    }
                case "JSON":
                    schemaKey.setConvertedType(SchemaKey.ConvertedType.JSON_TO_AVRO);
                    Schema jsonSchema = (Schema) SCHEMA_CACHE.computeIfAbsent(schemaKey,
                        () -> JsonSchema.of(schemaMetadata.getSchema()).toAvroSchema());
                    return transformJson((ObjectNode) object, jsonSchema);
                case "PRIMITIVE":
                    return transformPrimitive(object);
                default:
                    throw new IllegalArgumentException("Unsupported schema type: " + schemaMetadata.getSchemaType());
            }
        } catch (Throwable throwable) {
            throw new MessageSerDeException(ExceptionCode.MESSAGE_SERIALIZE_TO_LAKEHOUSE_ERROR, throwable);
        }
    }

    protected Schema extendWithMetadata(Schema schema) {
        return schema;
    }

    private GenericRecord transformJson(ObjectNode value, Schema schema) throws IOException {
        return JsonAvroConverter.INSTANCE.convertToGenericDataRecord(value, schema);
    }

    private GenericRecord transformPrimitive(Object object) {
        var nullableSchema = Schema.createUnion(Arrays.asList(
            Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES)));
        Schema recordSchema = SchemaBuilder.record(PRIMITIVE_RECORD_NAME)
            .fields()
            .name(PRIMITIVE_RECORD_FIELD_NAME).type(nullableSchema).withDefault(null)
            .endRecord();
        GenericRecord record = new GenericData.Record(recordSchema);
        record.put(PRIMITIVE_RECORD_FIELD_NAME, object);
        return record;
    }

    private GenericRecord transformAvroPrimitive(SchemaMetadata schemaMetadata, Object value) {
        Schema schema = new Schema.Parser().parse(schemaMetadata.getSchema());
        var nullableSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
        Schema recordSchema = SchemaBuilder.record(PRIMITIVE_RECORD_NAME)
            .fields()
            .name(PRIMITIVE_RECORD_FIELD_NAME).type(nullableSchema).withDefault(null)
            .endRecord();
        GenericRecord record = new GenericData.Record(recordSchema);
        record.put(PRIMITIVE_RECORD_FIELD_NAME, value);
        return record;
    }
}
