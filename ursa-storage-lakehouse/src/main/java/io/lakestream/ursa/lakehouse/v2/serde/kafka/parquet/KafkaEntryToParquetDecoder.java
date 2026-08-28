/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.serde.kafka.parquet;

import static io.lakestream.ursa.lakehouse.v2.serde.kafka.parquet.KafkaEntryToParquetEncoder.PRIMITIVE_RECORD_FIELD_NAME;
import static io.lakestream.ursa.lakehouse.v2.serde.kafka.parquet.KafkaEntryToParquetEncoder.PRIMITIVE_RECORD_NAME;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.exception.RuntimeExceptionWithCode;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import io.lakestream.ursa.materialization.serde.EntryDecoder;
import io.lakestream.ursa.materialization.serde.SchemaKey;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaEntryDecoder;
import io.lakestream.ursa.materialization.serde.utils.avro.JsonToAvro;
import io.lakestream.ursa.materialization.serde.utils.json.AvroJsonConverter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class KafkaEntryToParquetDecoder extends KafkaEntryDecoder<GenericRecord>
    implements EntryDecoder<GenericRecord> {

    public KafkaEntryToParquetDecoder(SchemaService schemaService) {
        super(schemaService);
    }

    @Override
    protected ByteBuffer transform(GenericRecord genericRecord, SchemaMetadata schemaMetadata, SchemaKey schemaKey)
        throws ExceptionWithCode {

        try {
            switch (schemaMetadata.getSchemaType()) {
                case "AVRO":
                    if (genericRecord.getSchema().getName().equals(PRIMITIVE_RECORD_NAME)) {
                        return transformAvroNonRecord(genericRecord, schemaKey.getTopicName());
                    } else {
                        return transformAvro(genericRecord, schemaKey.getTopicName());
                    }
                case "JSON":
                    schemaKey.setConvertedType(SchemaKey.ConvertedType.JSON_TO_AVRO);
                    Schema jsonSchema = (Schema) SCHEMA_CACHE.computeIfAbsent(schemaKey,
                        () -> JsonToAvro.convert(schemaMetadata.getSchema()));
                    return transformJson(genericRecord, jsonSchema, schemaKey.getSchemaVersion());
                case "PRIMITIVE":
                    return transformPrimitive(genericRecord);
                default:
                    return null;
            }
        } catch (RuntimeExceptionWithCode runtimeExceptionWithCode) {
            throw runtimeExceptionWithCode;
        } catch (Throwable throwable) {
            throw new ExceptionWithCode(ExceptionCode.MESSAGE_DESERIALIZE_FROM_LAKEHOUSE_ERROR,
                "failed to get message from the generic record", throwable);
        }
    }

    private ByteBuffer transformAvro(GenericRecord genericRecord, String topic) {
        var serializer = schemaService.getSerializer("AVRO");
        var value = serializer.serialize(TopicName.get(topic).getPartitionedTopicName(), genericRecord);
        return ByteBuffer.wrap(value);
    }

    private ByteBuffer transformAvroNonRecord(GenericRecord genericRecord, String topic) {
        var serializer = schemaService.getSerializer("AVRO");
        var value = serializer.serialize(TopicName.get(topic).getPartitionedTopicName(),
            genericRecord.get(PRIMITIVE_RECORD_FIELD_NAME));
        return ByteBuffer.wrap(value);
    }

    private ByteBuffer transformJson(GenericRecord genericRecord, Schema schema, long schemaId) throws IOException {
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeByte(0);
        byteBuf.writeInt(Math.toIntExact(schemaId));
        AvroJsonConverter.INSTANCE.convertToJsonBytes(genericRecord, byteBuf);
        return byteBuf.nioBuffer();
    }

    private ByteBuffer transformPrimitive(GenericRecord genericRecord) {
        var payload = genericRecord.get(PRIMITIVE_RECORD_FIELD_NAME);
        if (payload == null) {
            return null;
        }
        if (payload instanceof ByteBuffer) {
            return (ByteBuffer) payload;
        } else if (payload instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) payload);
        }
        throw new IllegalArgumentException("Field is not binary type");
    }
}
