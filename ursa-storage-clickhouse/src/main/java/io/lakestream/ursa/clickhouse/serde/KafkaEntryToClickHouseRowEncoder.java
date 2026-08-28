/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.MessageSerDeException;
import io.lakestream.ursa.materialization.serde.EntryEncoder;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.SchemaKey;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaEntryEncoder;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

/**
 * Decodes a Kafka source message into a ClickHouse row ({@code Map<column, value>}) using the
 * topic's registered schema. The {@link KafkaEntryEncoder} base handles batch-splitting and the
 * Confluent-registry schema lookup / deserialize; this subclass only flattens the decoded value
 * into a row. Destination column types are owned by {@code ClickHouseTableSchemaService}, so this
 * transform does not consult the {@link TableSchemaService}.
 */
@Slf4j
public class KafkaEntryToClickHouseRowEncoder extends KafkaEntryEncoder<Map<String, Object>>
        implements EntryEncoder<Map<String, Object>> {

    public KafkaEntryToClickHouseRowEncoder(SchemaService schemaService) {
        super(schemaService);
    }

    @Override
    protected Map<String, Object> transform(Object object, SchemaMetadata schemaMetadata, SchemaKey schemaKey,
                                            TableSchemaService tableSchemaService, EntryEncoderContext context)
            throws MessageSerDeException {
        if (object == null) {
            throw new MessageSerDeException(ExceptionCode.MESSAGE_NULL_VALUE,
                    "null value is not supported to write to the ClickHouse table");
        }
        try {
            return switch (schemaMetadata.getSchemaType()) {
                // An AVRO record flattens to a multi-column row; an AVRO-wrapped primitive (no record)
                // becomes a single value column.
                case "AVRO" -> object instanceof GenericRecord avroRecord
                        ? ClickHouseRowConverter.fromAvro(avroRecord)
                        : ClickHouseRowConverter.fromPrimitive(object);
                case "JSON" -> ClickHouseRowConverter.fromJson((JsonNode) object);
                case "PROTOBUF" -> ClickHouseRowConverter.fromProtobuf((Message) object);
                // PRIMITIVE (raw bytes) and anything else: a single value column.
                default -> ClickHouseRowConverter.fromPrimitive(object);
            };
        } catch (Throwable t) {
            throw new MessageSerDeException(ExceptionCode.MESSAGE_SERIALIZE_TO_LAKEHOUSE_ERROR, t);
        }
    }
}
