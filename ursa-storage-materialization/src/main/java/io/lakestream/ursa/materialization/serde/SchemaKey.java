/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import java.util.Objects;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.With;

@Builder
@Setter
@Getter
@EqualsAndHashCode
public class SchemaKey {

    public enum ConvertedType {
        JSON_TO_AVRO,
        JSON_TO_ICEBERG_RECORD,
        JSON_TO_DELTA_RECORD,
        PROTOBUF_TO_ICEBERG_RECORD,
        PROTOBUF_TO_DELTA_RECORD,
        BYTES_PROTOBUF,
        PROTOBUF_AVRO,
        AVRO_TO_ICEBERG_PRIMITIVE,
        AVRO_TO_ICEBERG_RECORD,
        AVRO_TO_DELTA_RECORD,
        AVRO_TO_DELTA_PRIMITIVE,
        PROTOBUF_NATIVE_TO_ICEBERG,
        PROTOBUF_NATIVE_TO_DELTA,
        KAFKA_PRIMITIVE_TO_ICEBERG,
        KAFKA_PRIMITIVE_TO_DELTA,
    }

    public enum MessageType {
        KAFKA,
    }

    private String topicName;
    @With
    private long schemaVersion;
    @With
    private ConvertedType convertedType;
    @With
    private MessageType messageType;

    public SchemaKey withTopicName(String topicName) {
        if (Objects.equals(this.topicName, topicName)) {
            return this;
        }
        return new SchemaKey(topicName, schemaVersion, convertedType, messageType);
    }

}
