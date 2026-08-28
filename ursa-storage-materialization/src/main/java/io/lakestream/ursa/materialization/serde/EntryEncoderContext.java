/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import static io.lakestream.ursa.materialization.serde.EntryEncoder.MAPPER;
import static io.lakestream.ursa.materialization.serde.InternalFieldNames.INTERNAL_EVENT_TIME;
import static io.lakestream.ursa.materialization.serde.InternalFieldNames.INTERNAL_MESSAGE_OFFSET;
import static io.lakestream.ursa.materialization.serde.InternalFieldNames.INTERNAL_PROPERTIES;
import static io.lakestream.ursa.materialization.serde.InternalFieldNames.INTERNAL_PUBLISH_TIME;
import static io.lakestream.ursa.materialization.serde.InternalFieldNames.INTERNAL_SCHEMA_VERSION;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.ByteBuf;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.Variants;

@Slf4j
@Builder(toBuilder = true)
public record EntryEncoderContext(
    EntryFormat entryFormat,
    boolean isVariantEnabled,
    boolean isPersistExtraMetadata,
    boolean isPersistKey,
    boolean isUnityCatalog,
    byte[] keyBytes,
    Long publishTime,
    Long eventTime,
    Long schemaVersion,
    String messageOffset,
    Map<String, String> properties,
    MissingSchemaVersionTracker missingSchemaVersionTracker,
    Optional<String> protobufMessageName,
    Optional<Long> baseSchemaVersion) {

    public EntryEncoderContext {
        entryFormat = entryFormat == null ? EntryFormat.URSA : entryFormat;
        properties = properties == null ? Map.of() : Map.copyOf(properties);
        protobufMessageName = protobufMessageName == null ? Optional.empty() : protobufMessageName;
        baseSchemaVersion = baseSchemaVersion == null ? Optional.empty() : baseSchemaVersion;
    }

    public static class EntryEncoderContextBuilder {
        private byte[] keyBytes;
        private Optional<String> protobufMessageName = Optional.empty();
        private Optional<Long> baseSchemaVersion = Optional.empty();
        private Map<String, String> properties = Collections.emptyMap();
        public EntryEncoderContextBuilder key(Optional<ByteBuf> key) {
            if (key != null && key.isPresent()) {
                ByteBuf keyBuf = key.get();
                this.keyBytes = new byte[keyBuf.readableBytes()];
                keyBuf.getBytes(keyBuf.readerIndex(), this.keyBytes);
            } else {
                this.keyBytes = null;
            }
            return this;
        }
    }

    public EntryEncoderContext withProtobufMessageName(Optional<String> protobufMessageName) {
        return this.toBuilder().protobufMessageName(protobufMessageName).build();
    }

    public String toMeta() {
        try {
            ObjectNode json = MAPPER.createObjectNode();
            if (messageOffset != null) {
                json.put(INTERNAL_MESSAGE_OFFSET, messageOffset);
            }
            if (publishTime != null) {
                json.put(INTERNAL_PUBLISH_TIME, publishTime);
            }
            if (eventTime != null) {
                json.put(INTERNAL_EVENT_TIME, eventTime);
            }
            if (schemaVersion != null) {
                json.put(INTERNAL_SCHEMA_VERSION, schemaVersion);
            }
            if (properties != null && !properties.isEmpty()) {
                ObjectNode propertiesNode = MAPPER.createObjectNode();
                for (var property : properties.entrySet()) {
                    propertiesNode.put(property.getKey(), property.getValue());
                }
                json.set(INTERNAL_PROPERTIES, propertiesNode);
            }
            return MAPPER.writeValueAsString(json);
        } catch (Exception e) {
            log.warn("Failed to convert message metadata to variant metadata", e);
            return null;
        }
    }

    public byte[] keyBytesArray() {
        return keyBytes;
    }

    public java.nio.ByteBuffer keyByteBuffer() {
        return keyBytes != null ? java.nio.ByteBuffer.wrap(keyBytes) : null;
    }

    public Variant toMetaVariant() {
        String jsonString = toMeta();
        var variantMetadata = Variants.metadata(INTERNAL_EVENT_TIME, INTERNAL_PUBLISH_TIME,
            INTERNAL_SCHEMA_VERSION, INTERNAL_MESSAGE_OFFSET, INTERNAL_PROPERTIES);
        return Variant.of(variantMetadata, Variants.of(jsonString));
    }
}
