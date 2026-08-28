/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class EntryEncoderContextTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void builderUsesProtocolNeutralDefaults() {
        EntryEncoderContext context = EntryEncoderContext.builder().build();

        assertThat(context.properties()).isEmpty();
        assertThat(context.protobufMessageName()).isEmpty();
        assertThat(context.baseSchemaVersion()).isEmpty();
        assertThat(context.toMeta()).isEqualTo("{}");
    }

    @Test
    void keyCopiesSourceBuffer() {
        byte[] key = "customer-7".getBytes(StandardCharsets.UTF_8);
        var buffer = Unpooled.wrappedBuffer(key);
        EntryEncoderContext context = EntryEncoderContext.builder()
                .key(Optional.of(buffer))
                .build();

        key[0] = 'X';
        byte[] copied = new byte[context.keyByteBuffer().remaining()];
        context.keyByteBuffer().get(copied);
        assertThat(new String(copied, StandardCharsets.UTF_8)).isEqualTo("customer-7");
    }

    @Test
    void metadataJsonUsesNeutralKafkaFields() throws Exception {
        EntryEncoderContext context = EntryEncoderContext.builder()
                .messageOffset("42")
                .publishTime(100L)
                .eventTime(90L)
                .schemaVersion(3L)
                .properties(Map.of("trace-id", "abc"))
                .build();

        var metadata = JSON.readTree(context.toMeta());
        assertThat(metadata.get(InternalFieldNames.INTERNAL_MESSAGE_OFFSET).asText()).isEqualTo("42");
        assertThat(metadata.get(InternalFieldNames.INTERNAL_PUBLISH_TIME).asLong()).isEqualTo(100L);
        assertThat(metadata.get(InternalFieldNames.INTERNAL_EVENT_TIME).asLong()).isEqualTo(90L);
        assertThat(metadata.get(InternalFieldNames.INTERNAL_SCHEMA_VERSION).asLong()).isEqualTo(3L);
        assertThat(metadata.get(InternalFieldNames.INTERNAL_PROPERTIES).get("trace-id").asText())
                .isEqualTo("abc");
    }

    @Test
    void protobufMessageNameCanBeReplacedImmutably() {
        EntryEncoderContext original = EntryEncoderContext.builder().build();
        EntryEncoderContext updated = original.withProtobufMessageName(Optional.of("example.Event"));

        assertThat(original.protobufMessageName()).isEmpty();
        assertThat(updated.protobufMessageName()).contains("example.Event");
    }
}
