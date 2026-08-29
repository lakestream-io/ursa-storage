/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.catalog.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LogMetadataSerdeTest {

    private static final LogMetadataSerde SERDE = LogMetadataSerde.INSTANCE;
    private static final String METADATA_PATH = "/streams/org/analytics/stream";

    @Test
    void preservesPersistedJsonFormat() throws Exception {
        assertMetadata(new LogMetadata(1L, null, OptionalLong.of(2L)),
                "{\"streamId\":1,\"properties\":{},\"terminatedOffset\":2,\"deleted\":false}");
        assertMetadata(new LogMetadata(2L, null, OptionalLong.empty()),
                "{\"streamId\":2,\"properties\":{},\"terminatedOffset\":null,\"deleted\":false}");
        assertMetadata(new LogMetadata(3L, Map.of("key", "value"), OptionalLong.of(4L)),
                "{\"streamId\":3,\"properties\":{\"key\":\"value\"},\"terminatedOffset\":4,\"deleted\":false}");
    }

    @Test
    void roundTripsRegistrationAndDeletionFields() throws Exception {
        assertMetadata(new LogMetadata(4L, Map.of("key", "value"), OptionalLong.of(5L),
                        "incarnation-1", "owner-1", 7L, true),
                "{\"streamId\":4,\"properties\":{\"key\":\"value\"},\"terminatedOffset\":5,"
                        + "\"registrationIncarnationId\":\"incarnation-1\","
                        + "\"registrationOwnerToken\":\"owner-1\","
                        + "\"registrationOwnerGeneration\":7,\"deleted\":true}");
    }

    @Test
    void rejectsRegistrationIdentityWithoutGenerationThroughCompatibilityConstructors() {
        assertThrows(IllegalArgumentException.class, () -> new LogMetadata(
            4L, Map.of(), OptionalLong.empty(), "incarnation-1", "owner-1"));
        assertThrows(IllegalArgumentException.class, () -> new LogMetadata(
            4L, Map.of(), OptionalLong.empty(), "incarnation-1", "owner-1", true));
    }

    @Test
    void roundTripsRetiredAndPurgeableStreamIdsInDeterministicOrder() throws Exception {
        assertMetadata(new LogMetadata(4L, Map.of(), OptionalLong.empty(),
                        "incarnation-1", "owner-1", 7L, true, Set.of(9L, 7L),
                        Set.of(9L), Set.of(
                            new LogMetadata.RetiredStreamMapping(9L, "native/old"),
                            new LogMetadata.RetiredStreamMapping(7L, "external/stable")),
                        Set.of("native/old", "external/stable", "native/pending")),
                "{\"streamId\":4,\"properties\":{},\"terminatedOffset\":null,"
                        + "\"registrationIncarnationId\":\"incarnation-1\","
                        + "\"registrationOwnerToken\":\"owner-1\","
                        + "\"registrationOwnerGeneration\":7,"
                        + "\"retiredStreamIds\":[7,9],"
                        + "\"purgeableRetiredStreamIds\":[9],"
                        + "\"retiredStreamMappings\":["
                        + "{\"streamId\":7,\"mappingKey\":\"external/stable\"},"
                        + "{\"streamId\":9,\"mappingKey\":\"native/old\"}],"
                        + "\"retiredMappingKeys\":[\"external/stable\",\"native/old\","
                        + "\"native/pending\"],"
                        + "\"deleted\":true}");
    }

    @Test
    void persistsRetiredMappingPurgeIntentWhenTrue() throws Exception {
        assertMetadata(new LogMetadata(4L, Map.of(), OptionalLong.empty(),
                        "incarnation-1", "owner-1", 7L, true, Set.of(), Set.of(),
                        Set.of(new LogMetadata.RetiredStreamMapping(
                            -1L, "native/pending", true)),
                        Set.of("native/pending")),
                "{\"streamId\":4,\"properties\":{},\"terminatedOffset\":null,"
                        + "\"registrationIncarnationId\":\"incarnation-1\","
                        + "\"registrationOwnerToken\":\"owner-1\","
                        + "\"registrationOwnerGeneration\":7,"
                        + "\"retiredStreamMappings\":["
                        + "{\"streamId\":-1,\"mappingKey\":\"native/pending\","
                        + "\"purge\":true}],"
                        + "\"retiredMappingKeys\":[\"native/pending\"],"
                        + "\"deleted\":true}");
    }

    @Test
    void readsLegacyJsonWithoutRegistrationAndDeletionFields() throws Exception {
        byte[] content = "{\"streamId\":3,\"properties\":{},\"terminatedOffset\":4}"
                .getBytes(StandardCharsets.UTF_8);

        LogMetadata metadata = SERDE.deserialize(METADATA_PATH, content);

        assertNull(metadata.registrationIncarnationId());
        assertNull(metadata.registrationOwnerToken());
        assertNull(metadata.registrationOwnerGeneration());
        assertEquals(Set.of(), metadata.retiredStreamIds());
        assertFalse(metadata.deleted());
    }

    @Test
    void preservesLegacyEmptyMetadataSentinel() throws Exception {
        assertEquals(0, SERDE.serialize(METADATA_PATH, LogMetadata.EMPTY).length);
        assertSame(LogMetadata.EMPTY,
                SERDE.deserialize(METADATA_PATH, new byte[0]));
    }

    @Test
    void ignoresUnknownFieldsForForwardCompatibility() throws Exception {
        byte[] content = ("{\"streamId\":3,\"properties\":{},\"terminatedOffset\":4,"
                + "\"futureField\":\"ignored\"}").getBytes(StandardCharsets.UTF_8);

        assertEquals(new LogMetadata(3L, Map.of(), OptionalLong.of(4L)),
                SERDE.deserialize(METADATA_PATH, content));
    }

    @Test
    void rejectsPartialOrInvalidRegistrationIdentity() {
        assertThrows(IllegalArgumentException.class, () -> new LogMetadata(
            1L, Map.of(), OptionalLong.empty(), "incarnation", null, null, false));
        assertThrows(IllegalArgumentException.class, () -> new LogMetadata(
            1L, Map.of(), OptionalLong.empty(), null, "owner", null, false));
        assertThrows(IllegalArgumentException.class, () -> new LogMetadata(
            1L, Map.of(), OptionalLong.empty(), null, null, 1L, false));
        assertThrows(IllegalArgumentException.class, () -> new LogMetadata(
            1L, Map.of(), OptionalLong.empty(), "incarnation", "owner", -1L, false));
        assertThrows(IllegalArgumentException.class, () -> new LogMetadata(
            1L, Map.of(), OptionalLong.empty(), "incarnation", "owner", 1L, true,
            Set.of(-1L)));
    }

    @Test
    void rejectsPersistedPartialRegistrationIdentity() {
        byte[] missingOwner = ("{\"streamId\":3,\"properties\":{},\"terminatedOffset\":null,"
                + "\"registrationIncarnationId\":\"incarnation\",\"deleted\":false}")
            .getBytes(StandardCharsets.UTF_8);
        byte[] missingGeneration = ("{\"streamId\":3,\"properties\":{},"
                + "\"terminatedOffset\":null,"
                + "\"registrationIncarnationId\":\"incarnation\","
                + "\"registrationOwnerToken\":\"owner\",\"deleted\":false}")
            .getBytes(StandardCharsets.UTF_8);

        assertThrows(IllegalArgumentException.class,
            () -> SERDE.deserialize(METADATA_PATH, missingOwner));
        assertThrows(IllegalArgumentException.class,
            () -> SERDE.deserialize(METADATA_PATH, missingGeneration));
    }

    private static void assertMetadata(LogMetadata metadata, String expectedJson) throws Exception {
        byte[] bytes = SERDE.serialize(METADATA_PATH, metadata);
        assertEquals(expectedJson, new String(bytes, StandardCharsets.UTF_8));
        assertEquals(metadata, SERDE.deserialize(METADATA_PATH, bytes));
    }
}
