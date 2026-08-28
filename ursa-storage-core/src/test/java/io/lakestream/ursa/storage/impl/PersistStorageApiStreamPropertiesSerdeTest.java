/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.lakestream.ursa.storage.StreamProperties;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class PersistStorageApiStreamPropertiesSerdeTest {

    @Test
    void ignoresUnknownFieldsFromNewerWriters() throws Exception {
        byte[] futureStreamProperties = ("{\"key\":\"topic-with-future-properties\","
                + "\"futureField\":{\"enabled\":true}}")
                .getBytes(StandardCharsets.UTF_8);

        assertEquals(
                new StreamProperties("topic-with-future-properties"),
                PersistStorageApi.deserializeStreamProperties(futureStreamProperties));
    }
}
