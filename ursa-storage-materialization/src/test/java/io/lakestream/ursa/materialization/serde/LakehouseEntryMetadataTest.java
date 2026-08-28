/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.CodedOutputStream;
import io.lakestream.api.EntryHeader;
import java.io.ByteArrayOutputStream;
import java.util.Base64;
import org.junit.jupiter.api.Test;

class LakehouseEntryMetadataTest {

    @Test
    void roundTripsKafkaMetadata() throws Exception {
        EntryHeader header = new EntryHeader(17L, 1, 23L, 31, 47L);
        LakehouseEntryMetadata expected = new LakehouseEntryMetadata(header, 5L);

        LakehouseEntryMetadata actual = LakehouseEntryMetadata.of(expected.serializeTo());

        assertThat(actual.getEntryHeader()).isEqualTo(header);
        assertThat(actual.getSchemaVersion()).isEqualTo(5L);
    }

    @Test
    void readsEntryHeaderFromLegacyWirePayloadAndIgnoresRemovedFields() throws Exception {
        var header = io.lakestream.ursa.materialization.serde.proto.EntryHeader.newBuilder()
                .setOffset(11L)
                .setNumberOfMessages(2)
                .setWrittenTimestamp(13L)
                .setEntrySize(17)
                .setCumulativeSize(19L)
                .build();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CodedOutputStream output = CodedOutputStream.newInstance(bytes);
        output.writeByteArray(1, new byte[] {10, 0});
        output.writeByteArray(2, header.toByteArray());
        output.writeByteArray(3, new byte[] {1, 2, 3});
        output.flush();

        LakehouseEntryMetadata actual = LakehouseEntryMetadata.of(
                Base64.getEncoder().encodeToString(bytes.toByteArray()));

        assertThat(actual.getEntryHeader()).isEqualTo(new EntryHeader(11L, 2, 13L, 17, 19L));
        assertThat(actual.getSchemaVersion()).isNull();
    }
}
