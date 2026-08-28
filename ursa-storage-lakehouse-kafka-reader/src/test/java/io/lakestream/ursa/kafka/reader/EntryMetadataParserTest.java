/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.kafka.reader;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import io.lakestream.api.EntryHeader;
import java.io.ByteArrayOutputStream;
import java.util.Base64;
import org.junit.jupiter.api.Test;

class EntryMetadataParserTest {

    @Test
    void parsesWireCompatibleLakehouseEntryHeader() throws Exception {
        EntryHeader expected = new EntryHeader(42, 3, 1234, 99, 1000);

        assertThat(EntryMetadataParser.parse(encode(expected))).isEqualTo(expected);
    }

    static String encode(EntryHeader header) throws Exception {
        ByteArrayOutputStream headerBytes = new ByteArrayOutputStream();
        CodedOutputStream headerOutput = CodedOutputStream.newInstance(headerBytes);
        headerOutput.writeInt64(1, header.offset());
        headerOutput.writeInt32(2, header.numberOfMessages());
        headerOutput.writeInt64(3, header.writtenTimestamp());
        headerOutput.writeInt32(4, header.entrySize());
        headerOutput.writeInt64(5, header.cumulativeSize());
        headerOutput.flush();

        ByteArrayOutputStream metadataBytes = new ByteArrayOutputStream();
        CodedOutputStream metadataOutput = CodedOutputStream.newInstance(metadataBytes);
        metadataOutput.writeBytes(2, ByteString.copyFrom(headerBytes.toByteArray()));
        metadataOutput.flush();
        return Base64.getEncoder().encodeToString(metadataBytes.toByteArray());
    }
}
