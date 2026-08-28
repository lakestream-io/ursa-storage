/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.kafka.reader;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import io.lakestream.api.EntryHeader;
import java.io.IOException;
import java.util.Base64;

final class EntryMetadataParser {

    private EntryMetadataParser() {
    }

    static EntryHeader parse(String encodedMetadata) throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(Base64.getDecoder().decode(encodedMetadata));
        while (!input.isAtEnd()) {
            int tag = input.readTag();
            if (tag == 18) {
                ByteString entryHeader = input.readBytes();
                return parseEntryHeader(entryHeader.newCodedInput());
            }
            if (!input.skipField(tag)) {
                break;
            }
        }
        throw new IOException("Lakehouse entry metadata does not contain an entry header");
    }

    private static EntryHeader parseEntryHeader(CodedInputStream input) throws IOException {
        long offset = 0;
        int numberOfMessages = 0;
        long writtenTimestamp = 0;
        int entrySize = 0;
        long cumulativeSize = 0;
        while (!input.isAtEnd()) {
            int tag = input.readTag();
            switch (tag) {
                case 8 -> offset = input.readInt64();
                case 16 -> numberOfMessages = input.readInt32();
                case 24 -> writtenTimestamp = input.readInt64();
                case 32 -> entrySize = input.readInt32();
                case 40 -> cumulativeSize = input.readInt64();
                default -> {
                    if (!input.skipField(tag)) {
                        return new EntryHeader(offset, numberOfMessages, writtenTimestamp, entrySize, cumulativeSize);
                    }
                }
            }
        }
        return new EntryHeader(offset, numberOfMessages, writtenTimestamp, entrySize, cumulativeSize);
    }
}
