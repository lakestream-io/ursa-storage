/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import io.lakestream.api.EntryHeader;
import java.io.IOException;
import java.util.Base64;
import java.util.Objects;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
public class LakehouseEntryMetadata {

    public record LakehouseEntryOffset(long entryId, long batchId) {

    }

    @Setter
    @Getter
    private EntryHeader entryHeader;

    @Setter
    @Getter
    private Long schemaVersion;

    @Setter
    @Getter
    private int numberOfMessagesInBatch;

    @Setter
    @Getter
    private boolean isNeedToPersistent;

    @Getter
    private LakehouseEntryOffset lakehouseEntryOffset;

    public LakehouseEntryMetadata(EntryHeader entryHeader, Long schemaVersion) {
        this.entryHeader = entryHeader;
        this.schemaVersion = schemaVersion;
        this.numberOfMessagesInBatch = entryHeader == null ? 1 : entryHeader.numberOfMessages();
    }

    public String serializeTo() throws IOException {
        var metadataBuilder =
            io.lakestream.ursa.materialization.serde.proto.LakehouseEntryMetadata.newBuilder();
        if (schemaVersion != null) {
            metadataBuilder.setSchemaVersion(schemaVersion);
        }
        if (entryHeader != null) {
            metadataBuilder.setEntryHeader(io.lakestream.ursa.materialization.serde.proto.EntryHeader.newBuilder()
                .setOffset(entryHeader.offset())
                .setNumberOfMessages(entryHeader.numberOfMessages())
                .setWrittenTimestamp(entryHeader.writtenTimestamp())
                .setEntrySize(entryHeader.entrySize())
                .setCumulativeSize(entryHeader.cumulativeSize()));
        }
        return Base64.getEncoder().encodeToString(metadataBuilder.build().toByteArray());
    }

    public static LakehouseEntryMetadata of(String lakehouseEntryMetadataString) throws IOException {
        var lakehouseEntryMetadata = new LakehouseEntryMetadata();
        var bytes = Base64.getDecoder().decode(lakehouseEntryMetadataString);
        var lem = io.lakestream.ursa.materialization.serde.proto.LakehouseEntryMetadata.parseFrom(bytes);
        if (lem.hasEntryHeader()) {
            var ehp = lem.getEntryHeader();
            var eh = new EntryHeader(ehp.getOffset(), ehp.getNumberOfMessages(),
                ehp.getWrittenTimestamp(), ehp.getEntrySize(), ehp.getCumulativeSize()
            );
            lakehouseEntryMetadata.setEntryHeader(eh);
            lakehouseEntryMetadata.setNumberOfMessagesInBatch(eh.numberOfMessages());
        }

        if (lem.hasSchemaVersion()) {
            lakehouseEntryMetadata.setSchemaVersion(lem.getSchemaVersion());
        }

        return lakehouseEntryMetadata;
    }

    public void setLakehouseEntryOffset(long entryId, long batchId) {
        this.lakehouseEntryOffset = new LakehouseEntryOffset(entryId, batchId);
    }

    @Override
    public LakehouseEntryMetadata clone() throws CloneNotSupportedException {
        LakehouseEntryMetadata copy = new LakehouseEntryMetadata(this.entryHeader, this.schemaVersion);
        copy.numberOfMessagesInBatch = this.numberOfMessagesInBatch;
        copy.isNeedToPersistent = this.isNeedToPersistent;
        copy.lakehouseEntryOffset = this.lakehouseEntryOffset;
        return copy;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LakehouseEntryMetadata that = (LakehouseEntryMetadata) o;
        return Objects.equals(schemaVersion, that.schemaVersion) && Objects.equals(entryHeader, that.entryHeader);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaVersion, entryHeader);
    }
}
