/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction;

/**
 * Signals that a pre-upgrade published-offset cursor still has no cumulative byte size when a
 * publication operation requires one.
 *
 * <p>The affected publication must remain quarantined until an operator records a verified,
 * positive cumulative size through the supported publish-offset administration command.
 */
public class LegacyPublishedOffsetException extends PublicationRecoveryException {

    private final String publicationName;
    private final long streamId;
    private final long offset;

    public LegacyPublishedOffsetException(
            String publicationName, long streamId, long offset, String reason) {
        this(publicationName, streamId, offset, reason, null);
    }

    public LegacyPublishedOffsetException(
            String publicationName, long streamId, long offset, String reason, Throwable cause) {
        super("Legacy published-offset cursor for " + publicationName + " (streamId=" + streamId
                + ", offset=" + offset + ") has no cumulative byte size: " + reason
                + ". Quarantine publication until update-publish-task-offset records a verified "
                + "positive cumulative size.", cause);
        this.publicationName = publicationName;
        this.streamId = streamId;
        this.offset = offset;
    }

    public String publicationName() {
        return publicationName;
    }

    public long streamId() {
        return streamId;
    }

    public long offset() {
        return offset;
    }
}
