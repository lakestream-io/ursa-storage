/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.exception;

import io.lakestream.api.StreamIdentifier;

/**
 * Signals that a native partition allocation lifecycle is fenced by retained metadata.
 *
 * <p>The catalog retains retired physical stream IDs and keyed-mapping fences so delayed cleanup
 * cannot delete an active allocation. This exception indicates that the retained allocation state
 * cannot be reconciled safely with the partition's current metadata and ownership generation.
 */
public class PartitionLifecycleFencedException extends LogFencedException {

    public PartitionLifecycleFencedException(
            StreamIdentifier identifier, int partitionIndex, String reason) {
        super(message(identifier, partitionIndex, reason));
    }

    public PartitionLifecycleFencedException(
            StreamIdentifier identifier, int partitionIndex, String reason, Throwable cause) {
        super(message(identifier, partitionIndex, reason), cause);
    }

    private static String message(
            StreamIdentifier identifier, int partitionIndex, String reason) {
        return "Partition allocation lifecycle is fenced for " + identifier.fullName()
            + "-partition-" + partitionIndex + ": " + reason;
    }
}
