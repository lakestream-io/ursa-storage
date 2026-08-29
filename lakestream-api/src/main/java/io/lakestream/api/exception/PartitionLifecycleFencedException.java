/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.exception;

import io.lakestream.api.StreamIdentifier;

/**
 * Signals that a partition operation belongs to a lifecycle fenced by retained metadata.
 *
 * <p>For an externally managed stream, a partition deleted in the current registration generation
 * cannot be registered again until the logical stream is unregistered and registered again. The
 * new stream registration advances the ownership generation without weakening the deletion fence
 * against delayed writers from the old generation.
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
        return "Partition lifecycle is fenced for " + identifier.fullName()
            + "-partition-" + partitionIndex + ": " + reason;
    }
}
