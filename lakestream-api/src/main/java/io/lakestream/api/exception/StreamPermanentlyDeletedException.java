/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.exception;

import io.lakestream.api.StreamIdentifier;

/**
 * Signals that a stream identity has a durable permanent-deletion fence.
 *
 * <p>The stream is absent, and creating or externally registering the same identity cannot make it
 * active again. A replacement resource must use a different {@link StreamIdentifier}.
 */
public class StreamPermanentlyDeletedException extends NoSuchStreamException {

    public StreamPermanentlyDeletedException(StreamIdentifier identifier) {
        super("Stream identity was permanently deleted: " + identifier.fullName());
    }

    public StreamPermanentlyDeletedException(StreamIdentifier identifier, Throwable cause) {
        super("Stream identity was permanently deleted: " + identifier.fullName(), cause);
    }
}
