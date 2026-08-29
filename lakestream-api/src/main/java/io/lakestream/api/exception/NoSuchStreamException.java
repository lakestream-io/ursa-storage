/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.exception;

import io.lakestream.api.StreamIdentifier;

/**
 * Thrown when a referenced stream does not exist in the catalog.
 */
public class NoSuchStreamException extends RuntimeException {

    /** Constructor for more specific missing-stream exception types. */
    protected NoSuchStreamException(String message) {
        super(message);
    }

    /** Constructor for more specific missing-stream exception types with a cause. */
    protected NoSuchStreamException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoSuchStreamException(StreamIdentifier identifier) {
        super("Stream not found: " + identifier.fullName());
    }

    public NoSuchStreamException(StreamIdentifier identifier, Throwable cause) {
        super("Stream not found: " + identifier.fullName(), cause);
    }
}
