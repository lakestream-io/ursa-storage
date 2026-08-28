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

    public NoSuchStreamException(StreamIdentifier identifier) {
        super("Stream not found: " + identifier.fullName());
    }

    public NoSuchStreamException(StreamIdentifier identifier, Throwable cause) {
        super("Stream not found: " + identifier.fullName(), cause);
    }
}
