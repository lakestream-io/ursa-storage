/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction;

/** Raised when a publisher no longer owns its publication lease or cursor revision. */
public class PublicationFencedException extends IllegalStateException {

    public PublicationFencedException(String message) {
        super(message);
    }

    public PublicationFencedException(String message, Throwable cause) {
        super(message, cause);
    }
}
