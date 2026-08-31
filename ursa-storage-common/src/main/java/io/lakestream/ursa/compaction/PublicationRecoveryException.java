/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction;

/**
 * Signals that durable publication metadata cannot be decoded or reconciled safely.
 *
 * <p>The publisher must release its lease and quarantine this partition instead of retrying the
 * same inconsistent cursor or prepared task on every scan. Recovery requires either a later
 * automatic repair or an operator to correct the durable state from verified source data.
 */
public class PublicationRecoveryException extends IllegalStateException {

    public PublicationRecoveryException(String message) {
        super(message);
    }

    public PublicationRecoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
