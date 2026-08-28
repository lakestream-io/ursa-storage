/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

/**
 * Options that control source-reader behavior.
 *
 * <p>{@code firstAttemptTimeMs} and {@code maxWaitForTxnResolutionSeconds} together implement a
 * bounded wait: when the broker reports no more messages, the reader keeps throwing
 * {@code NO_MORE_MESSAGES} (forcing the task to retry) until
 * {@code now - firstAttemptTimeMs >= maxWaitForTxnResolutionSeconds * 1000}, at which point any
 * in-flight source transactions in the compaction range will have been committed (and re-delivered)
 * or aborted (and filtered), so it is safe to treat no-more-messages as end-of-task.
 *
 * <p>A negative {@code maxWaitForTxnResolutionSeconds} (e.g. {@code -1}) disables the bound and the
 * reader waits forever — every no-more-messages observation forces another retry, never EOF.
 */
public record EntryReaderOptions(
        boolean skipMarkerMessages,
        long readTimeoutSeconds,
        long firstAttemptTimeMs,
        long maxWaitForTxnResolutionSeconds) {

    public static final long DEFAULT_READ_TIMEOUT_SECONDS = 180;

    public static final long DEFAULT_MAX_WAIT_FOR_TXN_RESOLUTION_SECONDS = -1;

    public static final EntryReaderOptions DEFAULT = new EntryReaderOptions(
        false, DEFAULT_READ_TIMEOUT_SECONDS, 0L, DEFAULT_MAX_WAIT_FOR_TXN_RESOLUTION_SECONDS);

    /**
     * Convenience constructor for callers that don't track task age (notably unit tests).
     * Treats {@code firstAttemptTimeMs} as far in the past, so the bounded wait fires immediately.
     */
    public EntryReaderOptions(boolean skipMarkerMessages) {
        this(skipMarkerMessages, DEFAULT_READ_TIMEOUT_SECONDS, 0L, DEFAULT_MAX_WAIT_FOR_TXN_RESOLUTION_SECONDS);
    }
}
