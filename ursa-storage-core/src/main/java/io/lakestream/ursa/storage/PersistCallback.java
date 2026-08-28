/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

/**
 * Callback interface for asynchronous persistence operations in the Ursa storage system.
 * This interface is used to handle the results of storage operations, particularly
 * in the context of Write-Ahead Logging (WAL) and other persistence mechanisms.
 *
 * Implementation considerations:
 * 1. Thread safety: Implementations should be thread-safe, as they may be called from different threads in
 *    asynchronous operations.
 * 2. Performance: The callback methods should be lightweight and avoid blocking operations, as they may be
 *    called frequently in high-throughput scenarios.
 * 3. Error handling: The `onFailure` method should handle errors gracefully, potentially logging them or
 *    triggering retry mechanisms depending on the specific use case.
 * 4. Consistency: Ensure that either `onSuccess` or `onFailure` is called exactly once for each persistence
 *    operation, but never both.
 * 5. Resource management: If the implementation holds any resources, it should ensure proper cleanup in both
 *    success and failure scenarios.
 * 6. Integration with metrics: Consider integrating with the metrics system (e.g., using `InstrumentProvider`)
 *    to track success and failure rates of persistence operations.
 * 7. Contextual information: Implementations might need to maintain additional context (e.g., stream ID, entry ID)
 *    to properly handle the callback, especially in systems dealing with multiple streams or entries.
 */
public interface PersistCallback {

    /**
     * Called when the persistence operation is successful.
     * @param addResult
     */
    void onSuccess(AddResult addResult);

    /**
     * Called when the persistence operation fails.
     *
     * @param t The Throwable that caused the failure. This allows for detailed error handling
     *          and logging in the implementation.
     */
    void onFailure(Throwable t);

}
