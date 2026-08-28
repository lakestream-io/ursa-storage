/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

/**
 * Manages the state of logs, which are identified with non-negative 64-bit integers called "stream id".
 * All methods are guaranteed to be thread safe. But to achieve strong consistency, you have to add extra
 * synchronization on it. For example,
 * {@snippet :
 * if (manager.getState(id) == FENCED) {
 *     doSomething();
 * }
 * }
 * The code above cannot guarantee the state is always {@code FENCED} during {@code doSomething()}.
 */
public interface LogStateManager {

    void setState(long streamId, LogState state);

    LogState getState(long streamId);
}
