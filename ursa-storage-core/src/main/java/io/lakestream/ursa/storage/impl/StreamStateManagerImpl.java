/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.api.LogState;
import io.lakestream.api.LogStateManager;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StreamStateManagerImpl implements LogStateManager {

    private final Set<Long> fencedStreamIds = ConcurrentHashMap.newKeySet();

    @Override
    public void setState(long streamId, LogState state) {
        if (state == LogState.FENCED) {
            fencedStreamIds.add(streamId);
        } else {
            fencedStreamIds.remove(streamId);
        }
    }

    @Override
    public LogState getState(long streamId) {
        return fencedStreamIds.contains(streamId) ? LogState.FENCED : LogState.NORMAL;
    }
}
