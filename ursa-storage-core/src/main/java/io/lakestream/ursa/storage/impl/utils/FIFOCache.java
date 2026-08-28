/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.utils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The FIFO cache used for write cache.
 */
public class FIFOCache<K, V> extends LinkedHashMap<K, V> {

    private final int capacity;
    private final Consumer<Map.Entry<K, V>> removalListener;

    public FIFOCache(int capacity, Consumer<Map.Entry<K, V>> removalListener) {
        super(capacity, 0.75f, false);
        this.capacity = capacity;
        this.removalListener = removalListener;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        boolean remove = size() > capacity;
        if (remove) {
            this.removalListener.accept(eldest);
        }
        return remove;
    }
}
