/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

import io.lakestream.ursa.storage.BaseStreamIDGenerator;
import io.lakestream.ursa.storage.IDGenerator;
import io.lakestream.ursa.storage.impl.MemIDGenerator;
import io.lakestream.ursa.storage.impl.OxiaIDGenerator;
import io.lakestream.ursa.storage.impl.RandomIDGenerator;
import io.lakestream.ursa.storage.impl.exception.IDGeneratorException;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CompactFileIDGenerator implements BaseStreamIDGenerator {

    public static final String OXIA_STREAM_COMPACT_KEY = "stream-%020d-compact";

    private Map<Long, IDGenerator> idGeneratorMap = new ConcurrentHashMap<>();

    private final String innerType;

    private final AsyncOxiaClient client;

    public CompactFileIDGenerator(String innerType) {
        this.innerType = innerType;
        this.client = null;
    }

    public CompactFileIDGenerator(String innerType, AsyncOxiaClient client) {
        this.innerType = innerType;
        this.client = client;
    }

    @Override
    public String generate(long streamID) throws IDGeneratorException {
        IDGenerator idGenerator = idGeneratorMap.computeIfAbsent(streamID,
                id -> {
                    if (MemIDGenerator.NAME.equals(innerType)) {
                        return new MemIDGenerator();
                    } else if (RandomIDGenerator.NAME.equals(innerType)) {
                        return new RandomIDGenerator();
                    } else if (OxiaIDGenerator.NAME.equals(innerType)) {
                        return new OxiaIDGenerator(String.format(OXIA_STREAM_COMPACT_KEY, id), client);
                    }
                    return null;
                });
        return idGenerator.generate();
    }

    public static CompactFileIDGenerator buildMemoryGenerator() {
        return new CompactFileIDGenerator(MemIDGenerator.NAME);
    }

    public static CompactFileIDGenerator buildRandomGenerator() {
        return new CompactFileIDGenerator(RandomIDGenerator.NAME);
    }

    public static CompactFileIDGenerator buildOxiaGenerator(AsyncOxiaClient client) {
        return new CompactFileIDGenerator(OxiaIDGenerator.NAME, client);
    }

}
