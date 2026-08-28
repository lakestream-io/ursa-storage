/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.ursa.storage.IDGenerator;
import io.lakestream.ursa.storage.impl.exception.IDGeneratorException;
import io.oxia.client.api.AsyncOxiaClient;

public class WALIdGenerator implements IDGenerator {

    public static final String WAL_KEY = "wal";

    private final IDGenerator idGenerator;

    private WALIdGenerator(IDGenerator idGenerator) {
        this.idGenerator = idGenerator;
    }

    public String generate() throws IDGeneratorException {
        return idGenerator.generate();
    }

    public static WALIdGenerator buildMemoryGenerator() {
        return new WALIdGenerator(new MemIDGenerator());
    }

    public static WALIdGenerator buildRandomGenerator() {
        return new WALIdGenerator(new RandomIDGenerator());
    }

    public static WALIdGenerator buildOxiaIdGenerator(AsyncOxiaClient client) {
        return new WALIdGenerator(new OxiaIDGenerator(WAL_KEY, client));
    }
}

