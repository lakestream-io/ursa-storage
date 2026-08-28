/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.ursa.storage.IDGenerator;
import io.lakestream.ursa.storage.impl.exception.IDGeneratorException;
import io.oxia.client.api.AsyncOxiaClient;

public class StreamIdGenerator implements IDGenerator {

    public static final String STREAM_KEY = "stream";

    private final IDGenerator idGenerator;

    private StreamIdGenerator(IDGenerator idGenerator) {
        this.idGenerator = idGenerator;
    }

    public String generate() throws IDGeneratorException {
        return idGenerator.generate();
    }

    public static StreamIdGenerator buildMemoryGenerator() {
        return new StreamIdGenerator(new MemIDGenerator());
    }

    public static StreamIdGenerator buildRandomGenerator() {
        return new StreamIdGenerator(new RandomIDGenerator());
    }

    public static StreamIdGenerator buildOxiaIdGenerator(AsyncOxiaClient client) {
        return new StreamIdGenerator(new OxiaIDGenerator(STREAM_KEY, client));
    }
}
