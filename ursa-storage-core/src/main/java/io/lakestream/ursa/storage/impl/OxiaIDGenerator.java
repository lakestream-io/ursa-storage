/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.ursa.storage.IDGenerator;
import io.lakestream.ursa.storage.impl.exception.IDGeneratorException;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.PutResult;
import java.util.concurrent.ExecutionException;

public class OxiaIDGenerator implements IDGenerator {

    public static final String NAME = "oxia";

    private final AsyncOxiaClient client;

    private final String key;

    public OxiaIDGenerator(String key, AsyncOxiaClient client) {
        this.key = key;
        this.client = client;
    }

    @Override
    public String generate() throws IDGeneratorException {
        try {
            PutResult putResult = client.put(key, new byte[0]).get();
            return String.valueOf(putResult.version().modificationsCount());
        } catch (InterruptedException | ExecutionException e) {
            throw new IDGeneratorException("Failed to generate id", e);
        }
    }
}
