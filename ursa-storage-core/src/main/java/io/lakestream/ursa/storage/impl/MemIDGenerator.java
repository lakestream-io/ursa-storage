/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.ursa.storage.IDGenerator;
import java.util.concurrent.atomic.AtomicLong;

public class MemIDGenerator implements IDGenerator {

    public static final String NAME = "memory";

    private final AtomicLong id = new AtomicLong();

    public MemIDGenerator() {}

    @Override
    public String generate() {
        return String.valueOf(id.getAndIncrement());
    }

}
