/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.ursa.storage.IDGenerator;
import java.util.UUID;

public class RandomIDGenerator implements IDGenerator {

    public static final String NAME = "random";

    @Override
    public String generate() {
        return UUID.randomUUID().toString();
    }
}
