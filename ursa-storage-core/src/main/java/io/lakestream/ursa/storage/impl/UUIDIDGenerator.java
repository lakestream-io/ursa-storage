/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.ursa.storage.IDGenerator;
import io.lakestream.ursa.storage.impl.exception.IDGeneratorException;
import java.util.UUID;

public class UUIDIDGenerator implements IDGenerator {

    @Override
    public String generate() throws IDGeneratorException {
        return UUID.randomUUID().toString();
    }

}
