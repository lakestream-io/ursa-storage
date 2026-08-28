/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.ursa.storage.IDGeneratorWithDate;
import io.lakestream.ursa.storage.impl.exception.IDGeneratorException;

public class UUIDWithDateIDGenerator extends IDGeneratorWithDate {

    private static final UUIDIDGenerator uuididGenerator = new UUIDIDGenerator();

    @Override
    protected String generateId() throws IDGeneratorException {
        return uuididGenerator.generate();
    }
}
