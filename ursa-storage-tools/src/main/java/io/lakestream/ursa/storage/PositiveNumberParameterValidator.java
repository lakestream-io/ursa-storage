/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

public class PositiveNumberParameterValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
        if (Integer.parseInt(value) <= 0) {
            throw new ParameterException("Parameter " + name + " should be > 0 (found " + value + ")");
        }
    }
}
