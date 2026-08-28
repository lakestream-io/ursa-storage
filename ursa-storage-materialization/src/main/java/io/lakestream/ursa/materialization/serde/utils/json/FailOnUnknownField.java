/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json;

import org.apache.avro.AvroTypeException;

public class FailOnUnknownField implements UnknownFieldListener {

    @Override
    public void onUnknownField(String name, Object value, String path) {
        throw new AvroTypeException("Field " + path + " is unknown");
    }

}
