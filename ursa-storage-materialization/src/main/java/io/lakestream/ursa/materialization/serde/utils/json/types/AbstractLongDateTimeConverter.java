/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json.types;

import org.apache.avro.Schema;

public abstract class AbstractLongDateTimeConverter extends AbstractDateTimeConverter {

    @Override
    protected Object convertNumber(Number numberValue) {
        return numberValue.longValue();
    }

    @Override
    protected Schema.Type getUnderlyingSchemaType() {
        return Schema.Type.LONG;
    }

}
