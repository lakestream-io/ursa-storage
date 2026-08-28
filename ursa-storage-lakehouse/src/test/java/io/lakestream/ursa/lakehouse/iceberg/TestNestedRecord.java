/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import org.apache.iceberg.Schema;

public class TestNestedRecord extends TestRecord {
    public TestNestedRecord(Schema schema, String field1, int field2) {
        super(schema, field1, field2);
    }
}
