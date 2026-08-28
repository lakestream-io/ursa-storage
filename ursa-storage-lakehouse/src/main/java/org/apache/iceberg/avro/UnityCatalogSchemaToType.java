/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package org.apache.iceberg.avro;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Schema visitor for Unity Catalog that converts UUID types to STRING types.
 */
public class UnityCatalogSchemaToType extends SchemaToTypeExtended {

    public UnityCatalogSchemaToType(Schema root) {
        super(root);
    }

    @Override
    public Type primitive(Schema primitive) {
        // Check if this is a UUID logical type
        if (primitive.getLogicalType() != null) {
            if (primitive.getLogicalType().getName().equals(LogicalTypes.uuid().getName())) {
                // Convert UUID to STRING for Unity Catalog compatibility
                return Types.StringType.get();
            }
        }

        // For all other types, use the parent implementation
        return super.primitive(primitive);
    }

    public static Type visit(Schema schema, UnityCatalogSchemaToType visitor, boolean isVariantEnabled) {
        return SchemaToTypeExtended.visit(schema, visitor, isVariantEnabled);
    }
}