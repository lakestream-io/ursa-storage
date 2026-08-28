/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

/** Reserved columns added by the Kafka lakehouse encoders. */
public final class LakehouseFieldNames {

    public static final String META = "__meta";
    public static final String INTERNAL_KEY = "__key";

    private LakehouseFieldNames() {
    }
}
