/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.utils.json.schema;

public enum Format {
    DATE_TIME("date-time"),
    DATE("date"),
    TIME("time"),
    EMAIL("email"),
    URI("uri"),
    UUID("uuid"),
    IPV4("ipv4"),
    IPV6("ipv6"),
    HOSTNAME("hostname");

    private final String value;

    Format(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static Format fromValue(String value) {
        for (Format format : Format.values()) {
            if (format.value.equals(value)) {
                return format;
            }
        }
        return null; // Return null for unknown formats since not all formats need to be predefined
    }
}
