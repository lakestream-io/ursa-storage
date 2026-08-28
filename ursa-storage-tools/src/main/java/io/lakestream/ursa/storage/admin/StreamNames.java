/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utilities for opaque stream names with numeric partition suffixes. */
final class StreamNames {

    private static final Pattern PARTITION_SUFFIX = Pattern.compile("^(.*)-partition-(\\d+)$");

    private StreamNames() {
    }

    static String normalize(String name) {
        String value = Objects.requireNonNull(name, "name").trim();
        int start = 0;
        int end = value.length();
        while (start < end && value.charAt(start) == '/') {
            start++;
        }
        while (end > start && value.charAt(end - 1) == '/') {
            end--;
        }
        value = value.substring(start, end);
        if (value.isBlank()) {
            throw new IllegalArgumentException("Stream name must not be blank");
        }
        return value;
    }

    static String baseName(String name) {
        String normalized = normalize(name);
        Matcher matcher = PARTITION_SUFFIX.matcher(normalized);
        return matcher.matches() ? matcher.group(1) : normalized;
    }

    static String partitionName(String name, int partition) {
        if (partition < 0) {
            throw new IllegalArgumentException("Partition must be non-negative");
        }
        return baseName(name) + "-partition-" + partition;
    }
}
