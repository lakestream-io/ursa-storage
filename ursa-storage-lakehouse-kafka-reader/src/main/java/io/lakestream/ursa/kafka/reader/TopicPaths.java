/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.kafka.reader;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class TopicPaths {

    private static final String DEFAULT_NAMESPACE = "default";
    private static final Pattern PARTITION_SUFFIX = Pattern.compile("^(.*)-partition-(\\d+)$");

    private TopicPaths() {
    }

    static String canonicalLogName(String name) {
        Objects.requireNonNull(name, "name");
        String value = stripSlashes(name.trim());
        if (value.contains("://")) {
            throw new IllegalArgumentException("URI-style log names are not supported: " + name);
        }
        String[] components = value.split("/", -1);
        if (components.length == 2) {
            return canonical(components[0], components[1]);
        }
        if (components.length == 1 && !components[0].isBlank()) {
            return canonical(DEFAULT_NAMESPACE, components[0]);
        }
        throw new IllegalArgumentException("Invalid log name: " + name);
    }

    static String storagePath(String storageRoot, String logName) {
        Components components = components(canonicalLogName(logName));
        return stripTrailingSlashes(storageRoot) + "/" + components.namespace
                + "/" + partitionedStreamName(components.name);
    }

    private static String canonical(String namespace, String name) {
        if (namespace.isBlank() || name.isBlank()) {
            throw new IllegalArgumentException("Invalid log name components");
        }
        return namespace + "/" + name;
    }

    private static Components components(String logName) {
        String[] components = logName.split("/", -1);
        return new Components(components[0], components[1]);
    }

    private static String partitionedStreamName(String name) {
        Matcher matcher = PARTITION_SUFFIX.matcher(name);
        return matcher.matches() ? matcher.group(1) : name;
    }

    private static String stripSlashes(String value) {
        int start = 0;
        int end = value.length();
        while (start < end && value.charAt(start) == '/') {
            start++;
        }
        while (end > start && value.charAt(end - 1) == '/') {
            end--;
        }
        return value.substring(start, end);
    }

    private static String stripTrailingSlashes(String value) {
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == '/') {
            end--;
        }
        return value.substring(0, end);
    }

    private record Components(String namespace, String name) {
    }
}
