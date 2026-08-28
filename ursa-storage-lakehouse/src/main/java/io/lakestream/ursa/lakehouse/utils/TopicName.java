/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Protocol-neutral Kafka/Lakestream topic name in {@code namespace/name-partition-N} form. */
public final class TopicName {
    public static final String DEFAULT_NAMESPACE = "default";
    private static final Pattern PARTITION_SUFFIX = Pattern.compile("-partition-(\\d+)$");

    private final String namespace;
    private final String localName;

    private TopicName(String namespace, String localName) {
        this.namespace = requireNamespace(namespace);
        this.localName = requireLocalName(localName);
    }

    public static TopicName get(String value) {
        Objects.requireNonNull(value, "value");
        String name = value.strip();
        if (name.isEmpty() || name.contains("://")) {
            throw new IllegalArgumentException("Invalid topic name: " + value);
        }
        int separator = name.lastIndexOf('/');
        return separator < 0
            ? new TopicName(DEFAULT_NAMESPACE, name)
            : new TopicName(name.substring(0, separator), name.substring(separator + 1));
    }

    public static TopicName getPartitionedTopicName(String value) {
        return get(get(value).getPartitionedTopicName());
    }

    public String getPartitionedTopicName() {
        Matcher matcher = PARTITION_SUFFIX.matcher(localName);
        String baseName = matcher.find() ? localName.substring(0, matcher.start()) : localName;
        return namespace + "/" + baseName;
    }

    public int getPartitionIndex() {
        Matcher matcher = PARTITION_SUFFIX.matcher(localName);
        return matcher.find() ? Integer.parseInt(matcher.group(1)) : -1;
    }

    public String getLocalName() {
        return localName;
    }

    public String getNamespace() {
        return namespace;
    }

    @Override
    public String toString() {
        return namespace + "/" + localName;
    }

    private static String requireNamespace(String value) {
        if (value == null || value.isBlank() || value.startsWith("/")
                || value.endsWith("/") || value.contains("//")) {
            throw new IllegalArgumentException("Invalid topic namespace: " + value);
        }
        return value;
    }

    private static String requireLocalName(String value) {
        if (value == null || value.isBlank() || value.contains("/")) {
            throw new IllegalArgumentException("Invalid topic name: " + value);
        }
        return value;
    }
}
