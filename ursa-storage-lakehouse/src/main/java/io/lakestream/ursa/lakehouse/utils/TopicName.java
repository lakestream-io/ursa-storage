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
    /**
     * Prefix of the log names the catalog allocates for Lakestream-native streams, built as
     * {@code lakestream-native/<namespace>/<name>/partition-N}. Unlike the canonical form, the partition is
     * its own trailing segment, so {@link #get(String)} reads such a name as the namespace
     * {@code lakestream-native/<namespace>/<name>} holding a local name of {@code partition-N}. That reading
     * is what the compacted-object layout is built from and must not change; only identity resolution needs
     * to see through it.
     */
    private static final String NATIVE_NAME_PREFIX = "lakestream-native/";
    private static final Pattern PARTITION_SEGMENT = Pattern.compile("^partition-(\\d+)$");

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

    /**
     * Resolves the stream a name identifies, accepting both the canonical {@code namespace/name-partition-N}
     * form and the Lakestream-native allocation key {@code lakestream-native/namespace/name/partition-N}.
     *
     * <p>The returned local name never carries a partition, because a table is keyed by the stream rather
     * than by one of its partitions.
     */
    public static TopicName getStreamIdentity(String value) {
        Objects.requireNonNull(value, "value");
        String name = value.strip();
        return name.startsWith(NATIVE_NAME_PREFIX) ? nativeIdentity(name) : getPartitionedTopicName(name);
    }

    private static TopicName nativeIdentity(String value) {
        String remainder = value.substring(NATIVE_NAME_PREFIX.length());
        int partitionStart = remainder.lastIndexOf('/');
        if (partitionStart < 0
                || !PARTITION_SEGMENT.matcher(remainder.substring(partitionStart + 1)).matches()) {
            throw new IllegalArgumentException("Invalid topic name: " + value);
        }
        String fullName = remainder.substring(0, partitionStart);
        int nameStart = fullName.lastIndexOf('/');
        if (nameStart < 0) {
            throw new IllegalArgumentException("Invalid topic name: " + value);
        }
        return new TopicName(fullName.substring(0, nameStart), fullName.substring(nameStart + 1));
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
