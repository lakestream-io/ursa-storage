/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The log name a catalog allocates for one partition of a Lakestream-native stream:
 * {@code lakestream-native/<namespace>/<name>/partition-N}.
 *
 * <p>This name travels well beyond the catalog that mints it: it is the compaction task's topic, the
 * prefix compacted objects are laid out under, and the string sinks resolve a table from. It is the
 * single definition of that format, so producers and consumers cannot drift apart.
 *
 * <p>Note the two ways it differs from the canonical {@code namespace/name-partition-N} form. The
 * partition is a trailing path segment rather than a suffix on the name, and the namespace may itself
 * contain slashes, so the stream name is the last segment before the partition rather than the second
 * segment overall.
 */
public final class NativeLogName {

    private static final String PREFIX = "lakestream-native/";
    private static final String PARTITION_SEPARATOR = "/partition-";
    private static final Pattern PARTITION_SEGMENT = Pattern.compile("^partition-(\\d+)$");

    private NativeLogName() {
    }

    /** One parsed log name: the stream it belongs to, and which of its partitions it is. */
    public record Parsed(StreamIdentifier stream, int partition) {

        /** Canonical constructor: validates non-null fields and a non-negative partition. */
        public Parsed {
            Objects.requireNonNull(stream, "stream");
            if (partition < 0) {
                throw new IllegalArgumentException("partition must not be negative: " + partition);
            }
        }
    }

    /** Builds the log name for one partition of {@code stream}. */
    public static String of(StreamIdentifier stream, int partition) {
        Objects.requireNonNull(stream, "stream");
        if (partition < 0) {
            throw new IllegalArgumentException("partition must not be negative: " + partition);
        }
        return PREFIX + stream.fullName() + PARTITION_SEPARATOR + partition;
    }

    /**
     * Whether {@code value} is shaped like a native log name. A caller that also handles other name
     * forms tests this before {@link #parse(String)}, which rejects anything else.
     */
    public static boolean hasNativePrefix(String value) {
        return value != null && value.strip().startsWith(PREFIX);
    }

    /**
     * Reads back a name built by {@link #of(StreamIdentifier, int)}.
     *
     * @throws IllegalArgumentException if {@code value} is not a native log name
     */
    public static Parsed parse(String value) {
        Objects.requireNonNull(value, "value");
        String stripped = value.strip();
        if (!stripped.startsWith(PREFIX)) {
            throw new IllegalArgumentException("Not a Lakestream-native log name: " + value);
        }
        String remainder = stripped.substring(PREFIX.length());
        int partitionStart = remainder.lastIndexOf('/');
        if (partitionStart < 0) {
            throw new IllegalArgumentException("Invalid Lakestream-native log name: " + value);
        }
        Matcher partition = PARTITION_SEGMENT.matcher(remainder.substring(partitionStart + 1));
        if (!partition.matches()) {
            throw new IllegalArgumentException("Invalid Lakestream-native log name: " + value);
        }
        String fullName = remainder.substring(0, partitionStart);
        int nameStart = fullName.lastIndexOf('/');
        if (nameStart < 0) {
            throw new IllegalArgumentException("Invalid Lakestream-native log name: " + value);
        }
        String namespace = fullName.substring(0, nameStart);
        String name = fullName.substring(nameStart + 1);
        if (namespace.isBlank() || name.isBlank()) {
            throw new IllegalArgumentException("Invalid Lakestream-native log name: " + value);
        }
        return new Parsed(StreamIdentifier.of(namespace, name), Integer.parseInt(partition.group(1)));
    }
}
