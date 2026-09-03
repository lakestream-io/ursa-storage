/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableNaming;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Resolves the table a stream materializes into, from the log name a compaction task carries.
 *
 * <p>The writer resolves this through the materialization policy's {@link TableNaming}. The commit
 * runner only ever sees the log name, so it resolves it here - against the same template, and against
 * the stream properties that ride along on the task. Both sides therefore answer with one rule; a
 * template that renames tables moves them together rather than splitting the two apart.
 *
 * <p>With no template configured the answer is the stream's own namespace and name, which is what the
 * default template {@code ${stream.name}} produces.
 */
public final class StreamTableNaming {

    /** Overrides the table name a stream materializes into. See {@code TableNaming} for the syntax. */
    public static final String TABLE_NAME_TEMPLATE_PROPERTY = "tableNameTemplate";

    private StreamTableNaming() {
    }

    /**
     * Resolves the table for {@code logName}.
     *
     * @param logName the compaction task's topic, in either the canonical or the native log-name form
     * @param properties the task's effective configuration, which carries both the template and the
     *                   stream properties a template may interpolate
     */
    public static TableIdentifier resolve(String logName, Properties properties) {
        TopicName identity = TopicName.getStreamIdentity(logName);
        StreamIdentifier stream = StreamIdentifier.of(identity.getNamespace(), identity.getLocalName());
        String template = properties == null ? null : properties.getProperty(TABLE_NAME_TEMPLATE_PROPERTY);
        if (template == null || template.isBlank()) {
            return new TableIdentifier(stream.namespace(), stream.name());
        }
        return new TableNaming(Optional.empty(), template).toTableIdentifier(stream, asMap(properties));
    }

    private static Map<String, String> asMap(Properties properties) {
        Map<String, String> map = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            map.put(key, properties.getProperty(key));
        }
        return map;
    }
}
