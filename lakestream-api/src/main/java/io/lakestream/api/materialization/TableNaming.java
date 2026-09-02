/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import io.lakestream.api.StreamIdentifier;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Namespace-level template used to derive a {@link TableIdentifier} for each
 * stream during policy resolution.
 *
 * <p>The {@code tableNameTemplate} is mandatory and may reference stream
 * attributes (e.g., {@code ${stream.name}}); the actual interpolation rules
 * are implemented by {@link #toTableIdentifier(StreamIdentifier, Map)}.
 *
 * <p>Supported template variables (case-sensitive):
 * <ul>
 *   <li>{@code ${stream.namespace}} — the stream's namespace</li>
 *   <li>{@code ${stream.name}} — the stream's name within the namespace</li>
 *   <li>{@code ${stream.property.<key>}} — the stream property named
 *       {@code <key>}; only resolved by {@link #toTableIdentifier(StreamIdentifier, Map)}.
 *       The single-argument {@link #toTableIdentifier(StreamIdentifier)} has no
 *       properties available and rejects templates using this variable</li>
 * </ul>
 *
 * @param tableNamespacePrefix optional namespace prefix to prepend; may be empty
 * @param tableNameTemplate    template for the derived table name (non-null, non-empty)
 */
public record TableNaming(Optional<String> tableNamespacePrefix, String tableNameTemplate) {

    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([^}]+)}");
    private static final String STREAM_NAMESPACE_VAR = "stream.namespace";
    private static final String STREAM_NAME_VAR = "stream.name";
    private static final String STREAM_PROPERTY_PREFIX = "stream.property.";

    /** Canonical constructor: validates the optional and required template fields. */
    public TableNaming {
        Objects.requireNonNull(tableNamespacePrefix,
                "tableNamespacePrefix cannot be null; use Optional.empty()");
        Objects.requireNonNull(tableNameTemplate, "tableNameTemplate");
        if (tableNameTemplate.isEmpty()) {
            throw new IllegalArgumentException("tableNameTemplate must not be empty");
        }
    }

    /**
     * Interpolates {@link #tableNameTemplate} with the given stream's attributes
     * and returns the resulting {@link TableIdentifier}.
     *
     * <p>Equivalent to {@link #toTableIdentifier(StreamIdentifier, Map)} with no
     * stream properties available; a template referencing
     * {@code ${stream.property.<key>}} is therefore rejected.
     *
     * @param streamId the stream whose attributes are substituted into the template
     * @return the resulting table identifier (catalog implicit on the owning policy)
     * @throws IllegalArgumentException if the template references an unknown
     *     variable, or if the interpolated table name is blank
     * @throws NullPointerException if {@code streamId} is {@code null}
     */
    public TableIdentifier toTableIdentifier(StreamIdentifier streamId) {
        return toTableIdentifier(streamId, Map.of());
    }

    /**
     * Interpolates {@link #tableNameTemplate} with the given stream's attributes
     * and properties, and returns the resulting {@link TableIdentifier}.
     *
     * <p>The interpolated table name is the {@code tableNameTemplate} with each
     * supported variable substituted. The table namespace component is
     * {@link #tableNamespacePrefix} when present (used as a literal — it is not
     * interpolated), otherwise {@code streamId.namespace()}.
     *
     * @param streamId   the stream whose attributes are substituted into the template
     * @param properties the stream's properties, consulted for
     *                   {@code ${stream.property.<key>}} variables
     * @return the resulting table identifier (catalog implicit on the owning policy)
     * @throws IllegalArgumentException if the template references an unknown
     *     variable, references a stream property that is unset or blank in
     *     {@code properties}, or if the interpolated table name is blank
     * @throws NullPointerException if {@code streamId} or {@code properties} is {@code null}
     */
    public TableIdentifier toTableIdentifier(StreamIdentifier streamId, Map<String, String> properties) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(properties, "properties");
        Matcher matcher = VARIABLE_PATTERN.matcher(tableNameTemplate);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            String variable = matcher.group(1);
            String replacement;
            if (STREAM_NAMESPACE_VAR.equals(variable)) {
                replacement = streamId.namespace();
            } else if (STREAM_NAME_VAR.equals(variable)) {
                replacement = streamId.name();
            } else if (variable.startsWith(STREAM_PROPERTY_PREFIX)) {
                String key = variable.substring(STREAM_PROPERTY_PREFIX.length());
                replacement = properties.get(key);
                if (replacement == null || replacement.isBlank()) {
                    throw new IllegalArgumentException(
                            "Stream property '" + key + "' is not set for " + streamId.fullName());
                }
            } else {
                throw new IllegalArgumentException("Unknown template variable: ${" + variable + "}");
            }
            matcher.appendReplacement(out, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(out);
        String interpolatedName = out.toString();
        if (interpolatedName.isBlank()) {
            throw new IllegalArgumentException(
                    "Interpolated table name is empty for template: " + tableNameTemplate);
        }
        String namespacePart = tableNamespacePrefix.orElseGet(streamId::namespace);
        return new TableIdentifier(namespacePart, interpolatedName);
    }
}
