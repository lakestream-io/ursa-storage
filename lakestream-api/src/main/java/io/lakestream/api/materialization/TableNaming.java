/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import io.lakestream.api.StreamIdentifier;
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
 * are implemented by {@link #toTableIdentifier(StreamIdentifier)}.
 *
 * <p>Supported template variables (case-sensitive):
 * <ul>
 *   <li>{@code ${stream.namespace}} — the stream's namespace</li>
 *   <li>{@code ${stream.name}} — the stream's name within the namespace</li>
 * </ul>
 *
 * @param tableNamespacePrefix optional namespace prefix to prepend; may be empty
 * @param tableNameTemplate    template for the derived table name (non-null, non-empty)
 */
public record TableNaming(Optional<String> tableNamespacePrefix, String tableNameTemplate) {

    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([^}]+)}");
    private static final String STREAM_NAMESPACE_VAR = "stream.namespace";
    private static final String STREAM_NAME_VAR = "stream.name";

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
     * <p>The interpolated table name is the {@code tableNameTemplate} with each
     * supported variable substituted. The table namespace component is
     * {@link #tableNamespacePrefix} when present (used as a literal — it is not
     * interpolated), otherwise {@code streamId.namespace()}.
     *
     * @param streamId the stream whose attributes are substituted into the template
     * @return the resulting table identifier (catalog implicit on the owning policy)
     * @throws IllegalArgumentException if the template references an unknown
     *     variable, or if the interpolated table name is blank
     * @throws NullPointerException if {@code streamId} is {@code null}
     */
    public TableIdentifier toTableIdentifier(StreamIdentifier streamId) {
        Objects.requireNonNull(streamId, "streamId");
        Matcher matcher = VARIABLE_PATTERN.matcher(tableNameTemplate);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            String variable = matcher.group(1);
            String replacement = switch (variable) {
                case STREAM_NAMESPACE_VAR -> streamId.namespace();
                case STREAM_NAME_VAR -> streamId.name();
                default -> throw new IllegalArgumentException(
                        "Unknown template variable: ${" + variable + "}");
            };
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
