/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import java.util.Objects;
import java.util.Optional;

/**
 * Schema-evolution capabilities permitted by the destination table.
 *
 * <p>Every option is an {@link Optional} of {@link Boolean} so individual
 * fields can be overridden per stream while still inheriting the namespace
 * (and ultimately the engine) defaults. The provided static factories
 * pre-configure the booleans for common backends:
 *
 * <ul>
 *   <li>{@link #forIceberg()} / {@link #forDelta()} — permissive: allow
 *       {@code addColumn}, {@code addNullableColumn}, {@code widenType};
 *       deny everything else.</li>
 *   <li>{@link #forClickHouse()} — strict: allow only {@code addColumn} and
 *       {@code addNullableColumn}; deny everything else.</li>
 * </ul>
 *
 * <p>The factories wrap each value in {@code Optional.of(...)} so they remain
 * effective overrides at every layer; unset fields remain {@link Optional#empty()}
 * and fall through to the resolver's defaults.
 *
 * @param addColumn          allow adding a new (non-null) column
 * @param addNullableColumn  allow adding a new nullable column
 * @param dropColumn         allow dropping a column
 * @param widenType          allow widening a column's type (int → long)
 * @param narrowType         allow narrowing a column's type
 * @param renameColumn       allow renaming a column
 * @param reorderColumns     allow reordering columns
 * @param nullabilityRelax   allow relaxing required → optional
 * @param nullabilityTighten allow tightening optional → required
 */
public record EvolutionPolicy(
        Optional<Boolean> addColumn,
        Optional<Boolean> addNullableColumn,
        Optional<Boolean> dropColumn,
        Optional<Boolean> widenType,
        Optional<Boolean> narrowType,
        Optional<Boolean> renameColumn,
        Optional<Boolean> reorderColumns,
        Optional<Boolean> nullabilityRelax,
        Optional<Boolean> nullabilityTighten) {

    /** Canonical constructor: validates that no Optional field is {@code null}. */
    public EvolutionPolicy {
        Objects.requireNonNull(addColumn, "addColumn cannot be null; use Optional.empty()");
        Objects.requireNonNull(addNullableColumn,
                "addNullableColumn cannot be null; use Optional.empty()");
        Objects.requireNonNull(dropColumn, "dropColumn cannot be null; use Optional.empty()");
        Objects.requireNonNull(widenType, "widenType cannot be null; use Optional.empty()");
        Objects.requireNonNull(narrowType, "narrowType cannot be null; use Optional.empty()");
        Objects.requireNonNull(renameColumn, "renameColumn cannot be null; use Optional.empty()");
        Objects.requireNonNull(reorderColumns,
                "reorderColumns cannot be null; use Optional.empty()");
        Objects.requireNonNull(nullabilityRelax,
                "nullabilityRelax cannot be null; use Optional.empty()");
        Objects.requireNonNull(nullabilityTighten,
                "nullabilityTighten cannot be null; use Optional.empty()");
    }

    /**
     * Returns the permissive defaults for Apache Iceberg destinations:
     * {@code addColumn}, {@code addNullableColumn}, and {@code widenType} are
     * permitted; all other evolutions are denied.
     */
    public static EvolutionPolicy forIceberg() {
        return permissive();
    }

    /**
     * Returns the permissive defaults for Delta Lake destinations.
     * Identical to {@link #forIceberg()}.
     */
    public static EvolutionPolicy forDelta() {
        return permissive();
    }

    /**
     * Returns the strict defaults for ClickHouse destinations:
     * {@code addColumn} and {@code addNullableColumn} are permitted; all other
     * evolutions are denied.
     */
    public static EvolutionPolicy forClickHouse() {
        return new EvolutionPolicy(
                Optional.of(true),
                Optional.of(true),
                Optional.of(false),
                Optional.of(false),
                Optional.of(false),
                Optional.of(false),
                Optional.of(false),
                Optional.of(false),
                Optional.of(false));
    }

    private static EvolutionPolicy permissive() {
        return new EvolutionPolicy(
                Optional.of(true),
                Optional.of(true),
                Optional.of(false),
                Optional.of(true),
                Optional.of(false),
                Optional.of(false),
                Optional.of(false),
                Optional.of(false),
                Optional.of(false));
    }
}
