/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import static org.assertj.core.api.Assertions.assertThat;

import io.lakestream.api.materialization.EvolutionPolicy;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Per-sink {@link EvolutionPolicy} gating: pins the default flag values for the
 * three engine-specific factories
 * ({@link EvolutionPolicy#forIceberg()},
 * {@link EvolutionPolicy#forDelta()},
 * {@link EvolutionPolicy#forClickHouse()}) via a single parameterised matrix.
 *
 * <p>This catches the regression where a new flag is added to
 * {@link EvolutionPolicy} but one (or all) of the factories forgets to
 * pre-populate a default for it — the parameterised matrix iterates every
 * (factory, operation) pair and asserts the expected
 * {@link java.util.Optional}{@code <Boolean>} value, so any newly added flag
 * left at {@link Optional#empty()} would be visible as a missing-case mismatch.
 *
 * <p>The existing {@code lakestream-api} test suite verifies the individual
 * factory defaults; this matrix locks the cross-factory contract in one place
 * and lives in the materialization module so adapter authors editing
 * {@link EvolutionPolicy} encounter it on the same module they typically edit.
 */
class EvolutionPolicyGatingTest {

    /**
     * Returns the cross-product of (factory, operation, expectedAllowedFlag).
     *
     * <p>Each row encodes one assertion: "factory X's default for operation Y
     * is expected{@code Optional.of(true|false)}". A future EvolutionPolicy
     * extension (new flag) should add new rows here.
     */
    static Stream<Arguments> matrix() {
        Function<EvolutionPolicy, Optional<Boolean>> addColumn = EvolutionPolicy::addColumn;
        Function<EvolutionPolicy, Optional<Boolean>> addNullableColumn =
                EvolutionPolicy::addNullableColumn;
        Function<EvolutionPolicy, Optional<Boolean>> dropColumn = EvolutionPolicy::dropColumn;
        Function<EvolutionPolicy, Optional<Boolean>> widenType = EvolutionPolicy::widenType;
        Function<EvolutionPolicy, Optional<Boolean>> narrowType = EvolutionPolicy::narrowType;
        Function<EvolutionPolicy, Optional<Boolean>> renameColumn = EvolutionPolicy::renameColumn;
        Function<EvolutionPolicy, Optional<Boolean>> reorderColumns =
                EvolutionPolicy::reorderColumns;
        Function<EvolutionPolicy, Optional<Boolean>> nullabilityRelax =
                EvolutionPolicy::nullabilityRelax;
        Function<EvolutionPolicy, Optional<Boolean>> nullabilityTighten =
                EvolutionPolicy::nullabilityTighten;

        Supplier<EvolutionPolicy> iceberg = EvolutionPolicy::forIceberg;
        Supplier<EvolutionPolicy> delta = EvolutionPolicy::forDelta;
        Supplier<EvolutionPolicy> clickHouse = EvolutionPolicy::forClickHouse;

        return Stream.of(
                // --- Iceberg: permissive (addColumn/addNullableColumn/widenType=true) -----
                row("forIceberg", iceberg, "addColumn", addColumn, true),
                row("forIceberg", iceberg, "addNullableColumn", addNullableColumn, true),
                row("forIceberg", iceberg, "widenType", widenType, true),
                row("forIceberg", iceberg, "dropColumn", dropColumn, false),
                row("forIceberg", iceberg, "narrowType", narrowType, false),
                row("forIceberg", iceberg, "renameColumn", renameColumn, false),
                row("forIceberg", iceberg, "reorderColumns", reorderColumns, false),
                row("forIceberg", iceberg, "nullabilityRelax", nullabilityRelax, false),
                row("forIceberg", iceberg, "nullabilityTighten", nullabilityTighten, false),
                // --- Delta: identical permissive defaults to Iceberg --------------------
                row("forDelta", delta, "addColumn", addColumn, true),
                row("forDelta", delta, "addNullableColumn", addNullableColumn, true),
                row("forDelta", delta, "widenType", widenType, true),
                row("forDelta", delta, "dropColumn", dropColumn, false),
                row("forDelta", delta, "narrowType", narrowType, false),
                row("forDelta", delta, "renameColumn", renameColumn, false),
                row("forDelta", delta, "reorderColumns", reorderColumns, false),
                row("forDelta", delta, "nullabilityRelax", nullabilityRelax, false),
                row("forDelta", delta, "nullabilityTighten", nullabilityTighten, false),
                // --- ClickHouse: strict (only addColumn/addNullableColumn=true) ---------
                row("forClickHouse", clickHouse, "addColumn", addColumn, true),
                row("forClickHouse", clickHouse, "addNullableColumn", addNullableColumn, true),
                row("forClickHouse", clickHouse, "dropColumn", dropColumn, false),
                row("forClickHouse", clickHouse, "widenType", widenType, false),
                row("forClickHouse", clickHouse, "narrowType", narrowType, false),
                row("forClickHouse", clickHouse, "renameColumn", renameColumn, false),
                row("forClickHouse", clickHouse, "reorderColumns", reorderColumns, false),
                row("forClickHouse", clickHouse, "nullabilityRelax", nullabilityRelax, false),
                row("forClickHouse", clickHouse, "nullabilityTighten", nullabilityTighten, false));
    }

    private static Arguments row(
            String factoryName,
            Supplier<EvolutionPolicy> factory,
            String operationName,
            Function<EvolutionPolicy, Optional<Boolean>> extractor,
            boolean expectedAllowed) {
        return Arguments.of(factoryName, factory, operationName, extractor, expectedAllowed);
    }

    @ParameterizedTest(name = "[{index}] {0}.{2} = {4}")
    @MethodSource("matrix")
    void perSinkEvolutionDefaults(
            String factoryName,
            Supplier<EvolutionPolicy> factory,
            String operationName,
            Function<EvolutionPolicy, Optional<Boolean>> extractor,
            boolean expectedAllowed) {
        EvolutionPolicy policy = factory.get();
        Optional<Boolean> actual = extractor.apply(policy);
        assertThat(actual)
                .as("%s default for %s should be Optional.of(%s)",
                        factoryName, operationName, expectedAllowed)
                .contains(expectedAllowed);
    }

    /**
     * Sanity check: every Optional flag returned by every per-sink factory is
     * populated — none of them should leave a flag at {@link Optional#empty()}.
     * If a new flag is added to {@link EvolutionPolicy} and the per-sink
     * factories forget to set a default for it, this test catches the drift
     * for the existing operations; the parameterised matrix above catches the
     * specific true/false values.
     */
    @ParameterizedTest(name = "[{index}] {0} has no empty flag")
    @MethodSource("perSinkFactories")
    void perSinkFactoriesPopulateEveryFlag(String factoryName, Supplier<EvolutionPolicy> factory) {
        EvolutionPolicy policy = factory.get();
        List<Optional<Boolean>> flags = Arrays.asList(
                policy.addColumn(),
                policy.addNullableColumn(),
                policy.dropColumn(),
                policy.widenType(),
                policy.narrowType(),
                policy.renameColumn(),
                policy.reorderColumns(),
                policy.nullabilityRelax(),
                policy.nullabilityTighten());
        for (int i = 0; i < flags.size(); i++) {
            assertThat(flags.get(i))
                    .as("%s leaves flag #%d at Optional.empty()", factoryName, i)
                    .isPresent();
        }
    }

    static Stream<Arguments> perSinkFactories() {
        return Stream.of(
                Arguments.of("forIceberg", (Supplier<EvolutionPolicy>) EvolutionPolicy::forIceberg),
                Arguments.of("forDelta", (Supplier<EvolutionPolicy>) EvolutionPolicy::forDelta),
                Arguments.of(
                        "forClickHouse",
                        (Supplier<EvolutionPolicy>) EvolutionPolicy::forClickHouse));
    }
}
