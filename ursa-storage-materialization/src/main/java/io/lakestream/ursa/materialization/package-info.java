/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
/**
 * Public Service-Provider Interface (SPI) of the stream-to-table
 * materialization framework.
 *
 * <p>Sink back-ends (lakehouse for T8, ClickHouse for T11) plug in by
 * implementing {@link io.lakestream.ursa.materialization.TableMaterializer}
 * and {@link io.lakestream.ursa.materialization.TableMaterializerFactory},
 * registering the factory through {@link java.util.ServiceLoader}.
 *
 * <p>The orchestrator (T9 / T10) drives sinks through
 * {@link io.lakestream.ursa.materialization.MaterializationService}, handing
 * the service a {@link io.lakestream.ursa.materialization.MaterializationRuntime}
 * bag of framework services at startup and individual
 * {@link io.lakestream.ursa.materialization.MaterializationTask} units of
 * work at runtime.
 *
 * <p>Supporting types ({@code CommitResult}, {@code MaterializationContext},
 * {@code FailureRecord}, {@code MaterializationServiceConfig},
 * {@code MaterializationException}, {@code MaterializationMetrics},
 * {@code FailureMessageHandler}) round out the contract so sinks never have to
 * depend on lakehouse-specific observability or configuration code.
 *
 * <p>See {@code docs/lip/LIP-161-Table-Materialization-Framework.md} (added
 * in T15) for the design rationale.
 */
package io.lakestream.ursa.materialization;
