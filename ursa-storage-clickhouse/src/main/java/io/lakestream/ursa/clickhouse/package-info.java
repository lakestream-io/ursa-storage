/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * ClickHouse implementation of the stream-to-table materialization SPI defined
 * in {@code ursa-storage-materialization}.
 *
 * <p>The module registers a single
 * {@link io.lakestream.ursa.materialization.TableMaterializerFactory} for
 * {@link io.lakestream.api.materialization.TableCatalogType#CLICKHOUSE}
 * via {@code META-INF/services} so the orchestrator's {@code ServiceLoader}
 * dispatch picks it up at runtime when a stream's effective materialization
 * resolves to a ClickHouse catalog.
 *
 * <p>The current implementation buffers rows in memory and flushes them via
 * batched {@code INSERT INTO} statements over the bundled
 * {@code com.clickhouse:clickhouse-jdbc} driver. Idempotency relies on the
 * destination table engine: {@code ReplacingMergeTree} when the policy is
 * {@code UPSERT} (or any primary key is provided) and {@code MergeTree} for the
 * default append-only path.
 *
 * <p>Schema service integration is provided by
 * {@link io.lakestream.ursa.clickhouse.ClickHouseTableSchemaService}, which
 * translates source schemas (via
 * {@link io.lakestream.ursa.clickhouse.AvroToClickHouseSchema}) into typed
 * {@link io.lakestream.ursa.clickhouse.ClickHouseSchema} descriptors, issues
 * {@code CREATE TABLE} on first use, {@code ALTER TABLE ADD COLUMN} for
 * additions, and rejects drop / type-change evolutions per
 * {@link io.lakestream.api.materialization.EvolutionPolicy#forClickHouse()}.
 * The materializer's row decoder switches between schema-service-driven
 * decoding (when a source schema version is set) and a JSON fallback for
 * unversioned streams.
 *
 * <h2>Testing</h2>
 *
 * <p>Unit tests run by default ({@code mvn test -pl ursa-storage-clickhouse}) and
 * use Mockito-backed JDBC stand-ins so they require neither Docker nor a live
 * ClickHouse instance.
 *
 * <p>Tag-gated end-to-end tests live in
 * {@code ClickHouseTableMaterializerEndToEndTest} and spin up a real
 * ClickHouse container via Testcontainers. They are excluded from the default
 * surefire run via the {@code excludeGroups=clickhouse} property and require
 * Docker. Opt in with:
 * <pre>{@code
 *   mvn -B -ntp test -pl ursa-storage-clickhouse \
 *       -Dgroups=clickhouse -DexcludeGroups=
 * }</pre>
 * The local {@code docker/docker-compose.yaml} also defines a {@code clickhouse}
 * service (ports {@code 8123} / {@code 9000}) for interactive development
 * outside the test harness.
 */
package io.lakestream.ursa.clickhouse;
