/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.assertj.core.api.Assertions.assertThat;

import io.lakestream.api.StreamIdentifier;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TableMaterializationPolicy#resolve} — verifies the
 * namespace → stream deep-merge semantics, the stream-side opt-out, the
 * catalog-lookup short-circuits, list-replace behaviour, and the
 * connection-overrides / table-naming asymmetries.
 */
class TableMaterializationPolicyResolveTest {

    private static final StreamIdentifier STREAM =
            StreamIdentifier.of("public/default", "orders");

    private static final TableCatalog ICEBERG_CATALOG = new TableCatalog(
            "iceberg-glue", TableCatalogType.ICEBERG, Map.of(), Map.of());
    private static final TableCatalog DELTA_CATALOG = new TableCatalog(
            "delta-uc", TableCatalogType.DELTA, Map.of(), Map.of());

    private static Function<String, Optional<TableCatalog>> lookup(TableCatalog... catalogs) {
        Map<String, TableCatalog> byName = new java.util.LinkedHashMap<>();
        for (TableCatalog c : catalogs) {
            byName.put(c.name(), c);
        }
        return name -> Optional.ofNullable(byName.get(name));
    }

    @Test
    void bothEmptyReturnsEmpty() {
        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.empty(), Optional.empty(), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isEmpty();
    }

    @Test
    void streamEnabledFalseShortCircuits() {
        TableMaterializationPolicy ns = namespaceFullyPopulated();
        TableMaterializationPolicy stream = streamWithOnly(p -> p
                .enabled(Optional.of(false)));

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(ns), Optional.of(stream), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isEmpty();
    }

    @Test
    void namespaceOnlyMaterializes() {
        TableMaterializationPolicy ns = namespaceFullyPopulated();

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(ns), Optional.empty(), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isPresent();
        ResolvedMaterialization rm = result.get();
        assertThat(rm.catalog()).isEqualTo(ICEBERG_CATALOG);
        // Template "${stream.name}" with prefix "warehouse" → (warehouse, orders).
        assertThat(rm.tableIdentifier()).isEqualTo(new TableIdentifier("warehouse", "orders"));
        // Framework was inherited from namespace.
        assertThat(rm.effectivePolicy().framework())
                .isPresent()
                .get()
                .extracting(FrameworkConf::writeMode)
                .isEqualTo(Optional.of(WriteMode.APPEND));
        // tableNaming is preserved on the effective policy for inspection.
        assertThat(rm.effectivePolicy().tableNaming()).isPresent();
    }

    @Test
    void streamCatalogRefOverridesNamespace() {
        TableMaterializationPolicy ns = namespaceFullyPopulated();
        TableMaterializationPolicy stream = streamWithOnly(p -> p
                .catalogRef(Optional.of("delta-uc")));

        AtomicReference<String> seenName = new AtomicReference<>();
        Function<String, Optional<TableCatalog>> spy = name -> {
            seenName.set(name);
            return name.equals("delta-uc") ? Optional.of(DELTA_CATALOG) : Optional.empty();
        };

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(ns), Optional.of(stream), STREAM, spy);

        assertThat(seenName.get()).isEqualTo("delta-uc");
        assertThat(result).isPresent();
        assertThat(result.get().catalog()).isEqualTo(DELTA_CATALOG);
        assertThat(result.get().effectivePolicy().catalogRef()).contains("delta-uc");
    }

    @Test
    void streamTableIdentifierShadowsTemplate() {
        TableMaterializationPolicy ns = namespaceFullyPopulated();
        TableMaterializationPolicy stream = streamWithOnly(p -> p
                .tableIdentifier(Optional.of(new TableIdentifier("custom_ns", "custom_table"))));

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(ns), Optional.of(stream), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isPresent();
        assertThat(result.get().tableIdentifier())
                .isEqualTo(new TableIdentifier("custom_ns", "custom_table"));
        assertThat(result.get().effectivePolicy().tableIdentifier())
                .contains(new TableIdentifier("custom_ns", "custom_table"));
    }

    @Test
    void evolutionFieldOverridesStreamOverNamespace() {
        TableMaterializationPolicy ns = namespaceFullyPopulated();
        // forIceberg() sets addColumn=true, dropColumn=false; stream overrides only dropColumn.
        EvolutionPolicy streamEvolution = new EvolutionPolicy(
                Optional.empty(),
                Optional.empty(),
                Optional.of(true),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        TableMaterializationPolicy stream = streamWithOnly(p -> p
                .evolution(Optional.of(streamEvolution)));

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(ns), Optional.of(stream), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isPresent();
        EvolutionPolicy effective = result.get().effectivePolicy().evolution().orElseThrow();
        assertThat(effective.addColumn()).contains(true);           // inherited
        assertThat(effective.addNullableColumn()).contains(true);   // inherited
        assertThat(effective.dropColumn()).contains(true);          // overridden
        assertThat(effective.widenType()).contains(true);           // inherited
        assertThat(effective.narrowType()).contains(false);         // inherited
        assertThat(effective.renameColumn()).contains(false);       // inherited
        assertThat(effective.reorderColumns()).contains(false);     // inherited
        assertThat(effective.nullabilityRelax()).contains(false);   // inherited
        assertThat(effective.nullabilityTighten()).contains(false); // inherited
    }

    @Test
    void frameworkSubFieldsMerge() {
        TableMaterializationPolicy ns = TableMaterializationPolicy.empty();
        ns = new TableMaterializationPolicy(
                Optional.of("iceberg-glue"),
                Optional.of(new TableNaming(Optional.of("warehouse"), "${stream.name}")),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new FrameworkConf(
                        Optional.of(WriteMode.APPEND),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());

        ErrorHandling errorHandling = new ErrorHandling(ErrorMode.SKIP, Optional.empty());
        TableMaterializationPolicy stream = streamWithOnly(p -> p
                .framework(Optional.of(new FrameworkConf(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(errorHandling),
                        Optional.empty()))));

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(ns), Optional.of(stream), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isPresent();
        FrameworkConf merged = result.get().effectivePolicy().framework().orElseThrow();
        assertThat(merged.writeMode()).contains(WriteMode.APPEND);
        assertThat(merged.errorHandling()).contains(errorHandling);
    }

    @Test
    void primaryKeyListReplaces() {
        TableMaterializationPolicy ns = withPrimaryKey(namespaceFullyPopulated(),
                List.of("a", "b"));
        TableMaterializationPolicy stream = streamWithOnly(p -> p
                .primaryKey(Optional.of(List.of("c"))));

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(ns), Optional.of(stream), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isPresent();
        assertThat(result.get().effectivePolicy().primaryKey())
                .contains(List.of("c"));
    }

    @Test
    void partitionByListReplaces() {
        PartitionSpec nsPartition = new PartitionSpec(
                "x", PartitionTransform.IDENTITY, Optional.empty());
        PartitionSpec streamPartition = new PartitionSpec(
                "t", PartitionTransform.DAY, Optional.empty());
        TableConf nsTable = new TableConf(
                Optional.empty(),
                Optional.of(List.of(nsPartition)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        TableConf streamTable = new TableConf(
                Optional.empty(),
                Optional.of(List.of(streamPartition)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        TableMaterializationPolicy ns = withTable(namespaceFullyPopulated(), nsTable);
        TableMaterializationPolicy stream = streamWithOnly(p -> p
                .table(Optional.of(streamTable)));

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(ns), Optional.of(stream), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isPresent();
        TableConf effective = result.get().effectivePolicy().table().orElseThrow();
        assertThat(effective.partitionBy()).contains(List.of(streamPartition));
    }

    @Test
    void sortByListReplaces() {
        SortColumn nsSort = new SortColumn("a", SortDirection.ASC, false);
        SortColumn streamSort = new SortColumn("b", SortDirection.DESC, true);
        TableConf nsTable = new TableConf(
                Optional.empty(),
                Optional.empty(),
                Optional.of(List.of(nsSort)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        TableConf streamTable = new TableConf(
                Optional.empty(),
                Optional.empty(),
                Optional.of(List.of(streamSort)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        TableMaterializationPolicy ns = withTable(namespaceFullyPopulated(), nsTable);
        TableMaterializationPolicy stream = streamWithOnly(p -> p
                .table(Optional.of(streamTable)));

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(ns), Optional.of(stream), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isPresent();
        TableConf effective = result.get().effectivePolicy().table().orElseThrow();
        assertThat(effective.sortBy()).contains(List.of(streamSort));
    }

    @Test
    void connectionOverridesStreamOnly() {
        TableMaterializationPolicy ns = withConnectionOverrides(
                namespaceFullyPopulated(), Map.of("a", "1"));
        TableMaterializationPolicy stream = withConnectionOverrides(
                streamWithOnly(p -> p), Map.of("b", "2"));

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(ns), Optional.of(stream), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isPresent();
        // Namespace's connectionOverrides are ignored; only stream's are kept.
        assertThat(result.get().effectivePolicy().connectionOverrides())
                .isEqualTo(Map.of("b", "2"));
    }

    @Test
    void missingCatalogReturnsEmpty() {
        TableMaterializationPolicy ns = new TableMaterializationPolicy(
                Optional.of("nonexistent"),
                Optional.of(new TableNaming(Optional.of("warehouse"), "${stream.name}")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(ns), Optional.empty(), STREAM, name -> Optional.empty());

        assertThat(result).isEmpty();
    }

    @Test
    void missingTableIdentifierReturnsEmpty() {
        // catalogRef is set, but no tableNaming on namespace and no override on stream.
        TableMaterializationPolicy ns = new TableMaterializationPolicy(
                Optional.of("iceberg-glue"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(ns), Optional.empty(), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isEmpty();
    }

    @Test
    void tableNamingTakenFromNamespace() {
        TableNaming namespaceNaming = new TableNaming(Optional.of("warehouse"), "${stream.name}");
        TableMaterializationPolicy ns = new TableMaterializationPolicy(
                Optional.of("iceberg-glue"),
                Optional.of(namespaceNaming),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());

        // Stream's policy has no tableNaming of its own.
        TableMaterializationPolicy stream = TableMaterializationPolicy.empty();

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(ns), Optional.of(stream), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isPresent();
        // The effective policy preserves the namespace's tableNaming for inspection.
        assertThat(result.get().effectivePolicy().tableNaming()).contains(namespaceNaming);
    }

    @Test
    void namespaceEnabledIsIgnored() {
        // Per spec: namespace policy is always "on" if set; namespace.enabled=false
        // does NOT prevent materialization. Only stream.enabled=false short-circuits.
        TableMaterializationPolicy ns = new TableMaterializationPolicy(
                Optional.of("iceberg-glue"),
                Optional.of(new TableNaming(Optional.of("warehouse"), "${stream.name}")),
                Optional.empty(),
                Optional.of(false),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(ns), Optional.empty(), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isPresent();
        assertThat(result.get().tableIdentifier())
                .isEqualTo(new TableIdentifier("warehouse", "orders"));
    }

    // --- helpers ---------------------------------------------------------

    /**
     * Returns a namespace policy with catalogRef=iceberg-glue, tableNaming
     * template "${stream.name}" with prefix "warehouse", framework with
     * writeMode=APPEND, and {@link EvolutionPolicy#forIceberg()}.
     */
    private static TableMaterializationPolicy namespaceFullyPopulated() {
        return new TableMaterializationPolicy(
                Optional.of("iceberg-glue"),
                Optional.of(new TableNaming(Optional.of("warehouse"), "${stream.name}")),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new FrameworkConf(
                        Optional.of(WriteMode.APPEND),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                Optional.of(EvolutionPolicy.forIceberg()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());
    }

    private static TableMaterializationPolicy streamWithOnly(
            Function<StreamPolicyBuilder, StreamPolicyBuilder> tweak) {
        return tweak.apply(new StreamPolicyBuilder()).build();
    }

    private static TableMaterializationPolicy withPrimaryKey(
            TableMaterializationPolicy base, List<String> primaryKey) {
        return new TableMaterializationPolicy(
                base.catalogRef(),
                base.tableNaming(),
                base.tableIdentifier(),
                base.enabled(),
                base.framework(),
                base.evolution(),
                Optional.of(primaryKey),
                base.baseSchemaVersion(),
                base.table(),
                base.connectionOverrides());
    }

    private static TableMaterializationPolicy withTable(
            TableMaterializationPolicy base, TableConf table) {
        return new TableMaterializationPolicy(
                base.catalogRef(),
                base.tableNaming(),
                base.tableIdentifier(),
                base.enabled(),
                base.framework(),
                base.evolution(),
                base.primaryKey(),
                base.baseSchemaVersion(),
                Optional.of(table),
                base.connectionOverrides());
    }

    private static TableMaterializationPolicy withConnectionOverrides(
            TableMaterializationPolicy base, Map<String, String> overrides) {
        return new TableMaterializationPolicy(
                base.catalogRef(),
                base.tableNaming(),
                base.tableIdentifier(),
                base.enabled(),
                base.framework(),
                base.evolution(),
                base.primaryKey(),
                base.baseSchemaVersion(),
                base.table(),
                overrides);
    }

    /** Tiny builder for sparse stream-layer policies used inside the tests. */
    private static final class StreamPolicyBuilder {
        private Optional<String> catalogRef = Optional.empty();
        private Optional<TableNaming> tableNaming = Optional.empty();
        private Optional<TableIdentifier> tableIdentifier = Optional.empty();
        private Optional<Boolean> enabled = Optional.empty();
        private Optional<FrameworkConf> framework = Optional.empty();
        private Optional<EvolutionPolicy> evolution = Optional.empty();
        private Optional<List<String>> primaryKey = Optional.empty();
        private Optional<Long> baseSchemaVersion = Optional.empty();
        private Optional<TableConf> table = Optional.empty();

        StreamPolicyBuilder catalogRef(Optional<String> v) {
            this.catalogRef = v;
            return this;
        }

        StreamPolicyBuilder tableNaming(Optional<TableNaming> v) {
            this.tableNaming = v;
            return this;
        }

        StreamPolicyBuilder tableIdentifier(Optional<TableIdentifier> v) {
            this.tableIdentifier = v;
            return this;
        }

        StreamPolicyBuilder enabled(Optional<Boolean> v) {
            this.enabled = v;
            return this;
        }

        StreamPolicyBuilder framework(Optional<FrameworkConf> v) {
            this.framework = v;
            return this;
        }

        StreamPolicyBuilder evolution(Optional<EvolutionPolicy> v) {
            this.evolution = v;
            return this;
        }

        StreamPolicyBuilder primaryKey(Optional<List<String>> v) {
            this.primaryKey = v;
            return this;
        }

        StreamPolicyBuilder baseSchemaVersion(Optional<Long> v) {
            this.baseSchemaVersion = v;
            return this;
        }

        StreamPolicyBuilder table(Optional<TableConf> v) {
            this.table = v;
            return this;
        }

        TableMaterializationPolicy build() {
            return new TableMaterializationPolicy(
                    catalogRef,
                    tableNaming,
                    tableIdentifier,
                    enabled,
                    framework,
                    evolution,
                    primaryKey,
                    baseSchemaVersion,
                    table,
                    Map.of());
        }
    }
}
