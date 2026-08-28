/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.materialization;

import static org.assertj.core.api.Assertions.assertThat;

import io.lakestream.api.StreamIdentifier;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * Cross-layer tests for {@link TableMaterializationPolicy#resolve} that exercise
 * the cluster-default &rarr; namespace &rarr; stream three-layer precedence
 * model. The shipped {@code resolve(...)} signature only takes two layers
 * (namespace + stream); the cluster layer is therefore modelled by manually
 * pre-merging a cluster baseline into the namespace argument before calling
 * {@code resolve(...)} (the same shape an operator would use to seed cluster
 * defaults at boot time).
 *
 * <p>These cases complement {@link TableMaterializationPolicyResolveTest} —
 * they do not duplicate it. The focus here is on the three-layer scalar
 * precedence, the connection-overrides merge that lives outside
 * {@code resolve(...)}, and a representative deep {@code evolution} merge that
 * mixes per-field inheritance with sparse per-field override.
 */
class TableMaterializationPolicyCrossLayerTest {

    private static final StreamIdentifier STREAM =
            StreamIdentifier.of("public/default", "orders");

    private static final TableCatalog ICEBERG_CATALOG = new TableCatalog(
            "iceberg-glue",
            TableCatalogType.ICEBERG,
            Map.of("dsn", "X", "user", "ursa"),
            Map.of());

    private static Function<String, Optional<TableCatalog>> lookup(TableCatalog... catalogs) {
        Map<String, TableCatalog> byName = new LinkedHashMap<>();
        for (TableCatalog c : catalogs) {
            byName.put(c.name(), c);
        }
        return name -> Optional.ofNullable(byName.get(name));
    }

    @Test
    void threeLayerScalarPrecedence_clusterBaselineNamespaceOverridesStreamOverrides() {
        // Cluster default: catalogRef + tableNaming + evolution permissive Iceberg defaults +
        // a baseSchemaVersion that nobody overrides — should survive to the resolved policy.
        TableMaterializationPolicy cluster = new TableMaterializationPolicy(
                Optional.of("iceberg-glue"),
                Optional.of(new TableNaming(Optional.of("cluster_prefix"), "${stream.name}")),
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
                Optional.of(7L),
                Optional.empty(),
                Map.of());

        // Namespace overrides tableNaming (different prefix) and inherits everything else
        // from the cluster baseline.
        TableMaterializationPolicy namespace = mergeAsHigherLayer(
                cluster,
                Optional.of("iceberg-glue"),
                Optional.of(new TableNaming(Optional.of("warehouse"), "${stream.name}")),
                Optional.<TableIdentifier>empty(),
                Optional.<Boolean>empty(),
                Optional.<FrameworkConf>empty(),
                Optional.<EvolutionPolicy>empty(),
                Optional.<java.util.List<String>>empty(),
                Optional.<Long>empty(),
                Optional.<TableConf>empty(),
                Map.<String, String>of());

        // Stream overrides writeMode → UPSERT only; namespace tableNaming and cluster
        // baseSchemaVersion should both survive.
        FrameworkConf streamFramework = new FrameworkConf(
                Optional.of(WriteMode.UPSERT),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        TableMaterializationPolicy stream = new TableMaterializationPolicy(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(streamFramework),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(namespace), Optional.of(stream), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isPresent();
        ResolvedMaterialization rm = result.get();

        // Lowest-set layer wins per field:
        assertThat(rm.tableIdentifier())
                .as("namespace's tableNaming with prefix=\"warehouse\" should win over cluster's")
                .isEqualTo(new TableIdentifier("warehouse", "orders"));
        assertThat(rm.effectivePolicy().baseSchemaVersion())
                .as("baseSchemaVersion set only on cluster baseline should survive")
                .contains(7L);
        assertThat(rm.effectivePolicy().framework().orElseThrow().writeMode())
                .as("writeMode from stream wins over cluster's APPEND")
                .contains(WriteMode.UPSERT);
        assertThat(rm.effectivePolicy().evolution())
                .as("evolution inherited from cluster baseline (Iceberg permissive defaults)")
                .contains(EvolutionPolicy.forIceberg());
    }

    @Test
    void complexTemplateInterpolatesBothVariables() {
        // Namespace uses a complex template that references BOTH ${stream.namespace} and
        // ${stream.name} with a literal suffix; stream sets nothing — verify the resolver
        // produces the fully interpolated TableIdentifier end-to-end.
        TableNaming naming = new TableNaming(
                Optional.empty(), "${stream.namespace}__${stream.name}_events");
        TableMaterializationPolicy namespace = new TableMaterializationPolicy(
                Optional.of("iceberg-glue"),
                Optional.of(naming),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(namespace),
                Optional.of(TableMaterializationPolicy.empty()),
                STREAM,
                lookup(ICEBERG_CATALOG));

        assertThat(result).isPresent();
        TableIdentifier id = result.get().tableIdentifier();
        // Default namespace fallback: when no prefix is set, the table namespace is taken
        // from the stream's namespace verbatim.
        assertThat(id.namespace()).isEqualTo("public/default");
        // Template uses both vars + a literal "__" separator + a literal "_events" suffix.
        assertThat(id.name()).isEqualTo("public/default__orders_events");
    }

    @Test
    void streamTableIdentifierShadowsNamespaceTableNaming() {
        // Namespace tableNaming would derive ("events", "foo"); stream pins
        // ("custom_db", "custom_name") — verify the explicit identifier wins.
        TableNaming namingThatWouldProduceEventsFoo = new TableNaming(
                Optional.of("events"), "foo");
        TableMaterializationPolicy namespace = new TableMaterializationPolicy(
                Optional.of("iceberg-glue"),
                Optional.of(namingThatWouldProduceEventsFoo),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());

        TableIdentifier streamExplicit = new TableIdentifier("custom_db", "custom_name");
        TableMaterializationPolicy stream = new TableMaterializationPolicy(
                Optional.empty(),
                Optional.empty(),
                Optional.of(streamExplicit),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(namespace), Optional.of(stream), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isPresent();
        assertThat(result.get().tableIdentifier()).isEqualTo(streamExplicit);
        // Sanity: namespace.tableNaming is preserved on the effective policy for inspection
        // even though it was shadowed for identifier derivation.
        assertThat(result.get().effectivePolicy().tableNaming())
                .contains(namingThatWouldProduceEventsFoo);
    }

    @Test
    void streamConnectionOverridesMergedAtopCatalogConnection() {
        // The resolver itself doesn't merge connection settings — TableCatalog.connection
        // is the baseline and TableMaterializationPolicy.connectionOverrides is layered on
        // top by materializer construction. We model that merge here as a small helper and
        // verify the end shape: namespace's overrides are dropped (resolver ignores them),
        // stream's overrides shadow the corresponding catalog keys, and unspecified keys
        // inherit from the catalog.
        TableMaterializationPolicy namespace = new TableMaterializationPolicy(
                Optional.of("iceberg-glue"),
                Optional.of(new TableNaming(Optional.of("warehouse"), "${stream.name}")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                // Namespace's connectionOverrides are intentionally dropped by the resolver.
                Map.of("user", "namespace_user_should_be_ignored", "extra_ns_key", "ignored"));

        TableMaterializationPolicy stream = new TableMaterializationPolicy(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of("user", "admin"));

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(namespace), Optional.of(stream), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isPresent();
        ResolvedMaterialization rm = result.get();

        // Resolver-side: only stream's connectionOverrides survive on the effective policy.
        assertThat(rm.effectivePolicy().connectionOverrides())
                .isEqualTo(Map.of("user", "admin"));

        // Materializer-side merge: TableCatalog.connection + effectivePolicy.connectionOverrides.
        // This is what the sink factories actually see at construction time.
        Map<String, String> merged = mergeConnection(
                rm.catalog().connection(), rm.effectivePolicy().connectionOverrides());
        assertThat(merged).containsEntry("dsn", "X");      // inherited from catalog
        assertThat(merged).containsEntry("user", "admin"); // stream override
        assertThat(merged).doesNotContainKey("extra_ns_key"); // namespace's are dropped
        assertThat(merged).hasSize(2);
    }

    @Test
    void evolutionDeepMergeNamespacePermissiveBaselineWithStreamSparseOverride() {
        // Namespace sets the permissive Iceberg defaults (addColumn/addNullableColumn/
        // widenType=true, everything else false). Stream policy sets only dropColumn=true.
        // Resolved policy: all permissive flags from namespace + dropColumn=true.
        TableMaterializationPolicy namespace = new TableMaterializationPolicy(
                Optional.of("iceberg-glue"),
                Optional.of(new TableNaming(Optional.of("warehouse"), "${stream.name}")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(EvolutionPolicy.forIceberg()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());

        EvolutionPolicy sparseOverride = new EvolutionPolicy(
                Optional.empty(),
                Optional.empty(),
                Optional.of(true),  // only dropColumn flipped
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        TableMaterializationPolicy stream = new TableMaterializationPolicy(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(sparseOverride),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());

        Optional<ResolvedMaterialization> result = TableMaterializationPolicy.resolve(
                Optional.of(namespace), Optional.of(stream), STREAM, lookup(ICEBERG_CATALOG));

        assertThat(result).isPresent();
        EvolutionPolicy effective = result.get().effectivePolicy().evolution().orElseThrow();

        // Inherited from namespace (Iceberg permissive defaults).
        assertThat(effective.addColumn()).contains(true);
        assertThat(effective.addNullableColumn()).contains(true);
        assertThat(effective.widenType()).contains(true);
        assertThat(effective.narrowType()).contains(false);
        assertThat(effective.renameColumn()).contains(false);
        assertThat(effective.reorderColumns()).contains(false);
        assertThat(effective.nullabilityRelax()).contains(false);
        assertThat(effective.nullabilityTighten()).contains(false);
        // Overridden by the stream layer.
        assertThat(effective.dropColumn()).contains(true);
    }

    // --- helpers ---------------------------------------------------------

    /**
     * Models a higher policy layer (e.g., namespace built atop cluster defaults) by
     * deep-merging the {@code higher} fields onto the {@code lower} baseline using the
     * same per-field "higher wins iff present" semantics the resolver itself uses.
     *
     * <p>This is intentionally a small re-implementation in test code: it lets the test
     * exercise the resolver's two-layer signature while modelling a three-layer
     * cluster &rarr; namespace &rarr; stream stack from the outside.
     */
    @SuppressWarnings({"checkstyle:ParameterNumber", "MethodLength"})
    private static TableMaterializationPolicy mergeAsHigherLayer(
            TableMaterializationPolicy lower,
            Optional<String> catalogRef,
            Optional<TableNaming> tableNaming,
            Optional<TableIdentifier> tableIdentifier,
            Optional<Boolean> enabled,
            Optional<FrameworkConf> framework,
            Optional<EvolutionPolicy> evolution,
            Optional<java.util.List<String>> primaryKey,
            Optional<Long> baseSchemaVersion,
            Optional<TableConf> table,
            Map<String, String> connectionOverrides) {
        return new TableMaterializationPolicy(
                catalogRef.isPresent() ? catalogRef : lower.catalogRef(),
                tableNaming.isPresent() ? tableNaming : lower.tableNaming(),
                tableIdentifier.isPresent() ? tableIdentifier : lower.tableIdentifier(),
                enabled.isPresent() ? enabled : lower.enabled(),
                framework.isPresent() ? framework : lower.framework(),
                evolution.isPresent() ? evolution : lower.evolution(),
                primaryKey.isPresent() ? primaryKey : lower.primaryKey(),
                baseSchemaVersion.isPresent() ? baseSchemaVersion : lower.baseSchemaVersion(),
                table.isPresent() ? table : lower.table(),
                connectionOverrides.isEmpty() ? lower.connectionOverrides() : connectionOverrides);
    }

    /**
     * Models the materializer-construction-time merge of catalog connection settings with
     * the per-stream {@code connectionOverrides}. Defined here (not in production code) to
     * pin the contract the sink factories rely on without binding to any specific factory
     * implementation. The merge is "stream wins per key; otherwise inherit from catalog".
     */
    private static Map<String, String> mergeConnection(
            Map<String, String> catalogConnection, Map<String, String> streamOverrides) {
        Map<String, String> out = new HashMap<>(catalogConnection);
        out.putAll(streamOverrides);
        return out;
    }
}
