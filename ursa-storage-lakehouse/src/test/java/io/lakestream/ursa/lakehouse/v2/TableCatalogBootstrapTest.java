/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.Namespace;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.CommitConfig;
import io.lakestream.api.materialization.FrameworkConf;
import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.api.materialization.TableMode;
import io.lakestream.api.materialization.WriteMode;
import io.lakestream.ursa.lakehouse.v2.TableCatalogBootstrap.BootstrapResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@Tag("lakehouse")
class TableCatalogBootstrapTest {

    @Mock
    private StreamCatalog streamCatalog;

    private final List<TableCatalog> recorded = new ArrayList<>();
    private final Map<String, TableCatalog> registry = new HashMap<>();
    private final Map<String, TableMaterializationPolicy> namespacePolicies = new HashMap<>();
    private final java.util.concurrent.atomic.AtomicReference<TableMaterializationPolicy> clusterDefaultPolicy =
            new java.util.concurrent.atomic.AtomicReference<>();

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        recorded.clear();
        registry.clear();
        namespacePolicies.clear();
        clusterDefaultPolicy.set(null);
        // Cluster-wide default path (no materializationDefaultNamespace): record the policy so the
        // assertions below can inspect it.
        when(streamCatalog.setClusterDefaultMaterialization(any())).thenAnswer(invocation -> {
            clusterDefaultPolicy.set(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        });
        // Mirror IndexedStreamCatalog's behaviour: registerTableCatalog overwrites silently.
        when(streamCatalog.registerTableCatalog(any())).thenAnswer(invocation -> {
            TableCatalog catalog = invocation.getArgument(0);
            recorded.add(catalog);
            registry.put(catalog.name(), catalog);
            return CompletableFuture.completedFuture(null);
        });
        // The bridge creates the namespace carrying the policy (ordering-safe against lazy namespace
        // creation); record the attached policy so the assertions below can inspect it.
        when(streamCatalog.createNamespace(any())).thenAnswer(invocation -> {
            Namespace ns = invocation.getArgument(0);
            ns.materialization().ifPresent(policy -> namespacePolicies.put(ns.name(), policy));
            return CompletableFuture.completedFuture(null);
        });
        // Fallback path (namespace already exists) updates the policy directly.
        when(streamCatalog.setNamespaceMaterialization(any(), any())).thenAnswer(invocation -> {
            namespacePolicies.put(invocation.getArgument(0), invocation.getArgument(1));
            return CompletableFuture.completedFuture(null);
        });
    }

    @Test
    void parsesIcebergCatalogProperties() {
        Properties props = new Properties();
        props.setProperty("iceberg.catalog.foo.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
        props.setProperty("iceberg.catalog.foo.warehouse", "s3://bucket/warehouse/");

        BootstrapResult result = TableCatalogBootstrap.bootstrap(streamCatalog, props);

        assertThat(result.registered()).containsExactly("foo");
        assertThat(result.skipped()).isEmpty();
        assertThat(result.errors()).isEmpty();

        TableCatalog catalog = registry.get("foo");
        assertThat(catalog).isNotNull();
        assertThat(catalog.name()).isEqualTo("foo");
        assertThat(catalog.type()).isEqualTo(TableCatalogType.ICEBERG);
        assertThat(catalog.connection())
                .containsEntry("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
                .containsEntry("warehouse", "s3://bucket/warehouse/");
        assertThat(catalog.properties()).isEmpty();
    }

    @Test
    void parsesDeltaCatalogProperties() {
        Properties props = new Properties();
        props.setProperty("delta.catalog.bar.warehouse", "s3://bucket/delta/");
        props.setProperty("delta.catalog.bar.region", "us-east-1");

        BootstrapResult result = TableCatalogBootstrap.bootstrap(streamCatalog, props);

        assertThat(result.registered()).containsExactly("bar");
        assertThat(result.errors()).isEmpty();

        TableCatalog catalog = registry.get("bar");
        assertThat(catalog.type()).isEqualTo(TableCatalogType.DELTA);
        assertThat(catalog.connection())
                .containsEntry("warehouse", "s3://bucket/delta/")
                .containsEntry("region", "us-east-1");
    }

    @Test
    void parsesDeltaUCViaUnityKeyHeuristic() {
        Properties props = new Properties();
        props.setProperty("delta.catalog.foo.unity-catalog-uri", "https://uc.example.com");
        props.setProperty("delta.catalog.foo.token", "secret");

        BootstrapResult result = TableCatalogBootstrap.bootstrap(streamCatalog, props);

        assertThat(result.registered()).containsExactly("foo");
        TableCatalog catalog = registry.get("foo");
        assertThat(catalog.type()).isEqualTo(TableCatalogType.DELTA_UC);
        assertThat(catalog.connection())
                .containsEntry("unity-catalog-uri", "https://uc.example.com")
                .containsEntry("token", "secret");
    }

    @Test
    void parsesDeltaUCViaUnityName() {
        Properties props = new Properties();
        props.setProperty("delta.catalog.unity.endpoint", "https://uc.example.com");

        BootstrapResult result = TableCatalogBootstrap.bootstrap(streamCatalog, props);

        assertThat(result.registered()).containsExactly("unity");
        assertThat(registry.get("unity").type()).isEqualTo(TableCatalogType.DELTA_UC);
    }

    @Test
    void parsesFlatUnityCatalogFamily() {
        Properties props = new Properties();
        props.setProperty("unityCatalogUri", "https://uc.example.com");
        props.setProperty("unityCatalogToken", "secret");
        props.setProperty("unityCatalogName", "main");
        props.setProperty("unityCatalogTokenFile", "/etc/uc.token");

        BootstrapResult result = TableCatalogBootstrap.bootstrap(streamCatalog, props);

        assertThat(result.registered()).containsExactly("unity");
        TableCatalog catalog = registry.get("unity");
        assertThat(catalog.type()).isEqualTo(TableCatalogType.DELTA_UC);
        assertThat(catalog.connection())
                .containsEntry("unity-catalog-uri", "https://uc.example.com")
                .containsEntry("unity-catalog-token", "secret")
                .containsEntry("unity-catalog-name", "main")
                .containsEntry("unity-catalog-token-file", "/etc/uc.token");
    }

    @Test
    void parsesClickHouseCatalogProperties() {
        Properties props = new Properties();
        props.setProperty("clickhouse.catalog.ch.url", "jdbc:clickhouse://ch.example.com:8443/");
        props.setProperty("clickhouse.catalog.ch.username", "default");

        BootstrapResult result = TableCatalogBootstrap.bootstrap(streamCatalog, props);

        assertThat(result.registered()).containsExactly("ch");
        TableCatalog catalog = registry.get("ch");
        assertThat(catalog.type()).isEqualTo(TableCatalogType.CLICKHOUSE);
        assertThat(catalog.connection())
                .containsEntry("url", "jdbc:clickhouse://ch.example.com:8443/")
                .containsEntry("username", "default");
    }

    @Test
    void skipsBigQueryWithoutEnumCase() {
        Properties props = new Properties();
        props.setProperty("bigquery.catalog.bq.project", "my-project");
        props.setProperty("bigquery.catalog.bq.dataset", "my_dataset");

        BootstrapResult result = TableCatalogBootstrap.bootstrap(streamCatalog, props);

        assertThat(result.registered()).isEmpty();
        assertThat(result.errors()).isEmpty();
        assertThat(result.skipped()).contains("bigquery.catalog.bq");
        assertThat(registry).doesNotContainKey("bq");
    }

    @Test
    void idempotentOnRepeatCall() {
        Properties props = new Properties();
        props.setProperty("iceberg.catalog.foo.warehouse", "s3://bucket/");

        BootstrapResult first = TableCatalogBootstrap.bootstrap(streamCatalog, props);
        BootstrapResult second = TableCatalogBootstrap.bootstrap(streamCatalog, props);

        assertThat(first.registered()).containsExactly("foo");
        assertThat(second.registered()).containsExactly("foo");
        // Same catalog persisted once; second call's AlreadyExistsException is swallowed.
        assertThat(registry).hasSize(1);
        assertThat(registry.get("foo").type()).isEqualTo(TableCatalogType.ICEBERG);
        assertThat(second.errors()).isEmpty();
        // Both calls invoke registerTableCatalog (the idempotency happens server-side).
        ArgumentCaptor<TableCatalog> captor = ArgumentCaptor.forClass(TableCatalog.class);
        verify(streamCatalog, atLeastOnce()).registerTableCatalog(captor.capture());
        assertThat(captor.getAllValues()).hasSize(2);
        assertThat(captor.getAllValues()).allMatch(c -> "foo".equals(c.name()));
    }

    @Test
    void ignoresUnrecognisedPrefix() {
        Properties props = new Properties();
        props.setProperty("random.key", "value");
        props.setProperty("totally.unrelated.setting", "true");

        BootstrapResult result = TableCatalogBootstrap.bootstrap(streamCatalog, props);

        assertThat(result.registered()).isEmpty();
        assertThat(result.skipped()).isEmpty();
        assertThat(result.errors()).isEmpty();
        assertThat(registry).isEmpty();
    }

    @Test
    void skipsPrefixesWithNoSubkeys() {
        // delta.catalog.foo with no further dot-suffix → no usable subkeys → skipped (logged WARN).
        Properties props = new Properties();
        props.setProperty("delta.catalog.foo", "ignored");

        BootstrapResult result = TableCatalogBootstrap.bootstrap(streamCatalog, props);

        assertThat(result.registered()).isEmpty();
        assertThat(result.skipped()).contains("delta.catalog.foo");
    }

    @Test
    void handlesRegistrationFailureGracefully() {
        // Override the mock to fail with a non-AlreadyExists exception.
        // Use doAnswer to avoid re-invoking the existing stub during reconfiguration.
        doAnswer(invocation -> {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(new RuntimeException("oxia unreachable"));
            return failed;
        }).when(streamCatalog).registerTableCatalog(any());

        Properties props = new Properties();
        props.setProperty("iceberg.catalog.foo.warehouse", "s3://x/");

        BootstrapResult result = TableCatalogBootstrap.bootstrap(streamCatalog, props);

        assertThat(result.registered()).isEmpty();
        assertThat(result.errors()).hasSize(1);
        assertThat(result.errors().get(0)).contains("iceberg.catalog.foo").contains("oxia unreachable");
        assertThat(result.hasErrors()).isTrue();
    }

    @Test
    void camelToKebabHelper() {
        assertThat(TableCatalogBootstrap.camelToKebab("Uri")).isEqualTo("uri");
        assertThat(TableCatalogBootstrap.camelToKebab("TokenFile")).isEqualTo("token-file");
        assertThat(TableCatalogBootstrap.camelToKebab("ClientId")).isEqualTo("client-id");
        assertThat(TableCatalogBootstrap.camelToKebab("byolSystemType")).isEqualTo("byol-system-type");
        assertThat(TableCatalogBootstrap.camelToKebab("")).isEqualTo("");
    }

    @Test
    void defaultPolicyBridgeSynthesizesCatalogAndNamespacePolicy() {
        // materializationEnabled + a default namespace + legacy lakehouse config → the bridge
        // registers a synthesized catalog and attaches an EXTERNAL namespace policy referencing it,
        // so the new pipeline materializes every stream in that namespace by just flipping the flag.
        Properties props = new Properties();
        props.setProperty("materializationEnabled", "true");
        props.setProperty("materializationDefaultNamespace", "public/default");
        props.setProperty("lakehouseType", "DELTA");
        props.setProperty("streamTableMode", "EXTERNAL");
        props.setProperty("cloudStorageEndpoint", "http://localstack:4566");

        TableCatalogBootstrap.bootstrap(streamCatalog, props);

        // A catalog was synthesized with the flat legacy props in properties() (so buildConfiguration
        // re-emits them top-level for the writers), control keys excluded.
        TableCatalog catalog = registry.get("default-delta");
        assertThat(catalog).isNotNull();
        assertThat(catalog.type()).isEqualTo(TableCatalogType.DELTA);
        assertThat(catalog.properties()).containsEntry("cloudStorageEndpoint", "http://localstack:4566");
        assertThat(catalog.properties()).containsEntry("lakehouseType", "DELTA");
        assertThat(catalog.properties()).doesNotContainKey("materializationEnabled");
        assertThat(catalog.properties()).doesNotContainKey("materializationDefaultNamespace");

        // The namespace policy references that catalog, is EXTERNAL, names tables by stream name.
        TableMaterializationPolicy policy = namespacePolicies.get("public/default");
        assertThat(policy).isNotNull();
        assertThat(policy.catalogRef()).contains("default-delta");
        assertThat(policy.enabled()).contains(Boolean.TRUE);
        assertThat(policy.table().flatMap(t -> t.mode())).contains(TableMode.EXTERNAL);
        assertThat(policy.tableNaming()).isPresent();
        assertThat(policy.tableNaming().get().tableNameTemplate()).isEqualTo("${stream.name}");
    }

    @Test
    void tableNameTemplateOverridesTheSynthesizedDefault() {
        Properties props = new Properties();
        props.setProperty("materializationEnabled", "true");
        props.setProperty("materializationDefaultNamespace", "public/default");
        props.setProperty("lakehouseType", "DELTA");
        props.setProperty("streamTableMode", "EXTERNAL");
        props.setProperty(TableCatalogBootstrap.TABLE_NAME_TEMPLATE_PROPERTY, "${stream.name}_v2");

        TableCatalogBootstrap.bootstrap(streamCatalog, props);

        TableMaterializationPolicy policy = namespacePolicies.get("public/default");
        assertThat(policy.tableNaming()).isPresent();
        assertThat(policy.tableNaming().get().tableNameTemplate()).isEqualTo("${stream.name}_v2");
    }

    @Test
    void tableNameTemplateRejectsStreamPropertiesForAManagedLakehouseSink() {
        // The commit runner resolves the table from the log name and cannot read stream properties, so
        // such a template would silently split the two resolutions apart. Fail at startup instead.
        Properties props = new Properties();
        props.setProperty("materializationEnabled", "true");
        props.setProperty("materializationDefaultNamespace", "public/default");
        props.setProperty("lakehouseType", "ICEBERG");
        props.setProperty("streamTableMode", "EXTERNAL");
        props.setProperty(TableCatalogBootstrap.TABLE_NAME_TEMPLATE_PROPERTY,
                "${stream.property.lakestream.kafka.topic.name}");

        assertThatThrownBy(() -> TableCatalogBootstrap.bootstrap(streamCatalog, props))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(TableCatalogBootstrap.TABLE_NAME_TEMPLATE_PROPERTY);
    }

    @Test
    void tableNameTemplateAllowsStreamPropertiesForAnInlineCommitSink() {
        Properties props = new Properties();
        props.setProperty("materializationEnabled", "true");
        props.setProperty("materializationDefaultNamespace", "public/default");
        props.setProperty("lakehouseType", "CLICKHOUSE");
        props.setProperty("streamTableMode", "EXTERNAL");
        props.setProperty("dsn", "jdbc:ch://clickhouse:8123/default");
        props.setProperty(TableCatalogBootstrap.TABLE_NAME_TEMPLATE_PROPERTY,
                "${stream.property.lakestream.kafka.topic.name}");

        TableCatalogBootstrap.bootstrap(streamCatalog, props);

        TableMaterializationPolicy policy = namespacePolicies.get("public/default");
        assertThat(policy.tableNaming().get().tableNameTemplate())
                .isEqualTo("${stream.property.lakestream.kafka.topic.name}");
    }

    @Test
    void defaultPolicyBridgeSynthesizesClickHouseCatalogIntoConnection() {
        // lakehouseType=CLICKHOUSE → the synthesized catalog must carry the JDBC connection in
        // connection() (not properties()), because ClickHouseConnectionFactory reads dsn/user/password
        // from connection(). The table-namespace prefix is pinned to the configured ClickHouse
        // database (a valid, existing DB), since the stream namespace is not a usable database name.
        Properties props = new Properties();
        props.setProperty("materializationEnabled", "true");
        props.setProperty("materializationDefaultNamespace", "public/default");
        props.setProperty("lakehouseType", "CLICKHOUSE");
        props.setProperty("streamTableMode", "EXTERNAL");
        props.setProperty("dsn", "jdbc:ch://clickhouse:8123/default");
        props.setProperty("user", "ursa");
        props.setProperty("password", "secret");
        props.setProperty("clickhouseDatabase", "analytics");

        TableCatalogBootstrap.bootstrap(streamCatalog, props);

        TableCatalog catalog = registry.get("default-clickhouse");
        assertThat(catalog).isNotNull();
        assertThat(catalog.type()).isEqualTo(TableCatalogType.CLICKHOUSE);
        // Connection keys land in connection(); properties() stays empty for ClickHouse.
        // Only genuine connection keys are routed into connection(): the client-v2 JDBC driver rejects
        // unknown properties, so the compaction config (streamTableMode, clickhouseDatabase, …) must NOT
        // leak in. dsn/user/password only.
        assertThat(catalog.connection())
                .containsOnlyKeys("dsn", "user", "password")
                .containsEntry("dsn", "jdbc:ch://clickhouse:8123/default")
                .containsEntry("user", "ursa")
                .containsEntry("password", "secret");
        assertThat(catalog.connection())
                .doesNotContainKey("materializationEnabled")
                .doesNotContainKey("streamTableMode")
                .doesNotContainKey("clickhouseDatabase")
                .doesNotContainKey("lakehouseType");
        assertThat(catalog.properties()).isEmpty();

        TableMaterializationPolicy policy = namespacePolicies.get("public/default");
        assertThat(policy).isNotNull();
        assertThat(policy.catalogRef()).contains("default-clickhouse");
        assertThat(policy.table().flatMap(t -> t.mode())).contains(TableMode.EXTERNAL);
        assertThat(policy.tableNaming()).isPresent();
        // ClickHouse uses a fixed database namespace, so the full stream namespace is encoded into
        // the table name (${stream.namespace}.${stream.name}); the ClickHouse sink folds the '/'
        // path separator to '.' at write time. Iceberg/Delta keep ${stream.name}.
        assertThat(policy.tableNaming().get().tableNameTemplate())
                .isEqualTo("${stream.namespace}.${stream.name}");
        // The ClickHouse database (table namespace) is pinned, not derived from the stream namespace.
        assertThat(policy.tableNaming().get().tableNamespacePrefix()).contains("analytics");
    }

    @Test
    void defaultPolicyBridgeNoOpWithoutFlag() {
        Properties props = new Properties();
        props.setProperty("materializationDefaultNamespace", "public/default");
        props.setProperty("lakehouseType", "DELTA");

        TableCatalogBootstrap.bootstrap(streamCatalog, props);

        assertThat(namespacePolicies).isEmpty();
        assertThat(registry).doesNotContainKey("default-delta");
    }

    @Test
    void defaultPolicyBridgeAppliesClusterWideWithoutDefaultNamespace() {
        // materializationEnabled=true with NO materializationDefaultNamespace → the synthesized policy
        // is applied cluster-wide (the lowest-priority baseline), not scoped to any namespace.
        Properties props = new Properties();
        props.setProperty("materializationEnabled", "true");
        props.setProperty("lakehouseType", "DELTA");

        TableCatalogBootstrap.bootstrap(streamCatalog, props);

        // No namespace metadata is touched...
        assertThat(namespacePolicies).isEmpty();
        // ...but the catalog is registered and the cluster-wide default policy references it.
        assertThat(registry).containsKey("default-delta");
        TableMaterializationPolicy policy = clusterDefaultPolicy.get();
        assertThat(policy).isNotNull();
        assertThat(policy.catalogRef()).contains("default-delta");
    }

    @Test
    void resolveFromPropertiesSynthesizesMaterializationFromTaskProperties() {
        // Back-compat: a stream with no policy whose compaction task carries the catalog config +
        // sdt.enabled resolves via resolveFromProperties (the task-property fallback).
        Properties props = new Properties();
        props.setProperty("sdt.enabled", "true");
        props.setProperty("lakehouseType", "ICEBERG");
        props.setProperty("iceberg.catalog.default-iceberg.warehouse", "s3://bucket/wh/");

        var resolved = TableCatalogBootstrap.resolveFromProperties(
                props, StreamIdentifier.of("public/test", "topic-a"));

        assertThat(resolved).isPresent();
        ResolvedMaterialization rm = resolved.get();
        assertThat(rm.catalog().type()).isEqualTo(TableCatalogType.ICEBERG);
        assertThat(rm.tableIdentifier().name()).isEqualTo("topic-a");
        assertThat(rm.effectivePolicy().catalogRef()).contains("default-iceberg");
    }

    @Test
    void resolveFromPropertiesPopulatesStructuredPolicyFromConfig() {
        // The synthesized policy must carry the STRUCTURED fields sinks read directly (ClickHouse reads
        // primaryKey / framework.writeMode / commit.batchSize / baseSchemaVersion, and
        // ClickHouseTableEngine.forPolicy derives the engine from writeMode + primaryKey), sourced from
        // the legacy DynamicConfigs keys — not left empty.
        Properties props = new Properties();
        props.setProperty("sdt.enabled", "true");
        props.setProperty("lakehouseType", "CLICKHOUSE");
        props.setProperty("upsert.mode.enabled", "true");
        props.setProperty("identifier.fields", "id, name");
        props.setProperty("base.schema.version", "7");
        props.setProperty("commit.batch.size", "250");

        var resolved = TableCatalogBootstrap.resolveFromProperties(
                props, StreamIdentifier.of("public/test", "topic-a"));

        assertThat(resolved).isPresent();
        TableMaterializationPolicy policy = resolved.get().effectivePolicy();
        assertThat(policy.primaryKey()).contains(List.of("id", "name"));
        assertThat(policy.framework().flatMap(FrameworkConf::writeMode)).contains(WriteMode.UPSERT);
        assertThat(policy.framework().flatMap(FrameworkConf::commit).flatMap(CommitConfig::batchSize))
                .contains(250);
        assertThat(policy.baseSchemaVersion()).contains(7L);
    }

    @Test
    void resolveFromPropertiesLeavesStructuredPolicyEmptyWhenConfigAbsent() {
        // Without the DynamicConfigs keys the structured fields stay empty so the sink applies its
        // own defaults (e.g. ClickHouse DEFAULT_BATCH_SIZE, MergeTree).
        Properties props = new Properties();
        props.setProperty("sdt.enabled", "true");
        props.setProperty("lakehouseType", "CLICKHOUSE");

        var resolved = TableCatalogBootstrap.resolveFromProperties(
                props, StreamIdentifier.of("public/test", "topic-a"));

        assertThat(resolved).isPresent();
        TableMaterializationPolicy policy = resolved.get().effectivePolicy();
        assertThat(policy.primaryKey()).isEmpty();
        assertThat(policy.framework()).isEmpty();
        assertThat(policy.baseSchemaVersion()).isEmpty();
    }

    @Test
    void resolveFromPropertiesReturnsEmptyWhenSdtDisabled() {
        Properties props = new Properties();
        props.setProperty("sdt.enabled", "false");
        props.setProperty("lakehouseType", "ICEBERG");

        assertThat(TableCatalogBootstrap.resolveFromProperties(
                props, StreamIdentifier.of("public/test", "topic-a"))).isEmpty();
    }

    @Test
    void resolveFromPropertiesSynthesizesManagedMaterializationForSbtOnly() {
        // Ursa-protocol case: SDT (external table) is off and no lakehouseType/catalog is configured, but
        // SBT compacts the WAL into topic-grouped parquet Compacted Objects. The task-property fallback
        // must resolve a managed-only NONE catalog so the internal managed writer materializes with no
        // external sink — otherwise the worker throws "No effective materialization policy".
        Properties props = new Properties();
        props.setProperty("sdt.enabled", "false");
        props.setProperty("sbt.enabled", "true");

        var resolved = TableCatalogBootstrap.resolveFromProperties(
                props, StreamIdentifier.of("public/test", "topic-a"));

        assertThat(resolved).isPresent();
        ResolvedMaterialization rm = resolved.get();
        assertThat(rm.catalog().type()).isEqualTo(TableCatalogType.NONE);
        assertThat(rm.tableIdentifier().name()).isEqualTo("topic-a");
        assertThat(rm.effectivePolicy().table().flatMap(t -> t.mode())).contains(TableMode.MANAGED);
    }

    @Test
    void resolveFromPropertiesReturnsEmptyWhenSdtOnlyButNoCatalog() {
        // SDT enabled with no lakehouseType/catalog is a misconfiguration for the external sink, but SBT
        // defaults to SDT's value — so the managed writer still resolves (managed compaction is the Ursa
        // baseline). Explicitly disabling SBT leaves nothing to write to.
        Properties props = new Properties();
        props.setProperty("sdt.enabled", "true");
        props.setProperty("sbt.enabled", "false");

        assertThat(TableCatalogBootstrap.resolveFromProperties(
                props, StreamIdentifier.of("public/test", "topic-a"))).isEmpty();
    }
}
