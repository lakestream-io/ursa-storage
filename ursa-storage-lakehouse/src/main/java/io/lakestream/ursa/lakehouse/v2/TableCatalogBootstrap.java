/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import io.lakestream.api.Namespace;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.exception.AlreadyExistsException;
import io.lakestream.api.materialization.CommitConfig;
import io.lakestream.api.materialization.FrameworkConf;
import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableConf;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.api.materialization.TableMode;
import io.lakestream.api.materialization.TableNaming;
import io.lakestream.api.materialization.WriteMode;
import io.lakestream.ursa.compaction.DynamicConfigs;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.CompletionException;
import lombok.extern.slf4j.Slf4j;

/**
 * Translates legacy lakehouse configuration keys into {@link TableCatalog} records and
 * registers them via {@link StreamCatalog#registerTableCatalog(TableCatalog)}.
 *
 * <p>Recognised prefixes:
 * <ul>
 *   <li>{@code iceberg.catalog.<name>.<key>=<value>} — registers one {@link TableCatalog} of
 *       type {@link TableCatalogType#ICEBERG} per {@code <name>}.</li>
 *   <li>{@code delta.catalog.<name>.<key>=<value>} — registers one {@link TableCatalog} of
 *       type {@link TableCatalogType#DELTA} per {@code <name>}. The type is promoted to
 *       {@link TableCatalogType#DELTA_UC} when the heuristic detects Unity Catalog usage:
 *       {@code <name>} is literally {@code unity}, or any key in the group starts with
 *       {@code unity-catalog-}, {@code unity_catalog_}, or is {@code catalog-impl}.</li>
 *   <li>{@code unityCatalog<Camel>=<value>} — the flat Unity Catalog family is coalesced
 *       into a single {@link TableCatalog} named {@code unity} of type
 *       {@link TableCatalogType#DELTA_UC}. Keys are normalised by stripping the
 *       {@code unityCatalog} prefix and converting camelCase to kebab-case
 *       (e.g. {@code unityCatalogUri} → {@code unity-catalog-uri}).</li>
 *   <li>{@code clickhouse.catalog.<name>.<key>=<value>} — registers one
 *       {@link TableCatalog} of type {@link TableCatalogType#CLICKHOUSE} per {@code <name>}.</li>
 *   <li>{@code bigquery.catalog.<name>.<key>=<value>} — skipped pending
 *       {@code TableCatalogType.BIGQUERY}; logged at INFO and recorded under
 *       {@link BootstrapResult#skipped()}.</li>
 * </ul>
 *
 * <p>For T7 everything parsed under a single catalog group is stored in
 * {@link TableCatalog#connection()}; {@link TableCatalog#properties()} stays empty.
 * Future tasks can refine the connection/properties split if needed.
 *
 * <p>The bootstrap is opt-in: callers explicitly invoke
 * {@link #bootstrap(StreamCatalog, Properties)}. Each registration is performed
 * synchronously by calling {@code .join()} on the future returned from
 * {@link StreamCatalog#registerTableCatalog(TableCatalog)}, mirroring the blocking
 * pattern used elsewhere in the lakestream layer. The operation is idempotent:
 * an existing catalog with the same name is treated as success (logged at INFO).
 */
@Slf4j
public final class TableCatalogBootstrap {

    private static final String ICEBERG_PREFIX = "iceberg.catalog.";
    private static final String DELTA_PREFIX = "delta.catalog.";
    private static final String CLICKHOUSE_PREFIX = "clickhouse.catalog.";
    private static final String BIGQUERY_PREFIX = "bigquery.catalog.";
    private static final String UNITY_FLAT_PREFIX = "unityCatalog";
    private static final String UNITY_NAME = "unity";
    /**
     * Connection keys the ClickHouse sink consumes ({@code ClickHouseConnectionFactory}). Only these are
     * placed in a synthesized ClickHouse {@code TableCatalog.connection()} — the client-v2 JDBC driver
     * rejects unknown properties, so the rest of the compaction config must not leak into it.
     */
    private static final List<String> CLICKHOUSE_CONNECTION_KEYS =
            List.of("dsn", "user", "password", "password-ref");

    private TableCatalogBootstrap() {
    }

    /**
     * Parses legacy catalog configuration from {@code properties} and registers
     * each recognised group as a {@link TableCatalog} via {@code streamCatalog}.
     *
     * @param streamCatalog target catalog (must be non-null)
     * @param properties    source configuration (must be non-null)
     * @return summary of registered, skipped, and errored catalog groups
     */
    public static BootstrapResult bootstrap(StreamCatalog streamCatalog, Properties properties) {
        Objects.requireNonNull(streamCatalog, "streamCatalog");
        Objects.requireNonNull(properties, "properties");

        // Group raw keys under prefixed catalogs first so we can detect "no useful subkeys".
        Map<String, Map<String, String>> icebergGroups = groupByPrefix(properties, ICEBERG_PREFIX);
        Map<String, Map<String, String>> deltaGroups = groupByPrefix(properties, DELTA_PREFIX);
        Map<String, Map<String, String>> clickhouseGroups = groupByPrefix(properties, CLICKHOUSE_PREFIX);
        Map<String, Map<String, String>> bigqueryGroups = groupByPrefix(properties, BIGQUERY_PREFIX);
        Map<String, String> unityFlat = collectFlatUnity(properties);

        List<String> registered = new ArrayList<>();
        List<String> skipped = new ArrayList<>();
        List<String> errors = new ArrayList<>();

        registerGroups(streamCatalog, icebergGroups, TableCatalogType.ICEBERG,
                ICEBERG_PREFIX, registered, skipped, errors,
                (name, conn) -> TableCatalogType.ICEBERG);

        registerGroups(streamCatalog, deltaGroups, TableCatalogType.DELTA,
                DELTA_PREFIX, registered, skipped, errors,
                TableCatalogBootstrap::classifyDelta);

        registerGroups(streamCatalog, clickhouseGroups, TableCatalogType.CLICKHOUSE,
                CLICKHOUSE_PREFIX, registered, skipped, errors,
                (name, conn) -> TableCatalogType.CLICKHOUSE);

        // BigQuery: skipped because TableCatalogType.BIGQUERY does not yet exist.
        for (Map.Entry<String, Map<String, String>> entry : bigqueryGroups.entrySet()) {
            String name = entry.getKey();
            log.info("bigquery.catalog.{} bootstrap skipped — TableCatalogType.BIGQUERY not yet defined", name);
            skipped.add(BIGQUERY_PREFIX + name);
        }

        // Flat unityCatalog* family is coalesced into a single DELTA_UC catalog named "unity".
        if (!unityFlat.isEmpty()) {
            registerOne(streamCatalog, UNITY_NAME, TableCatalogType.DELTA_UC, unityFlat,
                    "unityCatalog*", registered, errors);
        }

        // Legacy-config → default materialization policy bridge: lets an existing deployment switch
        // to the new pipeline by only enabling the flag, without authoring a new-API policy.
        bootstrapDefaultMaterialization(streamCatalog, properties, registered, errors);

        return new BootstrapResult(registered, skipped, errors);
    }

    /**
     * When {@code materializationEnabled=true} and a {@code materializationDefaultNamespace} is set,
     * synthesizes a default {@link TableCatalog} from the flat legacy lakehouse config and attaches a
     * default {@link TableMaterializationPolicy} (EXTERNAL, referencing that catalog, table name =
     * {@code ${stream.name}}) to that namespace. This makes catalog-side materialization resolution
     * succeed so the new {@code maybeMaterialize} pipeline materializes every stream in the namespace
     * — the same coverage the legacy global-config pipeline had — without per-stream policy authoring.
     *
     * <p>No-op when the flag is off or no default namespace is configured (then streams must carry
     * explicit policies). The flat lakehouse props are placed in the catalog's {@code properties()} so
     * {@code LakehouseWriterFactory.buildConfiguration} re-emits them as the top-level keys the
     * writers read.
     */
    static void bootstrapDefaultMaterialization(StreamCatalog streamCatalog, Properties properties,
                                                List<String> registered, List<String> errors) {
        if (!Boolean.parseBoolean(properties.getProperty("materializationEnabled", "false"))) {
            return;
        }
        // materializationDefaultNamespace scopes the synthesized default policy to a single namespace.
        // When it is absent/blank, apply the policy CLUSTER-WIDE: every stream in every namespace
        // materializes (the lowest-priority baseline in catalog-side materialization resolution).
        String namespace = properties.getProperty("materializationDefaultNamespace");
        boolean clusterWide = namespace == null || namespace.isBlank();
        Optional<CatalogAndPolicy> synthesized = buildCatalogAndPolicy(properties);
        if (synthesized.isEmpty()) {
            log.info("default-policy bridge: unsupported lakehouseType {}; skipping",
                    properties.getProperty("lakehouseType", "NONE"));
            return;
        }
        TableCatalog catalog = synthesized.get().catalog();
        String catalogName = catalog.name();
        TableCatalogType catalogType = catalog.type();
        TableMaterializationPolicy policy = synthesized.get().policy();
        TableMode mode = policy.table().flatMap(TableConf::mode).orElse(TableMode.EXTERNAL);
        try {
            streamCatalog.registerTableCatalog(catalog).join();
            registered.add(catalogName);
        } catch (CompletionException ce) {
            if (!(ce.getCause() instanceof AlreadyExistsException)) {
                log.warn("default-policy bridge: failed to register catalog {}", catalogName, ce);
                errors.add(catalogName);
                return;
            }
        }
        // Ensure the namespace metadata exists before attaching the policy. At compaction bootstrap the
        // default namespace may not have been created yet (it is created lazily on first stream), and a
        // namespace-scoped policy can neither be persisted nor resolved without it. Create it carrying
        // the policy; if it already exists, update its materialization instead. Either ordering with the
        // broker's own namespace creation is safe — createNamespace fails-if-exists and we recover.
        if (clusterWide) {
            // Cluster-wide: register the policy as the catalog's lowest-priority default. No namespace
            // metadata is touched; catalog-side resolution falls back to it for any namespace.
            try {
                streamCatalog.setClusterDefaultMaterialization(policy).join();
                log.info("default-policy bridge: cluster-wide default → catalog {} ({}), mode {}",
                        catalogName, catalogType, mode);
            } catch (RuntimeException e) {
                log.warn("default-policy bridge: failed to set cluster-wide default materialization", e);
                errors.add("cluster-default");
            }
            return;
        }
        try {
            streamCatalog.createNamespace(new Namespace(namespace, Map.of(), Optional.of(policy))).join();
            log.info("default-policy bridge: namespace {} → catalog {} ({}), mode {}",
                    namespace, catalogName, catalogType, mode);
        } catch (CompletionException ce) {
            if (ce.getCause() instanceof AlreadyExistsException) {
                try {
                    streamCatalog.setNamespaceMaterialization(namespace, policy).join();
                    log.info("default-policy bridge: namespace {} → catalog {} ({}), mode {} (updated existing)",
                            namespace, catalogName, catalogType, mode);
                } catch (RuntimeException e) {
                    log.warn("default-policy bridge: failed to set namespace materialization for {}",
                            namespace, e);
                    errors.add(namespace);
                }
            } else {
                log.warn("default-policy bridge: failed to create namespace {}", namespace, ce);
                errors.add(namespace);
            }
        }
    }

    /** A synthesized ({@link TableCatalog}, {@link TableMaterializationPolicy}) pair from flat config. */
    record CatalogAndPolicy(TableCatalog catalog, TableMaterializationPolicy policy) {
    }

    /**
     * Synthesizes a ({@link TableCatalog}, {@link TableMaterializationPolicy}) from the flat legacy
     * lakehouse config, or {@link Optional#empty()} when {@code lakehouseType} is unset/unsupported.
     * Shared by the startup default-policy bridge (which registers the catalog and scopes the policy to
     * a namespace / cluster) and the per-task compatibility resolution
     * ({@link #resolveFromProperties}). The policy is EXTERNAL/MANAGED per {@code streamTableMode},
     * enabled, with table naming {@code ${stream.name}} (namespace prefix = the ClickHouse database for
     * CLICKHOUSE, else derived from the stream).
     */
    static Optional<CatalogAndPolicy> buildCatalogAndPolicy(Properties properties) {
        String lakehouseType = properties.getProperty("lakehouseType", "NONE").toUpperCase(Locale.ROOT);
        TableCatalogType catalogType;
        switch (lakehouseType) {
            case "ICEBERG" -> catalogType = TableCatalogType.ICEBERG;
            case "DELTA" -> catalogType = TableCatalogType.DELTA;
            case "CLICKHOUSE" -> catalogType = TableCatalogType.CLICKHOUSE;
            default -> {
                return Optional.empty();
            }
        }
        String catalogName = properties.getProperty("catalog.name",
                "default-" + catalogType.name().toLowerCase(Locale.ROOT));
        Map<String, String> catalogProps = new LinkedHashMap<>();
        for (String key : properties.stringPropertyNames()) {
            if (key.equals("materializationEnabled") || key.equals("materializationDefaultNamespace")) {
                continue;
            }
            catalogProps.put(key, properties.getProperty(key));
        }
        // ClickHouse reads its connection (dsn/user/password) from TableCatalog.connection(), whereas
        // the lakehouse writers read flat keys from TableCatalog.properties(). Route the synthesized
        // props to the map the sink actually consults so the flip-the-flag bridge works for either.
        // For ClickHouse the connection map must contain ONLY genuine connection settings: the
        // client-v2 JDBC driver rejects unknown properties (ClientMisconfigurationException), so dumping
        // the whole compaction property bag (lakehouseType, metadataStoreUrl, storagePath, …) into
        // it fails the connect. Driver-specific options belong in the dsn URL query string. The Iceberg/
        // Delta writers, by contrast, tolerate extra keys and read what they need from properties().
        boolean clickhouse = catalogType == TableCatalogType.CLICKHOUSE;
        Map<String, String> connection;
        Map<String, String> catalogProperties;
        if (clickhouse) {
            connection = new LinkedHashMap<>();
            for (String key : CLICKHOUSE_CONNECTION_KEYS) {
                String value = properties.getProperty(key);
                if (value != null) {
                    connection.put(key, value);
                }
            }
            catalogProperties = Map.of();
        } else {
            connection = Map.of();
            catalogProperties = catalogProps;
        }
        TableCatalog catalog = new TableCatalog(catalogName, catalogType, connection, catalogProperties);

        TableMode mode = "MANAGED".equalsIgnoreCase(properties.getProperty("streamTableMode", "EXTERNAL"))
                ? TableMode.MANAGED : TableMode.EXTERNAL;
        Optional<String> tableNamespacePrefix = clickhouse
                ? Optional.of(properties.getProperty("clickhouseDatabase", "default"))
                : Optional.empty();
        // ClickHouse is a 2-level store (database.table) with no separate namespace tier, so the
        // stream namespace would otherwise be dropped and two streams with the same local name in
        // different namespaces would collide on one table. Encode the full identity into the table
        // name: database = clickhouseDatabase (fixed), table = ${stream.namespace}.${stream.name}
        // (the ClickHouse sink turns the namespace path separator '/' into '.'). Iceberg/Delta keep
        // ${stream.name} — they carry the namespace as a real multi-level table namespace instead.
        String tableNameTemplate = clickhouse ? "${stream.namespace}.${stream.name}" : "${stream.name}";

        // Carry the legacy DynamicConfigs / flat properties into the STRUCTURED policy fields so sinks
        // that read the policy (ClickHouse reads primaryKey / framework.writeMode / commit.batchSize;
        // ClickHouseTableEngine.forPolicy derives the engine from writeMode + primaryKey) behave the
        // same as the lakehouse writers, which read the equivalent flat keys from catalog.properties().
        // Absent values stay empty so the sink applies its own default.
        DynamicConfigs dc = DynamicConfigs.fromProperties(properties);
        Optional<List<String>> primaryKey = dc.identifierFields()
                .map(TableCatalogBootstrap::splitColumns)
                .filter(cols -> !cols.isEmpty());
        Optional<WriteMode> writeMode = dc.upsertModeEnabled()
                .map(upsert -> upsert ? WriteMode.UPSERT : WriteMode.APPEND);
        Optional<CommitConfig> commit = dc.commitBatchSize()
                .map(size -> new CommitConfig(Optional.empty(), Optional.empty(), Optional.of(size)));
        Optional<FrameworkConf> framework = (writeMode.isPresent() || commit.isPresent())
                ? Optional.of(new FrameworkConf(writeMode, Optional.empty(), Optional.empty(),
                        Optional.empty(), commit))
                : Optional.empty();

        TableMaterializationPolicy policy = new TableMaterializationPolicy(
                Optional.of(catalogName),
                Optional.of(new TableNaming(tableNamespacePrefix, tableNameTemplate)),
                Optional.empty(),
                Optional.of(Boolean.TRUE),
                framework,
                Optional.empty(),
                primaryKey,
                dc.baseSchemaVersion(),
                Optional.of(new TableConf(Optional.of(mode), Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.empty(), Optional.empty())),
                Map.of());
        return Optional.of(new CatalogAndPolicy(catalog, policy));
    }

    /**
     * Splits a comma-separated column list into trimmed, non-empty names, mirroring how the lakehouse
     * writers parse {@code identifierFields} ({@code LakehouseConfiguration.getIdentifierFields}).
     */
    private static List<String> splitColumns(String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        List<String> columns = new ArrayList<>();
        for (String part : csv.split(",")) {
            String trimmed = part.strip();
            if (!trimmed.isEmpty()) {
                columns.add(trimmed);
            }
        }
        return columns;
    }

    /**
     * Resolves a {@link ResolvedMaterialization} for {@code streamId} directly from flat legacy config
     * (e.g. compaction task properties carrying the catalog config + {@code DynamicConfigs}). This is the
     * backward-compatibility path for deployments that drove materialization through task properties
     * rather than a policy: when catalog-side materialization resolution returns empty, the worker
     * falls back to this. Returns {@link Optional#empty()} when neither SDT nor SBT is enabled, or the
     * config does not describe a supported catalog.
     *
     * <p>The gate accepts SBT as well as SDT. When an external catalog is configured ({@code
     * lakehouseType} set), {@link #buildCatalogAndPolicy} synthesizes it honouring {@code
     * streamTableMode} (so an ICEBERG MANAGED deployment still resolves its managed table here). When no
     * external catalog is configured but SBT is enabled — the Ursa-protocol case where SDT is disabled
     * and the WAL is compacted into topic-grouped parquet Compacted Objects — this synthesizes a
     * managed-only resolution over a {@link TableCatalogType#NONE} catalog, so the dispatch path builds
     * only the internal managed writer with no external sink. {@code sbtEnabled()} defaults to {@code
     * sdtEnabled()}, so a deployment that configures neither still resolves nothing.
     */
    public static Optional<ResolvedMaterialization> resolveFromProperties(Properties properties,
                                                                          StreamIdentifier streamId) {
        DynamicConfigs dynamicConfigs = DynamicConfigs.fromProperties(properties);
        boolean sdtEnabled = dynamicConfigs.sdtEnabled();
        boolean sbtEnabled = dynamicConfigs.sbtEnabled();
        if (!sdtEnabled && !sbtEnabled) {
            return Optional.empty();
        }
        CatalogAndPolicy cp = buildCatalogAndPolicy(properties).orElse(null);
        if (cp == null) {
            // No external catalog (lakehouseType unset). Only SBT can proceed: materialize the managed
            // Compacted-Object writer alone via a synthetic NONE catalog. (SDT with no catalog has no
            // sink to write to, so stay empty.)
            if (!sbtEnabled) {
                return Optional.empty();
            }
            cp = managedOnlyCatalogAndPolicy();
        }
        final CatalogAndPolicy resolved = cp;
        return TableMaterializationPolicy.resolve(Optional.of(resolved.policy()), Optional.empty(), streamId,
                name -> name.equals(resolved.catalog().name())
                        ? Optional.of(resolved.catalog()) : Optional.empty());
    }

    /**
     * Builds the synthetic ({@link TableCatalog}, {@link TableMaterializationPolicy}) for the SBT-only
     * case: a {@link TableCatalogType#NONE} catalog with a {@code MANAGED}, enabled policy naming the
     * table {@code ${stream.name}}. There is no external factory for a NONE catalog; the dispatch path
     * builds only the managed Compacted-Object writer.
     */
    private static CatalogAndPolicy managedOnlyCatalogAndPolicy() {
        String catalogName = "managed-none";
        TableCatalog catalog = new TableCatalog(catalogName, TableCatalogType.NONE, Map.of(), Map.of());
        TableMaterializationPolicy policy = new TableMaterializationPolicy(
                Optional.of(catalogName),
                Optional.of(new TableNaming(Optional.empty(), "${stream.name}")),
                Optional.empty(),
                Optional.of(Boolean.TRUE),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new TableConf(Optional.of(TableMode.MANAGED), Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.empty(), Optional.empty())),
                Map.of());
        return new CatalogAndPolicy(catalog, policy);
    }

    private static void registerGroups(
            StreamCatalog streamCatalog,
            Map<String, Map<String, String>> groups,
            TableCatalogType defaultType,
            String prefix,
            List<String> registered,
            List<String> skipped,
            List<String> errors,
            DeltaTypeClassifier classifier) {
        for (Map.Entry<String, Map<String, String>> entry : groups.entrySet()) {
            String name = entry.getKey();
            Map<String, String> connection = entry.getValue();
            if (connection.isEmpty()) {
                log.warn("{}{} has no usable subkeys; skipping", prefix, name);
                skipped.add(prefix + name);
                continue;
            }
            TableCatalogType type = classifier.classify(name, connection);
            if (type != defaultType) {
                log.info("{}{} promoted to type {} via Unity Catalog heuristic", prefix, name, type);
            }
            registerOne(streamCatalog, name, type, connection, prefix + name, registered, errors);
        }
    }

    private static void registerOne(
            StreamCatalog streamCatalog,
            String name,
            TableCatalogType type,
            Map<String, String> connection,
            String source,
            List<String> registered,
            List<String> errors) {
        TableCatalog catalog = new TableCatalog(name, type, connection, Map.of());
        try {
            streamCatalog.registerTableCatalog(catalog).join();
            registered.add(name);
            log.info("Bootstrapped TableCatalog {} (type={}, connection keys={})",
                    name, type, connection.keySet());
        } catch (RuntimeException re) {
            // join() wraps async failures in CompletionException; unwrap to detect AlreadyExists.
            Throwable cause = (re instanceof CompletionException && re.getCause() != null)
                    ? re.getCause() : re;
            if (cause instanceof AlreadyExistsException) {
                // Idempotent: a prior bootstrap already registered this catalog.
                registered.add(name);
                log.info("TableCatalog {} already registered; bootstrap is idempotent", name);
                return;
            }
            String msg = source + ": " + cause.getClass().getSimpleName()
                    + (cause.getMessage() != null ? ": " + cause.getMessage() : "");
            errors.add(msg);
            log.warn("Failed to register TableCatalog from {}: {}", source, cause.toString(), cause);
        }
    }

    private static TableCatalogType classifyDelta(String name, Map<String, String> connection) {
        if (UNITY_NAME.equalsIgnoreCase(name)) {
            return TableCatalogType.DELTA_UC;
        }
        for (String key : connection.keySet()) {
            String lower = key.toLowerCase(Locale.ROOT);
            if (lower.startsWith("unity-catalog-") || lower.startsWith("unity_catalog_")
                    || lower.equals("catalog-impl")) {
                return TableCatalogType.DELTA_UC;
            }
        }
        return TableCatalogType.DELTA;
    }

    /**
     * Groups properties matching {@code <prefix><name>.<key>=<value>} by {@code <name>}.
     * Returned outer/inner maps preserve insertion order for deterministic logging.
     */
    private static Map<String, Map<String, String>> groupByPrefix(Properties properties, String prefix) {
        // Sort keys for stable ordering across runs.
        Map<String, String> ordered = new TreeMap<>();
        for (String name : properties.stringPropertyNames()) {
            if (name.startsWith(prefix)) {
                ordered.put(name, properties.getProperty(name));
            }
        }
        Map<String, Map<String, String>> groups = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : ordered.entrySet()) {
            String suffix = entry.getKey().substring(prefix.length());
            if (suffix.isEmpty()) {
                continue;
            }
            int dot = suffix.indexOf('.');
            String groupName;
            String subKey;
            if (dot < 0) {
                groupName = suffix;
                subKey = "";
            } else {
                groupName = suffix.substring(0, dot);
                subKey = suffix.substring(dot + 1);
            }
            if (groupName.isEmpty()) {
                continue;
            }
            Map<String, String> inner = groups.computeIfAbsent(groupName, k -> new LinkedHashMap<>());
            if (!subKey.isEmpty()) {
                inner.put(subKey, entry.getValue());
            }
        }
        return groups;
    }

    /**
     * Collects the flat {@code unityCatalog*} family (NOT prefix-grouped) into a single map,
     * stripping the prefix and converting camelCase to kebab-case.
     * Other prefix-grouped {@code unityCatalog.*} forms are not captured here.
     */
    private static Map<String, String> collectFlatUnity(Properties properties) {
        Map<String, String> result = new LinkedHashMap<>();
        // Iterate in sorted key order for deterministic test assertions.
        Map<String, String> ordered = new TreeMap<>();
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith(UNITY_FLAT_PREFIX)
                    && key.length() > UNITY_FLAT_PREFIX.length()
                    && Character.isUpperCase(key.charAt(UNITY_FLAT_PREFIX.length()))) {
                ordered.put(key, properties.getProperty(key));
            }
        }
        for (Map.Entry<String, String> entry : ordered.entrySet()) {
            String key = entry.getKey();
            String trimmed = key.substring(UNITY_FLAT_PREFIX.length()); // e.g. "Uri"
            String kebab = "unity-catalog-" + camelToKebab(trimmed);
            result.put(kebab, entry.getValue());
        }
        return result;
    }

    /**
     * Converts a camelCase/PascalCase identifier into its kebab-case form.
     *
     * <p>Example: {@code "TokenFile"} → {@code "token-file"}, {@code "Uri"} → {@code "uri"}.
     * Returns lowercase input unchanged when there are no internal capital letters.
     */
    // Visible for testing.
    static String camelToKebab(String camelCase) {
        if (camelCase == null || camelCase.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder(camelCase.length() + 4);
        for (int i = 0; i < camelCase.length(); i++) {
            char c = camelCase.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) {
                    sb.append('-');
                }
                sb.append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    @FunctionalInterface
    private interface DeltaTypeClassifier {
        TableCatalogType classify(String name, Map<String, String> connection);
    }

    /**
     * Summary of a {@link #bootstrap(StreamCatalog, Properties)} call.
     *
     * <p>Fields:
     * <ul>
     *   <li>{@code registered} — catalog names successfully upserted (or already present)</li>
     *   <li>{@code skipped} — {@code <prefix>.<name>} entries recognised but with no useful
     *       subkeys, plus BigQuery groups skipped pending enum support</li>
     *   <li>{@code errors} — {@code <prefix>.<name>: <error message>} entries collected from
     *       registration failures</li>
     * </ul>
     */
    public record BootstrapResult(
            List<String> registered,
            List<String> skipped,
            List<String> errors) {

        public BootstrapResult {
            Objects.requireNonNull(registered, "registered");
            Objects.requireNonNull(skipped, "skipped");
            Objects.requireNonNull(errors, "errors");
            registered = Collections.unmodifiableList(new ArrayList<>(registered));
            skipped = Collections.unmodifiableList(new ArrayList<>(skipped));
            errors = Collections.unmodifiableList(new ArrayList<>(errors));
        }

        /** Convenience: did the bootstrap encounter any registration failures? */
        public boolean hasErrors() {
            return !errors.isEmpty();
        }
    }
}
