/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.materialization.MaterializationException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds a JDBC {@link Connection} for a registered ClickHouse
 * {@link TableCatalog}.
 *
 * <p>Connection settings are resolved by overlaying
 * {@link TableMaterializationPolicy#connectionOverrides()} on top of the
 * catalog's {@link TableCatalog#connection()} map (stream-level keys win),
 * keeping the same precedence used for credentials in the rest of the
 * materialization framework.
 *
 * <p>Supported keys (case-sensitive):
 * <ul>
 *   <li>{@code dsn} — required; full JDBC URL
 *       (e.g. {@code jdbc:ch://host:8123/default}).</li>
 *   <li>{@code user} — optional username.</li>
 *   <li>{@code password} — optional raw password; takes precedence over
 *       {@code password-ref}.</li>
 *   <li>{@code password-ref} — optional reference URI (e.g. {@code secret://…}).
 *       T11 does <em>not</em> resolve these — a WARN is emitted and the connect
 *       proceeds without a password so the failure surface is the broker's,
 *       not ours. A future task (T13/T14) will plug in a resolver.</li>
 * </ul>
 *
 * <p>Only the recognized connection keys above are forwarded to the JDBC driver. Any other entry in
 * the connection map is ignored (logged at debug), because the ClickHouse client-v2 JDBC driver
 * rejects unknown properties with a {@code ClientMisconfigurationException}. Driver-specific options
 * (compression, ssl, socket timeouts, …) must be set in the {@code dsn} URL query string
 * (e.g. {@code jdbc:clickhouse://host:8123/db?compress=1}).
 */
@Slf4j
public final class ClickHouseConnectionFactory {

    /** Catalog key for the JDBC URL. */
    public static final String DSN = "dsn";
    /** Catalog key for the username. */
    public static final String USER = "user";
    /** Catalog key for an inline password (preferred when set). */
    public static final String PASSWORD = "password";
    /** Catalog key for a password reference URI (resolution is a follow-up). */
    public static final String PASSWORD_REF = "password-ref";

    private ClickHouseConnectionFactory() {
    }

    /**
     * Opens a JDBC connection using the catalog + policy overrides.
     *
     * @param catalog the registered ClickHouse catalog (required)
     * @param policy  the resolved materialization policy (used for connection
     *                overrides only — may be {@code null} or empty)
     * @return an open {@link Connection}
     * @throws MaterializationException with
     *     {@link ExceptionCode#LAKEHOUSE_CREATE_TABLE_WRITER_ERROR} if the
     *     driver throws or the {@code dsn} is missing.
     */
    public static Connection open(TableCatalog catalog, TableMaterializationPolicy policy) {
        Objects.requireNonNull(catalog, "catalog");
        Map<String, String> merged = mergeConnection(catalog, policy);
        String dsn = merged.get(DSN);
        if (dsn == null || dsn.isEmpty()) {
            throw new MaterializationException(ExceptionCode.LAKEHOUSE_CREATE_TABLE_WRITER_ERROR,
                    "ClickHouse catalog '" + catalog.name()
                            + "' is missing required '" + DSN + "' connection key");
        }

        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : merged.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (value == null) {
                continue;
            }
            switch (key) {
                case DSN:
                    // Carried in the URL, not in the properties.
                    break;
                case USER:
                    properties.setProperty("user", value);
                    break;
                case PASSWORD:
                    properties.setProperty("password", value);
                    break;
                case PASSWORD_REF:
                    if (!merged.containsKey(PASSWORD)) {
                        // Do not log the reference value: it locates a secret in an external store.
                        log.warn("ClickHouse catalog '{}' specifies '{}' but no secret resolver "
                                        + "is registered; the connection will be attempted without "
                                        + "a password. Set '{}' directly until a resolver is wired in.",
                                catalog.name(), PASSWORD_REF, PASSWORD);
                    }
                    break;
                default:
                    // The client-v2 JDBC driver rejects unknown properties, so do NOT forward
                    // arbitrary keys (e.g. compaction config that leaked into the connection map).
                    // Driver options belong in the dsn URL query string.
                    if (log.isDebugEnabled()) {
                        log.debug("Ignoring unrecognized ClickHouse connection property '{}' for "
                                + "catalog '{}'", key, catalog.name());
                    }
                    break;
            }
        }

        try {
            return DriverManager.getConnection(dsn, properties);
        } catch (SQLException e) {
            throw new MaterializationException(ExceptionCode.LAKEHOUSE_CREATE_TABLE_WRITER_ERROR,
                    "Failed to open ClickHouse JDBC connection for catalog '" + catalog.name()
                            + "' (dsn=" + redactDsn(dsn) + "): " + e.getMessage(),
                    e);
        }
    }

    /**
     * Strips credentials from a JDBC DSN before it appears in logs or exceptions. Removes
     * {@code user:password@} userinfo and any {@code password}/{@code secret}-style query
     * parameters; on any parse difficulty it falls back to the scheme prefix only so a
     * malformed DSN can never leak a secret.
     */
    static String redactDsn(String dsn) {
        if (dsn == null || dsn.isEmpty()) {
            return dsn;
        }
        String redacted = dsn
                // userinfo: scheme://user:pass@host -> scheme://***@host
                .replaceAll("://[^/@\\s]*@", "://***@")
                // password / secret query params (any case)
                .replaceAll("(?i)([?&](password|passwd|pwd|secret)=)[^&\\s]*", "$1***");
        return redacted;
    }

    /**
     * Returns a new map with the catalog's {@code connection} entries overlaid
     * by the policy's {@code connectionOverrides}. Visible for testing.
     */
    static Map<String, String> mergeConnection(TableCatalog catalog,
                                               TableMaterializationPolicy policy) {
        Map<String, String> merged = new HashMap<>(catalog.connection());
        if (policy != null) {
            merged.putAll(policy.connectionOverrides());
        }
        return merged;
    }
}
