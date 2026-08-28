/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.api.materialization.WriteMode;

/**
 * Destination table engine used by the ClickHouse materializer.
 *
 * <p>The selection is policy-driven: {@link #REPLACING_MERGE_TREE} is chosen
 * when {@link WriteMode#UPSERT} is configured (or when an explicit primary
 * key is set on the policy and the policy did not opt out of upserts);
 * otherwise the default {@link #MERGE_TREE} append-only engine is used.
 *
 * <p>The materializer does not issue DDL to create/alter the engine — that
 * responsibility belongs to the schema service (T12). The enum is carried on
 * the materializer so commit metadata can surface which engine was assumed.
 */
public enum ClickHouseTableEngine {

    /** Append-only {@code MergeTree} family. */
    MERGE_TREE,

    /**
     * {@code ReplacingMergeTree} family. Idempotent {@code INSERT} semantics rely
     * on the engine deduplicating by {@code ORDER BY} key and (optionally) a
     * version column. The materializer assumes the table has been created with
     * an appropriate {@code ORDER BY (<primaryKey>)} clause.
     */
    REPLACING_MERGE_TREE;

    /**
     * Decides which engine to assume for the supplied policy.
     *
     * <p>Rules:
     * <ul>
     *   <li>{@link WriteMode#UPSERT} → {@link #REPLACING_MERGE_TREE}.</li>
     *   <li>{@link WriteMode#CDC} → {@link #REPLACING_MERGE_TREE} (CDC requires
     *       row-level dedup, which only the replacing variant provides on the
     *       ClickHouse side).</li>
     *   <li>Otherwise an explicit, non-empty primary key implies upsert intent
     *       and selects {@link #REPLACING_MERGE_TREE}.</li>
     *   <li>Otherwise {@link #MERGE_TREE}.</li>
     * </ul>
     */
    public static ClickHouseTableEngine forPolicy(TableMaterializationPolicy policy) {
        if (policy == null) {
            return MERGE_TREE;
        }
        WriteMode mode = policy.framework()
                .flatMap(io.lakestream.api.materialization.FrameworkConf::writeMode)
                .orElse(null);
        if (mode == WriteMode.UPSERT || mode == WriteMode.CDC) {
            return REPLACING_MERGE_TREE;
        }
        boolean hasPk = policy.primaryKey().map(pk -> !pk.isEmpty()).orElse(false);
        if (hasPk) {
            return REPLACING_MERGE_TREE;
        }
        return MERGE_TREE;
    }
}
