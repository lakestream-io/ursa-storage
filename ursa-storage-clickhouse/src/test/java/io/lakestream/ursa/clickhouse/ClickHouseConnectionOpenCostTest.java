/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Micro-benchmark for the cost of {@link ClickHouseConnectionFactory#open} — the per-compaction-task
 * connection open the ClickHouse sink pays today (a fresh JDBC connection per task, closed by
 * {@code ClickHouseTableMaterializer.close()}). Measures both the open-only latency and the realistic
 * per-task round trip (open + a trivial {@code SELECT 1} + close), reported as percentiles, to decide
 * whether a per-catalog connection pool is worth adding.
 *
 * <p>Docker-gated ({@code @Tag("clickhouse")}, excluded by default). Run with:
 * <pre>
 *   mvn -B -ntp test -pl ursa-storage-clickhouse -Dgroups=clickhouse -DexcludeGroups= \
 *       -Dtest=ClickHouseConnectionOpenCostTest
 * </pre>
 */
@Slf4j
@Tag("clickhouse")
@Testcontainers
class ClickHouseConnectionOpenCostTest {

    private static final DockerImageName CLICKHOUSE_IMAGE =
            DockerImageName.parse("clickhouse/clickhouse-server:24.10")
                    .asCompatibleSubstituteFor("clickhouse/clickhouse-server");

    @Container
    static final ClickHouseContainer CLICKHOUSE = new ClickHouseContainer(CLICKHOUSE_IMAGE);

    private static final int WARMUP = 20;
    private static final int ITERATIONS = 300;

    @Test
    void measureOpenCost() throws Exception {
        // Exercise the real production path: ClickHouseConnectionFactory.open reads dsn/user/password
        // from the catalog connection() map and calls DriverManager.getConnection.
        TableCatalog catalog = new TableCatalog(
                "bench-ch",
                TableCatalogType.CLICKHOUSE,
                Map.of(
                        ClickHouseConnectionFactory.DSN, CLICKHOUSE.getJdbcUrl(),
                        ClickHouseConnectionFactory.USER, CLICKHOUSE.getUsername(),
                        ClickHouseConnectionFactory.PASSWORD, CLICKHOUSE.getPassword()),
                Map.of());

        for (int i = 0; i < WARMUP; i++) {
            try (Connection c = ClickHouseConnectionFactory.open(catalog, null)) {
                queryOne(c);
            }
        }

        long[] openNanos = new long[ITERATIONS];
        long[] roundTripNanos = new long[ITERATIONS];
        for (int i = 0; i < ITERATIONS; i++) {
            long t0 = System.nanoTime();
            Connection connection = ClickHouseConnectionFactory.open(catalog, null);
            long t1 = System.nanoTime();
            try {
                queryOne(connection);
            } finally {
                connection.close();
            }
            long t2 = System.nanoTime();
            openNanos[i] = t1 - t0;
            roundTripNanos[i] = t2 - t0;
        }

        report("open() only", openNanos);
        report("open() + SELECT 1 + close()", roundTripNanos);
    }

    private static void queryOne(Connection connection) throws Exception {
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery("SELECT 1")) {
            rs.next();
        }
    }

    private static void report(String label, long[] samplesNanos) {
        long[] sorted = samplesNanos.clone();
        Arrays.sort(sorted);
        long sum = 0;
        for (long v : sorted) {
            sum += v;
        }
        log.info("ClickHouse connection cost [{}] over {} samples: "
                        + "min={} p50={} p90={} p99={} max={} mean={}",
                label, sorted.length,
                ms(sorted[0]),
                ms(percentile(sorted, 50)),
                ms(percentile(sorted, 90)),
                ms(percentile(sorted, 99)),
                ms(sorted[sorted.length - 1]),
                ms(sum / sorted.length));
    }

    /** Nearest-rank percentile (p in [0,100]) over an ascending-sorted array. */
    private static long percentile(long[] sorted, int p) {
        int rank = (int) Math.ceil(p / 100.0 * sorted.length);
        int idx = Math.min(Math.max(rank - 1, 0), sorted.length - 1);
        return sorted[idx];
    }

    private static String ms(long nanos) {
        return String.format(java.util.Locale.ROOT, "%.3fms", nanos / 1_000_000.0);
    }
}
