/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.io.parquet;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import org.apache.parquet.column.ParquetProperties;

public class ParquetConfig {

    private final LakehouseConfiguration configuration;

    public ParquetConfig(LakehouseConfiguration configuration) {
        this.configuration = configuration;
    }

    long getRowGroupSize() {
        return getLong("parquetRowGroupSize", 8 * 1024 * 1024L);
    }

    int getRowGroupRowCountLimit() {
        return getInt("parquetRowGroupRowCountLimit", ParquetProperties.DEFAULT_ROW_GROUP_ROW_COUNT_LIMIT);
    }

    int getPageSize() {
        return getInt("parquetPageSize", ParquetProperties.DEFAULT_PAGE_SIZE);
    }

    int getPageRowCountLimit() {
        return getInt("parquetPageRowCountLimit", ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT);
    }

    int getMinRowCountForPageSizeCheck() {
        return  getInt("parquetMinRowCountForPageSizeCheck", 10);
    }

    int getMaxRowCountForPageSizeCheck() {
        return getInt("parquetMaxRowCountForPageSizeCheck", 10000);
    }

    public int estimateMaxOpenParquetFilesForReading() {
        // use the heap memory 1/10 as the read limit
        var defaultValue = Math.max(1, Math.toIntExact(Runtime.getRuntime().maxMemory() / 10 / getRowGroupSize()));
        return getInt("parquetMaxOpenParquetFilesForReading", defaultValue);
    }

    private int getInt(String property, int defaultValue) {
        var value = configuration.getProperties().getProperty(property);
        if (value == null) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    private long getLong(String property, long defaultValue) {
        var value = configuration.getProperties().getProperty(property);
        if (value == null) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }
}
