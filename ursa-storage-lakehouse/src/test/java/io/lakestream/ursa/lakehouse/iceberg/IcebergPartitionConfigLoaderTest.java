/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Slf4j
@Tag("lakehouse")
public class IcebergPartitionConfigLoaderTest {

    @Test
    void testLoadPartitionConfig() {
        try {
            String jsonPartitions = "[{\"sourceColumn\":\"timestamp_col\",\"transform\":\"day\","
                + "\"targetName\":\"ts_day\"},{\"sourceColumn\":\"category\",\"transform\":\"identity\","
                + "\"targetName\":\"category_partition\"}]";
            List<IcebergPartitionConfig> partitions = IcebergPartitionConfigLoader.loadFromJson(jsonPartitions);
            assertEquals(2, partitions.size());
            assertEquals("timestamp_col", partitions.get(0).getSourceColumn());
            assertEquals("day", partitions.get(0).getTransform());
            assertEquals("ts_day", partitions.get(0).getTargetName());
            assertEquals("category", partitions.get(1).getSourceColumn());
            assertEquals("identity", partitions.get(1).getTransform());
            assertEquals("category_partition", partitions.get(1).getTargetName());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    void testTransformMissing() {
        try {
            String jsonPartitions = "[{\"sourceColumn\":\"timestamp_col\",\"targetName\":\"ts_day\"},"
                + "{\"sourceColumn\":\"category\",\"targetName\":\"category_partition\"}]";
            List<IcebergPartitionConfig> partitions = IcebergPartitionConfigLoader.loadFromJson(jsonPartitions);
            assertEquals(2, partitions.size());
            assertEquals("timestamp_col", partitions.get(0).getSourceColumn());
            assertNull(partitions.get(0).getTransform());
            assertEquals("ts_day", partitions.get(0).getTargetName());
            assertEquals("category", partitions.get(1).getSourceColumn());
            assertNull(partitions.get(1).getTransform());
            assertEquals("category_partition", partitions.get(1).getTargetName());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    void testTargetNameMissing() {
        try {
            String jsonPartitions = "[{\"sourceColumn\":\"timestamp_col\",\"transform\":\"day\"},"
                + "{\"sourceColumn\":\"category\",\"transform\":\"identity\"}]";
            List<IcebergPartitionConfig> partitions = IcebergPartitionConfigLoader.loadFromJson(jsonPartitions);
            assertEquals(2, partitions.size());
            assertEquals("timestamp_col", partitions.get(0).getSourceColumn());
            assertEquals("day", partitions.get(0).getTransform());
            assertNull(partitions.get(0).getTargetName());
            assertEquals("category", partitions.get(1).getSourceColumn());
            assertEquals("identity", partitions.get(1).getTransform());
            assertNull(partitions.get(1).getTargetName());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    void testEmptyJson() {
        try {
            String jsonPartitions = "[]";
            List<IcebergPartitionConfig> partitions = IcebergPartitionConfigLoader.loadFromJson(jsonPartitions);
            assertEquals(0, partitions.size());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    void testInvalidJsonV1() {
        try {
            String jsonPartitions = "xxx";
            List<IcebergPartitionConfig> partitions = IcebergPartitionConfigLoader.loadFromJson(jsonPartitions);
            fail();
        } catch (IOException e) {
            // Expected exception
        }
    }

    @Test
    void testInvalidJsonV2() {
        try {
            String jsonPartitions = "[{\"aa\":\"bb\",\"dd\":\"cc\"},{\"aab\":\"bbc\",\"ccd\":\"xxx\"}]";
            List<IcebergPartitionConfig> partitions = IcebergPartitionConfigLoader.loadFromJson(jsonPartitions);
            fail();
        } catch (IOException e) {
            // Expected exception
        }
    }
}
