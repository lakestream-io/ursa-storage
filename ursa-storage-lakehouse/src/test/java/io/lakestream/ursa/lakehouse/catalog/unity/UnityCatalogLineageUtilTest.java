/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.catalog.unity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class UnityCatalogLineageUtilTest {

    @Test
    void testBuildDeltaRequest() {
        Properties properties = new Properties();
        properties.put("unityCatalogName", "uc-main");
        properties.put("clusterName", "cluster");
        properties.put("unityCatalogByolSystemType", "KAFKA");
        LakehouseConfiguration config = new LakehouseConfiguration(properties);

        UnityCatalogExternalLineageRequest request =
                UnityCatalogLineageUtil.buildDeltaRequest(config, "ns/my-topic.v1").orElseThrow();

        assertEquals("ns/my-topic.v1", request.getTopicName());
        assertEquals("uc-main.ns.my__topic_v1", request.getTableFullName());
        assertEquals("lakestream_ursa_ns_my__topic_v1", request.getTopicMetadataName());
        assertEquals("cluster", request.getClusterName());
    }

    @Test
    void testBuildIcebergRequestSkipsNonUnityCatalogBackend() {
        Properties properties = new Properties();
        properties.put("lakehouseType", "ICEBERG");
        properties.put("catalog-backend", "HADOOP");
        LakehouseConfiguration config = new LakehouseConfiguration(properties);

        assertTrue(UnityCatalogLineageUtil.buildIcebergRequest(config, "ns/table",
                Optional.empty()).isEmpty());
    }

    @Test
    void testBuildIcebergRequest() {
        Properties properties = new Properties();
        properties.put("lakehouseType", "ICEBERG");
        properties.put("clusterName", "cluster");
        properties.put("iceberg.catalog.alpha.catalog-backend", "UNITYCATALOG");
        properties.put("iceberg.catalog.alpha.uri", "https://workspace/api/2.1/unity-catalog/iceberg-rest");
        properties.put("iceberg.catalog.alpha.warehouse", "catalog-a");
        LakehouseConfiguration config = new LakehouseConfiguration(properties);

        UnityCatalogExternalLineageRequest request =
                UnityCatalogLineageUtil.buildIcebergRequest(config, "ns/table",
                        Optional.of("alpha")).orElseThrow();

        assertEquals("ns/table", request.getTopicName());
        assertEquals("catalog-a.cluster_ns.table", request.getTableFullName());
        assertEquals("lakestream_ursa_cluster_ns_table", request.getTopicMetadataName());
        assertEquals("cluster", request.getClusterName());
    }

    @Test
    void testBuildRequestsForDeltaAndIceberg() {
        Properties properties = new Properties();
        properties.put("unityCatalogName", "uc-main");
        properties.put("clusterName", "cluster");
        properties.put("unityCatalogByolSystemType", "KAFKA");
        properties.put("lakehouseType", "DELTA_AND_ICEBERG");
        properties.put("iceberg.catalog.alpha.catalog-backend", "UNITYCATALOG");
        properties.put("iceberg.catalog.alpha.uri", "https://workspace/api/2.1/unity-catalog/iceberg-rest");
        properties.put("iceberg.catalog.alpha.warehouse", "catalog-a");
        LakehouseConfiguration config = new LakehouseConfiguration(properties);

        List<UnityCatalogExternalLineageRequest> requests =
                UnityCatalogLineageUtil.buildRequests(config, "ns/table", Optional.of("alpha"));

        assertEquals(2, requests.size());
        assertTrue(requests.stream().anyMatch(r -> "uc-main.ns.table".equals(r.getTableFullName())));
        assertTrue(requests.stream().anyMatch(r -> "catalog-a.cluster_ns.table".equals(r.getTableFullName())));
    }
}
