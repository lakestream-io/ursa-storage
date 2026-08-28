/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.lakestream.ursa.storage.impl.StorageConfig;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class CompactionMainTest {

    @Test
    public void testPrintConfiguration() throws Exception {
        Properties properties = new Properties();
        properties.put("s3SecretAccessKey", "s3SecretAccessValue");
        properties.put("s3AccessKeyId", "s3AccessKeyIdValue");
        properties.put("unityCatalogToken", "unityCatalogTokenValue");
        properties.put("unityCatalogClientSecret", "unityCatalogClientSecretValue");
        properties.put("iceberg.credential", "icebergCredentialValue");
        properties.put("iceberg.catalog.uat-lakestream.v2.credential", "catalogCredentialValue");
        properties.put("iceberg.catalog.uat-lakestream.v2.warehouse", "warehouseValue");

        StorageConfig safeProperties = CompactionMain.printConfiguration(properties, "test");
        for (String credentialKey : StorageConfig.CREDENTIAL_KEYS) {
            assertEquals("******", safeProperties.getProperties().get(credentialKey));
        }
        assertFalse(safeProperties.getProperties().containsKey(
                "iceberg.catalog.uat-lakestream.v2.credential"));
        assertEquals("warehouseValue", safeProperties.getProperties().get(
                "iceberg.catalog.uat-lakestream.v2.warehouse"));
        assertEquals("catalogCredentialValue", properties.get(
                "iceberg.catalog.uat-lakestream.v2.credential"));
    }
}
