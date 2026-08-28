/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableType;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityCatalogApi;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityTableIdentifier;
import java.util.Optional;

public class ExternalDeltaTableFactory {

    private ExternalDeltaTableFactory() {
    }

    public static ExternalDeltaTable getDeltaTable(LakehouseConfiguration config, String parentTopic) {
        UnityCatalogApi unityCatalogApi = UnityCatalogApi.getInstance(config);
        if (!unityCatalogApi.isEnableUnityCatalog()) {
            if (config.getStreamTableMode() == LakehouseConfiguration.StreamTableMode.EXTERNAL) {
                return new DirectExternalTable(config, parentTopic);
            }
            throw new IllegalArgumentException("Delta uc table must enable Unity catalog.");
        }
        UnityTableIdentifier identifier = UnityTableIdentifier.parse(parentTopic);
        Optional<TableInfo> tableOpt = unityCatalogApi.getTable(config.getUnityCatalogName(), identifier);
        if (tableOpt.isEmpty()) {
            if (config.isDeltaSupportManagedCommit()) {
                return new UCManagedTable(config, parentTopic);
            } else {
                return new UCExternalTable(config, parentTopic);
            }
        } else {
            TableInfo unityTable = tableOpt.get();
            if (unityTable.getTableType() == TableType.MANAGED) {
                return new UCManagedTable(config, parentTopic, unityTable);
            } else {
                return new UCExternalTable(config, parentTopic, unityTable);
            }
        }
    }
}
