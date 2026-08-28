/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.catalog.unity;

import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.GenerateTemporaryTableCredentialResponse;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableOperation;
import io.delta.kernel.types.StructType;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.unitycatalog.client.model.StagingTableInfo;
import java.util.Optional;

public interface UnityCatalogApi {

    static UnityCatalogApi getInstance(LakehouseConfiguration config) {
        if (config.isMockUnityCatalog()) {
            return MockUnityCatalog.getInstance(config);
        } else {
            return DatabricksUnityCatalog.getInstance(config);
        }
    }

    boolean isEnableUnityCatalog();

    TableInfo createExternalTable(String catalogName, UnityTableIdentifier identifier, String location,
                                  StructType structType);

    TableInfo createManagedTable(String catalogName, UnityTableIdentifier identifier, String tableId, String location,
                                 StructType structType);

    StagingTableInfo createStagingTable(String catalogName, UnityTableIdentifier identifier);

    Optional<TableInfo> getTable(String catalogName, UnityTableIdentifier identifier);

    void updateExternalTable(String catalogName, UnityTableIdentifier identifier, String location,
                             StructType structType);

    Optional<CatalogInfo> getCatalog(String catalogName);

    GenerateTemporaryTableCredentialResponse getTemporaryTableCredentials(String tableId,
                                                                          TableOperation operation);

    void createOrUpdateExternalLineage(UnityCatalogExternalLineageRequest request);
}
