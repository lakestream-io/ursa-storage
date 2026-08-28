/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import com.databricks.sdk.service.catalog.GenerateTemporaryTableCredentialResponse;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableOperation;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityCatalogUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public abstract class UCTable extends ExternalDeltaTable {

    protected TableInfo unityTable;

    protected GenerateTemporaryTableCredentialResponse tmpCredential;

    public UCTable(LakehouseConfiguration config, String parentTopic) {
        super(config, parentTopic);
    }

    private static final long CREDENTIAL_REFRESH_BUFFER_MS = 5 * 60 * 1000L;

    public synchronized GenerateTemporaryTableCredentialResponse getTmpCredential() {
        if (unityTable == null) {
            throw new IllegalStateException("The uc table is not initialized");
        }
        if (tmpCredential == null
            || System.currentTimeMillis() > tmpCredential.getExpirationTime() - CREDENTIAL_REFRESH_BUFFER_MS) {
            tmpCredential = unityCatalogApi.getTemporaryTableCredentials(unityTable.getTableId(),
                TableOperation.READ_WRITE);
        }
        return tmpCredential;
    }

    @Override
    synchronized void refreshTable() {
        if (unityTable == null) {
            return;
        }
        if (tmpCredential == null
            || System.currentTimeMillis() > tmpCredential.getExpirationTime() - CREDENTIAL_REFRESH_BUFFER_MS) {
            //refresh token and rebuild engine and table.
            tmpCredential = unityCatalogApi.getTemporaryTableCredentials(unityTable.getTableId(),
                TableOperation.READ_WRITE);
            Configuration externalHadoopConfig =
                UnityCatalogUtil.generateExternalHadoopConfig(config, tmpCredential);
            engine = DefaultEngine.create(externalHadoopConfig);
        }
    }

    @Override
    public synchronized Configuration getTableHadoopConfiguration() {
        return UnityCatalogUtil.generateExternalHadoopConfig(config, getTmpCredential());
    }

}
