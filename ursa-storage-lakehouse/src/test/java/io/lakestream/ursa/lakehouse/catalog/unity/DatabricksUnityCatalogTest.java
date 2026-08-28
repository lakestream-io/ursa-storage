/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.catalog.unity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.CreateExternalLineageRelationshipRequest;
import com.databricks.sdk.service.catalog.ExternalLineageAPI;
import com.databricks.sdk.service.catalog.ExternalLineageRelationship;
import com.databricks.sdk.service.catalog.ExternalMetadata;
import com.databricks.sdk.service.catalog.ExternalMetadataAPI;
import com.databricks.sdk.service.catalog.SystemType;
import com.databricks.sdk.service.catalog.UpdateExternalMetadataRequest;
import io.delta.kernel.types.StructType;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class DatabricksUnityCatalogTest {

    private WorkspaceClient mockWorkspaceClient;
    private ExternalMetadataAPI mockExternalMetadataAPI;
    private ExternalLineageAPI mockExternalLineageAPI;

    @BeforeEach
    void setUp() {
        DatabricksUnityCatalog.resetInstance();
        mockWorkspaceClient = mock(WorkspaceClient.class);
        mockExternalMetadataAPI = mock(ExternalMetadataAPI.class);
        mockExternalLineageAPI = mock(ExternalLineageAPI.class);
        when(mockWorkspaceClient.externalMetadata()).thenReturn(mockExternalMetadataAPI);
        when(mockWorkspaceClient.externalLineage()).thenReturn(mockExternalLineageAPI);
    }

    @AfterEach
    void tearDown() {
        DatabricksUnityCatalog.resetInstance();
        MockUnityCatalog.resetInstance();
    }

    private DatabricksUnityCatalog createUnityCatalog(Properties properties) {
        return new DatabricksUnityCatalog(new LakehouseConfiguration(properties), mockWorkspaceClient);
    }

    private DatabricksUnityCatalog createDisabledUnityCatalog() {
        return new DatabricksUnityCatalog(new LakehouseConfiguration(new Properties()), null);
    }

    @Test
    void testCatalogNameParameterMustNotBeNull() {
        DatabricksUnityCatalog unityCatalog = createUnityCatalog(new Properties());
        UnityTableIdentifier identifier = UnityTableIdentifier.parse("default/test-topic");
        StructType schema = new StructType();

        assertCatalogNameRequired(
            () -> unityCatalog.createExternalTable(null, identifier, "/tmp/table", schema));
        assertCatalogNameRequired(
            () -> unityCatalog.createManagedTable(null, identifier, "table-id", "/tmp/table", schema));
        assertCatalogNameRequired(
            () -> unityCatalog.createStagingTable(null, identifier));
        assertCatalogNameRequired(
            () -> unityCatalog.updateExternalTable(null, identifier, "/tmp/table", schema));
        assertCatalogNameRequired(
            () -> unityCatalog.getTable(null, identifier));
        assertCatalogNameRequired(
            () -> unityCatalog.getCatalog(null));
    }

    @Test
    void testCreateOrUpdateExternalLineageUsesProvidedRequest() {
        Properties properties = new Properties();
        properties.put("unityCatalogUri", "https://dbc-test.cloud.databricks.com/");
        DatabricksUnityCatalog unityCatalog = createUnityCatalog(properties);

        when(mockExternalMetadataAPI.getExternalMetadata(any(String.class))).thenReturn(new ExternalMetadata());
        when(mockExternalMetadataAPI.updateExternalMetadata(any(UpdateExternalMetadataRequest.class)))
                .thenReturn(new ExternalMetadata());
        when(mockExternalLineageAPI.createExternalLineageRelationship(
                any(CreateExternalLineageRelationshipRequest.class)))
                .thenReturn(new ExternalLineageRelationship());

        UnityCatalogExternalLineageRequest request = new UnityCatalogExternalLineageRequest(
                "ns/topic",
                "uc.org_analytics.topic",
                "lakestream_ursa_org_analytics_topic",
                "my-cluster",
                SystemType.KAFKA);

        unityCatalog.createOrUpdateExternalLineage(request);

        ArgumentCaptor<UpdateExternalMetadataRequest> metadataCaptor =
                ArgumentCaptor.forClass(UpdateExternalMetadataRequest.class);
        verify(mockExternalMetadataAPI).updateExternalMetadata(metadataCaptor.capture());
        assertEquals("lakestream_ursa_org_analytics_topic", metadataCaptor.getValue().getExternalMetadata().getName());
        assertEquals(SystemType.KAFKA, metadataCaptor.getValue().getExternalMetadata().getSystemType());
        assertEquals("my-cluster",
                metadataCaptor.getValue().getExternalMetadata().getProperties().get("source_lakestream_cluster"));

        ArgumentCaptor<CreateExternalLineageRelationshipRequest> lineageCaptor =
                ArgumentCaptor.forClass(CreateExternalLineageRelationshipRequest.class);
        verify(mockExternalLineageAPI).createExternalLineageRelationship(lineageCaptor.capture());
        assertEquals("uc.org_analytics.topic",
                lineageCaptor.getValue().getExternalLineageRelationship().getTarget().getTable().getName());
    }

    @Test
    void testCreateOrUpdateExternalLineageSkipsWhenDisabled() {
        DatabricksUnityCatalog unityCatalog = createDisabledUnityCatalog();
        unityCatalog.createOrUpdateExternalLineage(new UnityCatalogExternalLineageRequest(
                "ns/topic",
                "uc.org_analytics.topic",
                "lakestream_ursa_org_analytics_topic",
                "my-cluster",
                SystemType.KAFKA));

        verifyNoInteractions(mockExternalMetadataAPI);
        verifyNoInteractions(mockExternalLineageAPI);
    }

    @Test
    void testGetInstanceReusesDatabricksClientForDifferentCatalogs() {
        Properties properties = new Properties();
        properties.put("delta.catalog.alpha.unityCatalogUri", "http://127.0.0.1:8080");
        properties.put("delta.catalog.alpha.unityCatalogName", "uc-alpha");
        properties.put("delta.catalog.alpha.unityCatalogClientId", "client-alpha");
        properties.put("delta.catalog.alpha.unityCatalogClientSecret", "client-secret-alpha");
        properties.put("delta.catalog.beta.unityCatalogUri", "http://127.0.0.1:8080");
        properties.put("delta.catalog.beta.unityCatalogName", "uc-beta");
        properties.put("delta.catalog.beta.unityCatalogClientId", "client-alpha");
        properties.put("delta.catalog.beta.unityCatalogClientSecret", "client-secret-alpha");

        Properties alpha = new Properties();
        alpha.put("catalog.name", "alpha");
        alpha.putAll(properties);

        Properties beta = new Properties();
        beta.put("catalog.name", "beta");
        beta.putAll(properties);

        DatabricksUnityCatalog alphaCatalog =
            DatabricksUnityCatalog.getInstance(new LakehouseConfiguration(alpha));

        DatabricksUnityCatalog betaCatalog =
            DatabricksUnityCatalog.getInstance(new LakehouseConfiguration(beta));

        assertSame(alphaCatalog, betaCatalog);
    }

    @Test
    void testLegacyGlobalUnityCatalogConfigurationStillEnablesUnityCatalog() {
        Properties properties = new Properties();
        properties.put("unityCatalogUri", "http://127.0.0.1:8080");
        properties.put("unityCatalogName", "legacy-catalog");
        properties.put("unityCatalogClientId", "legacy-client");
        properties.put("unityCatalogClientSecret", "legacy-secret");

        DatabricksUnityCatalog unityCatalog =
            DatabricksUnityCatalog.getInstance(new LakehouseConfiguration(properties));

        assertTrue(unityCatalog.isEnableUnityCatalog());
        assertTrue(unityCatalog.getWorkspaceClient().isPresent());
    }

    private void assertCatalogNameRequired(Runnable runnable) {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, runnable::run);
        assertEquals("catalogName must not be null", error.getMessage());
    }
}
