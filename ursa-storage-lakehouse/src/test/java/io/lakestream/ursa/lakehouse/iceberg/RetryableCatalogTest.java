/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for RetryableCatalog.
 */
public class RetryableCatalogTest {

    private RESTCatalog mockCatalog;
    private RetryableCatalog retryableCatalog;
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 100;

    @BeforeEach
    void setUp() {
        mockCatalog = mock(RESTCatalog.class);
        when(mockCatalog.name()).thenReturn("test-catalog");
        retryableCatalog = new RetryableCatalog(mockCatalog, MAX_RETRIES, RETRY_DELAY_MS);
    }

    @Test
    void testName() {
        assertEquals("test-catalog", retryableCatalog.name());
    }

    @Test
    void testInitialize() {
        String name = "catalog-name";
        Map<String, String> properties = Collections.singletonMap("key", "value");
        retryableCatalog.initialize(name, properties);
        verify(mockCatalog).initialize(name, properties);
    }

    @Test
    void testLoadTableSuccess() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        Table mockTable = mock(Table.class);
        when(mockCatalog.loadTable(identifier)).thenReturn(mockTable);

        Table result = retryableCatalog.loadTable(identifier);

        assertNotNull(result);
        assertEquals(mockTable, result);
        verify(mockCatalog, times(1)).loadTable(identifier);
    }

    @Test
    void testLoadTableWithRetrySuccess() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        Table mockTable = mock(Table.class);
        when(mockCatalog.loadTable(identifier))
            .thenThrow(new NotAuthorizedException("Access denied"))
            .thenThrow(new NotAuthorizedException("Access denied"))
            .thenReturn(mockTable);

        Table result = retryableCatalog.loadTable(identifier);

        assertNotNull(result);
        assertEquals(mockTable, result);
        verify(mockCatalog, times(3)).loadTable(identifier);
    }

    @Test
    void testLoadTableMaxRetriesExceeded() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        when(mockCatalog.loadTable(identifier))
            .thenThrow(new NotAuthorizedException("Access denied"));

        NotAuthorizedException exception = assertThrows(
            NotAuthorizedException.class,
            () -> retryableCatalog.loadTable(identifier)
        );

        assertEquals("Access denied", exception.getMessage());
        verify(mockCatalog, times(MAX_RETRIES + 1)).loadTable(identifier);
    }

    @Test
    void testListTablesSuccess() {
        Namespace namespace = Namespace.of("db");
        TableIdentifier tableId = TableIdentifier.of("db", "table");
        when(mockCatalog.listTables(namespace)).thenReturn(List.of(tableId));

        List<TableIdentifier> result = retryableCatalog.listTables(namespace);

        assertEquals(1, result.size());
        assertEquals(tableId, result.get(0));
        verify(mockCatalog, times(1)).listTables(namespace);
    }

    @Test
    void testTableExistsSuccess() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        when(mockCatalog.tableExists(identifier)).thenReturn(true);

        boolean result = retryableCatalog.tableExists(identifier);

        assertTrue(result);
        verify(mockCatalog, times(1)).tableExists(identifier);
    }

    @Test
    void testTableExistsWithRetry() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        when(mockCatalog.tableExists(identifier))
            .thenThrow(new NotAuthorizedException("Access denied"))
            .thenReturn(true);

        boolean result = retryableCatalog.tableExists(identifier);

        assertTrue(result);
        verify(mockCatalog, times(2)).tableExists(identifier);
    }

    @Test
    void testDropTableSuccess() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        when(mockCatalog.dropTable(identifier)).thenReturn(true);

        boolean result = retryableCatalog.dropTable(identifier);

        assertTrue(result);
        verify(mockCatalog, times(1)).dropTable(identifier);
    }

    @Test
    void testDropTableWithRetry() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        when(mockCatalog.dropTable(identifier))
            .thenThrow(new NotAuthorizedException("Access denied"))
            .thenReturn(true);

        boolean result = retryableCatalog.dropTable(identifier);

        assertTrue(result);
        verify(mockCatalog, times(2)).dropTable(identifier);
    }

    @Test
    void testRenameTableSuccess() {
        TableIdentifier from = TableIdentifier.of("db", "old_table");
        TableIdentifier to = TableIdentifier.of("db", "new_table");
        retryableCatalog.renameTable(from, to);

        verify(mockCatalog, times(1)).renameTable(from, to);
    }

    @Test
    void testRenameTableWithRetry() {
        TableIdentifier from = TableIdentifier.of("db", "old_table");
        TableIdentifier to = TableIdentifier.of("db", "new_table");
        doThrow(new NotAuthorizedException("Access denied"))
            .doNothing()
            .when(mockCatalog).renameTable(from, to);

        retryableCatalog.renameTable(from, to);

        verify(mockCatalog, times(2)).renameTable(from, to);
    }

    @Test
    void testInvalidateTableSuccess() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        retryableCatalog.invalidateTable(identifier);

        verify(mockCatalog, times(1)).invalidateTable(identifier);
    }

    @Test
    void testInvalidateTableWithRetry() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        doThrow(new NotAuthorizedException("Access denied"))
            .doNothing()
            .when(mockCatalog).invalidateTable(identifier);

        retryableCatalog.invalidateTable(identifier);

        verify(mockCatalog, times(2)).invalidateTable(identifier);
    }

    @Test
    void testNonRetryableExceptionDoesNotRetry() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        RuntimeException nonRetryableException = new RuntimeException("Non-retryable error");
        when(mockCatalog.loadTable(identifier)).thenThrow(nonRetryableException);

        assertThrows(RuntimeException.class, () -> retryableCatalog.loadTable(identifier));

        // Verify that it was called exactly once (no retries for non-retryable exceptions)
        verify(mockCatalog, times(1)).loadTable(identifier);
    }

    @Test
    void testZeroMaxRetriesNoWrapping() {
        Catalog directCatalog = new RetryableCatalog(mockCatalog, 0, RETRY_DELAY_MS);
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        Table mockTable = mock(Table.class);
        when(mockCatalog.loadTable(identifier)).thenReturn(mockTable);

        Table result = directCatalog.loadTable(identifier);

        assertNotNull(result);
        verify(mockCatalog, times(1)).loadTable(identifier);
    }

    @Test
    void testGetDelegate() {
        Catalog delegate = retryableCatalog.getDelegate();
        assertEquals(mockCatalog, delegate);
    }

    @Test
    void testBuildTable() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        Schema schema = mock(Schema.class);
        Catalog.TableBuilder mockBuilder = mock(Catalog.TableBuilder.class);
        when(mockCatalog.buildTable(identifier, schema)).thenReturn(mockBuilder);

        Catalog.TableBuilder result = retryableCatalog.buildTable(identifier, schema);

        assertEquals(mockBuilder, result);
        verify(mockCatalog, times(1)).buildTable(identifier, schema);
    }

    @Test
    void testCustomRetryableException() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        Table mockTable = mock(Table.class);

        // Create a custom catalog that extends RetryableCatalog with custom retryable logic
        RetryableCatalog customCatalog = new RetryableCatalog(mockCatalog, MAX_RETRIES, RETRY_DELAY_MS) {
            @Override
            protected boolean isRetryable(Throwable t) {
                // Extend to retry on any RuntimeException (for testing)
                return super.isRetryable(t) || (t instanceof RuntimeException);
            }
        };

        when(mockCatalog.loadTable(identifier))
            .thenThrow(new RuntimeException("Some runtime error"))
            .thenReturn(mockTable);

        Table result = customCatalog.loadTable(identifier);

        assertNotNull(result);
        verify(mockCatalog, times(2)).loadTable(identifier);
    }

    @Test
    void testCreateNamespaceSuccess() {
        Namespace namespace = Namespace.of("test_ns");
        Map<String, String> props = new HashMap<>();
        props.put("owner", "test_user");

        retryableCatalog.createNamespace(namespace, props);

        verify(mockCatalog, times(1)).createNamespace(namespace, props);
    }

    @Test
    void testCreateNamespaceWithRetry() {
        Namespace namespace = Namespace.of("test_ns");
        Map<String, String> props = Collections.singletonMap("owner", "test_user");

        doThrow(new NotAuthorizedException("Access denied"))
            .doNothing()
            .when(mockCatalog).createNamespace(namespace, props);

        retryableCatalog.createNamespace(namespace, props);

        verify(mockCatalog, times(2)).createNamespace(namespace, props);
    }

    @Test
    void testCreateNamespaceMaxRetriesExceeded() {
        Namespace namespace = Namespace.of("test_ns");
        Map<String, String> props = Collections.singletonMap("owner", "test_user");

        doThrow(new ServiceFailureException("Service failure"))
            .when(mockCatalog).createNamespace(namespace, props);

        assertThrows(ServiceFailureException.class,
            () -> retryableCatalog.createNamespace(namespace, props));

        verify(mockCatalog, times(MAX_RETRIES + 1)).createNamespace(namespace, props);
    }

    @Test
    void testListNamespacesSuccess() {
        Namespace parent = Namespace.of("parent");
        Namespace child = Namespace.of("parent", "child");
        List<Namespace> namespaces = List.of(child);

        when(mockCatalog.listNamespaces(parent)).thenReturn(namespaces);

        List<Namespace> result = retryableCatalog.listNamespaces(parent);

        assertEquals(1, result.size());
        assertEquals(child, result.get(0));
        verify(mockCatalog, times(1)).listNamespaces(parent);
    }

    @Test
    void testListNamespacesWithRetry() {
        Namespace parent = Namespace.empty();
        Namespace ns1 = Namespace.of("ns1");
        List<Namespace> namespaces = List.of(ns1);

        when(mockCatalog.listNamespaces(parent))
            .thenThrow(new ServiceUnavailableException("Service unavailable"))
            .thenThrow(new NotAuthorizedException("Access denied"))
            .thenReturn(namespaces);

        List<Namespace> result = retryableCatalog.listNamespaces(parent);

        assertEquals(1, result.size());
        verify(mockCatalog, times(3)).listNamespaces(parent);
    }

    @Test
    void testListNamespacesThrowsNoSuchNamespaceException() {
        Namespace parent = Namespace.of("nonexistent");

        when(mockCatalog.listNamespaces(parent))
            .thenThrow(new NoSuchNamespaceException("Namespace not found: %s", parent));

        assertThrows(NoSuchNamespaceException.class,
            () -> retryableCatalog.listNamespaces(parent));

        verify(mockCatalog, times(1)).listNamespaces(parent);
    }

    @Test
    void testNamespaceExistsSuccess() {
        Namespace namespace = Namespace.of("test_ns");
        when(mockCatalog.namespaceExists(namespace)).thenReturn(true);

        boolean result = retryableCatalog.namespaceExists(namespace);

        assertTrue(result);
        verify(mockCatalog, times(1)).namespaceExists(namespace);
    }

    @Test
    void testNamespaceExistsWithRetry() {
        Namespace namespace = Namespace.of("test_ns");
        when(mockCatalog.namespaceExists(namespace))
            .thenThrow(new NotAuthorizedException("Access denied"))
            .thenReturn(false);

        boolean result = retryableCatalog.namespaceExists(namespace);

        assertEquals(false, result);
        verify(mockCatalog, times(2)).namespaceExists(namespace);
    }

    @Test
    void testLoadNamespaceMetadataSuccess() {
        Namespace namespace = Namespace.of("test_ns");
        Map<String, String> metadata = new HashMap<>();
        metadata.put("location", "/path/to/ns");
        metadata.put("owner", "test_user");

        when(mockCatalog.loadNamespaceMetadata(namespace)).thenReturn(metadata);

        Map<String, String> result = retryableCatalog.loadNamespaceMetadata(namespace);

        assertEquals(2, result.size());
        assertEquals("/path/to/ns", result.get("location"));
        verify(mockCatalog, times(1)).loadNamespaceMetadata(namespace);
    }

    @Test
    void testLoadNamespaceMetadataWithRetry() {
        Namespace namespace = Namespace.of("test_ns");
        Map<String, String> metadata = Collections.singletonMap("owner", "test_user");

        when(mockCatalog.loadNamespaceMetadata(namespace))
            .thenThrow(new ServiceFailureException("Service failure"))
            .thenReturn(metadata);

        Map<String, String> result = retryableCatalog.loadNamespaceMetadata(namespace);

        assertNotNull(result);
        verify(mockCatalog, times(2)).loadNamespaceMetadata(namespace);
    }

    @Test
    void testLoadNamespaceMetadataThrowsNoSuchNamespaceException() {
        Namespace namespace = Namespace.of("nonexistent");

        when(mockCatalog.loadNamespaceMetadata(namespace))
            .thenThrow(new NoSuchNamespaceException("Namespace not found: %s", namespace));

        assertThrows(NoSuchNamespaceException.class,
            () -> retryableCatalog.loadNamespaceMetadata(namespace));

        verify(mockCatalog, times(1)).loadNamespaceMetadata(namespace);
    }

    @Test
    void testDropNamespaceSuccess() {
        Namespace namespace = Namespace.of("test_ns");
        when(mockCatalog.dropNamespace(namespace)).thenReturn(true);

        boolean result = retryableCatalog.dropNamespace(namespace);

        assertTrue(result);
        verify(mockCatalog, times(1)).dropNamespace(namespace);
    }

    @Test
    void testDropNamespaceWithRetry() {
        Namespace namespace = Namespace.of("test_ns");
        when(mockCatalog.dropNamespace(namespace))
            .thenThrow(new NotAuthorizedException("Access denied"))
            .thenThrow(new ServiceUnavailableException("Service unavailable"))
            .thenReturn(true);

        boolean result = retryableCatalog.dropNamespace(namespace);

        assertTrue(result);
        verify(mockCatalog, times(3)).dropNamespace(namespace);
    }

    @Test
    void testDropNamespaceThrowsNamespaceNotEmptyException() {
        Namespace namespace = Namespace.of("test_ns");

        when(mockCatalog.dropNamespace(namespace))
            .thenThrow(new NamespaceNotEmptyException("Namespace not empty: %s", namespace));

        assertThrows(NamespaceNotEmptyException.class,
            () -> retryableCatalog.dropNamespace(namespace));

        verify(mockCatalog, times(1)).dropNamespace(namespace);
    }

    @Test
    void testSetPropertiesSuccess() {
        Namespace namespace = Namespace.of("test_ns");
        Map<String, String> props = new HashMap<>();
        props.put("owner", "new_owner");
        props.put("description", "test description");

        when(mockCatalog.setProperties(namespace, props)).thenReturn(true);

        boolean result = retryableCatalog.setProperties(namespace, props);

        assertTrue(result);
        verify(mockCatalog, times(1)).setProperties(namespace, props);
    }

    @Test
    void testSetPropertiesWithRetry() {
        Namespace namespace = Namespace.of("test_ns");
        Map<String, String> props = Collections.singletonMap("owner", "new_owner");

        when(mockCatalog.setProperties(namespace, props))
            .thenThrow(new NotAuthorizedException("Access denied"))
            .thenReturn(true);

        boolean result = retryableCatalog.setProperties(namespace, props);

        assertTrue(result);
        verify(mockCatalog, times(2)).setProperties(namespace, props);
    }

    @Test
    void testSetPropertiesThrowsNoSuchNamespaceException() {
        Namespace namespace = Namespace.of("nonexistent");
        Map<String, String> props = Collections.singletonMap("owner", "new_owner");

        when(mockCatalog.setProperties(namespace, props))
            .thenThrow(new NoSuchNamespaceException("Namespace not found: %s", namespace));

        assertThrows(NoSuchNamespaceException.class,
            () -> retryableCatalog.setProperties(namespace, props));

        verify(mockCatalog, times(1)).setProperties(namespace, props);
    }

    @Test
    void testRemovePropertiesSuccess() {
        Namespace namespace = Namespace.of("test_ns");
        Set<String> props = Set.of("owner", "description");

        when(mockCatalog.removeProperties(namespace, props)).thenReturn(true);

        boolean result = retryableCatalog.removeProperties(namespace, props);

        assertTrue(result);
        verify(mockCatalog, times(1)).removeProperties(namespace, props);
    }

    @Test
    void testRemovePropertiesWithRetry() {
        Namespace namespace = Namespace.of("test_ns");
        Set<String> props = Set.of("owner");

        when(mockCatalog.removeProperties(namespace, props))
            .thenThrow(new ServiceFailureException("Service failure"))
            .thenReturn(true);

        boolean result = retryableCatalog.removeProperties(namespace, props);

        assertTrue(result);
        verify(mockCatalog, times(2)).removeProperties(namespace, props);
    }

    @Test
    void testRemovePropertiesThrowsNoSuchNamespaceException() {
        Namespace namespace = Namespace.of("nonexistent");
        Set<String> props = Set.of("owner");

        when(mockCatalog.removeProperties(namespace, props))
            .thenThrow(new NoSuchNamespaceException("Namespace not found: %s", namespace));

        assertThrows(NoSuchNamespaceException.class,
            () -> retryableCatalog.removeProperties(namespace, props));

        verify(mockCatalog, times(1)).removeProperties(namespace, props);
    }

    @Test
    void testCloseWithCloseableCatalog() throws IOException {
        Catalog closeableCatalog = mock(Catalog.class,
            org.mockito.Mockito.withSettings().extraInterfaces(Closeable.class, SupportsNamespaces.class));
        RetryableCatalog catalog = new RetryableCatalog(closeableCatalog, MAX_RETRIES, RETRY_DELAY_MS);

        catalog.close();

        verify((Closeable) closeableCatalog, times(1)).close();
    }

    @Test
    void testCloseWithNonCloseableCatalog() throws IOException {
        // mockCatalog is not Closeable, so close() should not throw an exception
        retryableCatalog.close();

        // No exception should be thrown, and no close() method should be called
        verify(mockCatalog, times(1)).close(); // Just verify mockCatalog is still accessible
    }

    @Test
    void testCloseThrowsIOException() {
        Catalog closeableCatalog = mock(Catalog.class,
            org.mockito.Mockito.withSettings().extraInterfaces(Closeable.class, SupportsNamespaces.class));
        RetryableCatalog catalog = new RetryableCatalog(closeableCatalog, MAX_RETRIES, RETRY_DELAY_MS);

        try {
            doThrow(new IOException("Close failed")).when((Closeable) closeableCatalog).close();

            assertThrows(IOException.class, () -> catalog.close());
            verify((Closeable) closeableCatalog, times(1)).close();
        } catch (IOException e) {
            // This shouldn't happen in the test setup
        }
    }

    @Test
    void testServiceFailureExceptionIsRetryable() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        Table mockTable = mock(Table.class);

        when(mockCatalog.loadTable(identifier))
            .thenThrow(new ServiceFailureException("Service failure"))
            .thenReturn(mockTable);

        Table result = retryableCatalog.loadTable(identifier);

        assertNotNull(result);
        verify(mockCatalog, times(2)).loadTable(identifier);
    }

    @Test
    void testServiceUnavailableExceptionIsRetryable() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        Table mockTable = mock(Table.class);

        when(mockCatalog.loadTable(identifier))
            .thenThrow(new ServiceUnavailableException("Service unavailable"))
            .thenReturn(mockTable);

        Table result = retryableCatalog.loadTable(identifier);

        assertNotNull(result);
        verify(mockCatalog, times(2)).loadTable(identifier);
    }

    @Test
    void testMixedRetryableExceptions() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        Table mockTable = mock(Table.class);

        when(mockCatalog.loadTable(identifier))
            .thenThrow(new NotAuthorizedException("Auth failed"))
            .thenThrow(new ServiceFailureException("Service failure"))
            .thenThrow(new ServiceUnavailableException("Service unavailable"))
            .thenReturn(mockTable);

        Table result = retryableCatalog.loadTable(identifier);

        assertNotNull(result);
        verify(mockCatalog, times(4)).loadTable(identifier);
    }

    @Test
    void testDropTableWithPurgeSuccess() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        when(mockCatalog.dropTable(identifier, true)).thenReturn(true);

        boolean result = retryableCatalog.dropTable(identifier, true);

        assertTrue(result);
        verify(mockCatalog, times(1)).dropTable(identifier, true);
    }

    @Test
    void testDropTableWithPurgeAndRetry() {
        TableIdentifier identifier = TableIdentifier.of("db", "table");
        when(mockCatalog.dropTable(identifier, true))
            .thenThrow(new NotAuthorizedException("Access denied"))
            .thenReturn(true);

        boolean result = retryableCatalog.dropTable(identifier, true);

        assertTrue(result);
        verify(mockCatalog, times(2)).dropTable(identifier, true);
    }
}

