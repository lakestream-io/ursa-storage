/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReferencedCatalogTest {

    private Catalog mockCatalog;
    private ReferencedCatalog referencedCatalog;
    private Duration maxOpenTime;

    @BeforeEach
    void setUp() {
        mockCatalog = mock(Catalog.class, withSettings().extraInterfaces(Closeable.class));
        when(mockCatalog.name()).thenReturn("test-catalog");
        maxOpenTime = Duration.ofMillis(100); // Short duration for testing
        referencedCatalog = new ReferencedCatalog(mockCatalog, maxOpenTime);
    }

    @Test
    void testInitialState() {
        assertEquals(0, referencedCatalog.getRefCount());
        assertFalse(referencedCatalog.isClosed());
        assertEquals(mockCatalog, referencedCatalog.getCatalog());
    }

    @Test
    void testRetainAndRelease() {
        // Test retain increments ref count
        referencedCatalog.retain();
        assertEquals(1, referencedCatalog.getRefCount());

        referencedCatalog.retain();
        assertEquals(2, referencedCatalog.getRefCount());

        // Test release decrements ref count
        referencedCatalog.release();
        assertEquals(1, referencedCatalog.getRefCount());

        referencedCatalog.release();
        assertEquals(0, referencedCatalog.getRefCount());
    }

    @Test
    void testReleaseWithNegativeRefCount() {
        // Release without retain should throw exception
        assertThrows(IllegalStateException.class, () -> referencedCatalog.release());
    }

    @Test
    void testIsExpired() throws InterruptedException {
        // Initially not expired
        assertFalse(referencedCatalog.isExpired());

        // Wait for expiration
        Thread.sleep(maxOpenTime.toMillis() + 10);
        assertTrue(referencedCatalog.isExpired());
    }

    @Test
    void testReleaseClosesWhenExpiredAndRefCountZero() throws InterruptedException, IOException {
        // Retain and then wait for expiration
        referencedCatalog.retain();
        Thread.sleep(maxOpenTime.toMillis() + 10);
        assertTrue(referencedCatalog.isExpired());

        // Release should close the catalog when ref count reaches 0 and catalog is expired
        referencedCatalog.release();
        verify((Closeable) mockCatalog).close();
        assertTrue(referencedCatalog.isClosed());
    }

    @Test
    void testReleaseDoesNotCloseWhenNotExpired() throws IOException {
        // Retain and release immediately (not expired)
        referencedCatalog.retain();
        referencedCatalog.release();

        // Should not close because not expired
        verify((Closeable) mockCatalog, never()).close();
        assertFalse(referencedCatalog.isClosed());
    }

    @Test
    void testReleaseDoesNotCloseWhenRefCountPositive() throws InterruptedException, IOException {
        // Multiple retains
        referencedCatalog.retain();
        referencedCatalog.retain();

        // Wait for expiration
        Thread.sleep(maxOpenTime.toMillis() + 10);
        assertTrue(referencedCatalog.isExpired());

        // Release once - ref count still positive
        referencedCatalog.release();
        assertEquals(1, referencedCatalog.getRefCount());

        // Should not close because ref count > 0
        verify((Closeable) mockCatalog, never()).close();
        assertFalse(referencedCatalog.isClosed());
    }

    @Test
    void testSafeCloseWhenRefCountZero() throws IOException {
        // Should close when ref count is 0
        referencedCatalog.safeClose();
        verify((Closeable) mockCatalog).close();
        assertTrue(referencedCatalog.isClosed());
    }

    @Test
    void testSafeCloseWhenRefCountPositive() throws IOException {
        // Retain to make ref count positive
        referencedCatalog.retain();

        // Should not close when ref count > 0
        referencedCatalog.safeClose();
        verify((Closeable) mockCatalog, never()).close();
        assertFalse(referencedCatalog.isClosed());
    }

    @Test
    void testDirectClose() throws IOException {
        // Direct close should always close regardless of ref count
        referencedCatalog.retain();
        referencedCatalog.retain();
        assertEquals(2, referencedCatalog.getRefCount());

        referencedCatalog.close();
        verify((Closeable) mockCatalog).close();
        assertTrue(referencedCatalog.isClosed());
    }

    @Test
    void testCloseIdempotency() throws IOException {
        // Close multiple times should only close catalog once
        referencedCatalog.close();
        referencedCatalog.close();
        referencedCatalog.close();

        verify((Closeable) mockCatalog, times(1)).close();
    }

    @Test
    void testCloseWithIOException() throws IOException {
        // Setup mock to throw IOException
        doThrow(new IOException("Close failed")).when((Closeable) mockCatalog).close();

        // Close should not throw exception
        referencedCatalog.close();
        assertTrue(referencedCatalog.isClosed());
    }

    @Test
    void testNonCloseableCatalog() {
        // Create catalog without Closeable interface
        Catalog nonCloseableCatalog = mock(Catalog.class);
        when(nonCloseableCatalog.name()).thenReturn("non-closeable");
        ReferencedCatalog refCatalog = new ReferencedCatalog(nonCloseableCatalog, Duration.ofMinutes(1));

        // Close should not throw exception
        refCatalog.close();
        assertTrue(refCatalog.isClosed());
    }

    @Test
    void testCreationTimeTracking() {
        long beforeCreation = System.nanoTime();
        ReferencedCatalog catalog = new ReferencedCatalog(mockCatalog, Duration.ofMinutes(1));
        long afterCreation = System.nanoTime();

        assertTrue(catalog.getCreationNanos() >= beforeCreation);
        assertTrue(catalog.getCreationNanos() <= afterCreation);
    }
}