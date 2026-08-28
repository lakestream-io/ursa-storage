/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
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

/**
 * A wrapper around Iceberg Catalog that adds retry logic for NotAuthorizedException.
 * This wrapper retries catalog operations when NotAuthorizedException is thrown,
 * allowing for transient authorization issues to be automatically handled.
 */
@Slf4j
public class RetryableCatalog implements Catalog, SupportsNamespaces, Closeable {
    private final Catalog delegate;
    private final SupportsNamespaces nsDelegate;
    private final int maxRetries;
    private final long retryDelayMs;

    /**
     * Creates a new RetryableCatalog wrapper.
     *
     * @param delegate the catalog to wrap
     * @param maxRetries the maximum number of retry attempts for NotAuthorizedException
     * @param retryDelayMs the delay in milliseconds between retry attempts
     */
    public RetryableCatalog(Catalog delegate, int maxRetries, long retryDelayMs) {
        this.delegate = delegate;
        this.nsDelegate = (SupportsNamespaces) delegate;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
        delegate.initialize(name, properties);
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        return retry(() -> delegate.listTables(namespace), "listTables");
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
        return retry(() -> delegate.loadTable(identifier), "loadTable");
    }

    @Override
    public TableBuilder buildTable(TableIdentifier identifier, org.apache.iceberg.Schema schema) {
        return delegate.buildTable(identifier, schema);
    }

    @Override
    public boolean tableExists(TableIdentifier identifier) {
        return retry(() -> delegate.tableExists(identifier), "tableExists");
    }

    @Override
    public boolean dropTable(TableIdentifier identifier) {
        return retry(() -> delegate.dropTable(identifier), "dropTable");
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        return retry(() -> delegate.dropTable(identifier, purge), "dropTable with purge");
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
        retryVoid(() -> delegate.renameTable(from, to), "renameTable");
    }

    @Override
    public void invalidateTable(TableIdentifier identifier) {
        retryVoid(() -> delegate.invalidateTable(identifier), "invalidateTable");
    }

    @Override
    public void createNamespace(Namespace ns, Map<String, String> props) {
        retryVoid(() -> nsDelegate.createNamespace(ns, props), "createNamespace");
    }

    @Override
    public List<Namespace> listNamespaces(Namespace ns) throws NoSuchNamespaceException {
        return retry(() -> nsDelegate.listNamespaces(ns), "listNamespaces");
    }

    @Override
    public boolean namespaceExists(Namespace namespace) {
        return retry(() -> nsDelegate.namespaceExists(namespace), "namespaceExists");
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace ns) throws NoSuchNamespaceException {
        return retry(() -> nsDelegate.loadNamespaceMetadata(ns), "loadNamespaceMetadata");
    }

    @Override
    public boolean dropNamespace(Namespace ns) throws NamespaceNotEmptyException {
        return retry(() -> nsDelegate.dropNamespace(ns), "dropNamespace");
    }

    @Override
    public boolean setProperties(Namespace ns, Map<String, String> props)
        throws NoSuchNamespaceException {
        return retry(() -> nsDelegate.setProperties(ns, props), "setProperties");
    }

    @Override
    public boolean removeProperties(Namespace ns, Set<String> props) throws NoSuchNamespaceException {
        return retry(() -> nsDelegate.removeProperties(ns, props), "removeProperties");
    }

    @Override
    public void close() throws IOException {
        if (delegate instanceof Closeable closeableDelegate) {
            closeableDelegate.close();
        }
    }

    /**
     * Executes a callable with retry logic for retryable exceptions.
     *
     * @param callable the operation to execute
     * @param operationName the name of the operation for logging
     * @param <T> the return type
     * @return the result of the operation
     * @throws RuntimeException if the operation fails after all retries
     */
    private <T> T retry(Callable<T> callable, String operationName) {
        return retryWithResult(() -> {
            return callable.call();
        }, operationName);
    }

    /**
     * Executes a void operation with retry logic for retryable exceptions.
     *
     * @param runnable the operation to execute
     * @param operationName the name of the operation for logging
     * @throws RuntimeException if the operation fails after all retries
     */
    private void retryVoid(Runnable runnable, String operationName) {
        retryWithResult(() -> {
            runnable.run();
            return null;
        }, operationName);
    }

    /**
     * Common retry logic that handles both void and value-returning operations.
     *
     * @param supplier the operation to execute
     * @param operationName the name of the operation for logging
     * @param <T> the return type
     * @return the result of the operation
     * @throws RuntimeException if the operation fails after all retries
     */
    private <T> T retryWithResult(Callable<T> supplier, String operationName) {
        int attempt = 0;
        while (true) {
            try {
                return supplier.call();
            } catch (Throwable e) {
                if (!isRetryable(e)) {
                    throwAsRuntimeException(e);
                }
                attempt++;
                if (attempt > maxRetries) {
                    log.error(
                        "Operation '{}' failed after {} retries due to retryable error",
                        operationName, maxRetries
                    );
                    throwAsRuntimeException(e);
                }
                log.warn(
                    "Operation '{}' failed with retryable error (attempt {}/{}), retrying...",
                    operationName, attempt, maxRetries
                );
                sleepWithInterruptHandling(operationName);
            }
        }
    }

    /**
     * Throws the exception as a RuntimeException, preserving the original if applicable.
     *
     * @param e the exception to throw
     */
    private void throwAsRuntimeException(Throwable e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        throw new RuntimeException(e);
    }

    /**
     * Sleep for the retry delay, handling interruptions appropriately.
     *
     * @param operationName the name of the operation for logging
     */
    private void sleepWithInterruptHandling(String operationName) {
        try {
            Thread.sleep(retryDelayMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error("Retry interrupted for operation '{}'", operationName);
            throw new RuntimeException("Retry interrupted", ie);
        }
    }

    /**
     * Determine whether an exception is retryable.
     * Currently only NotAuthorizedException is retryable, but this method
     * centralizes the decision to simplify future extensions.
     */
    protected boolean isRetryable(Throwable t) {
        return t instanceof NotAuthorizedException
               || t instanceof ServiceFailureException
               || t instanceof ServiceUnavailableException;
    }

    /**
     * Get the underlying wrapped catalog.
     *
     * @return the wrapped catalog instance
     */
    public Catalog getDelegate() {
        return delegate;
    }
}

