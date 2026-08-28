/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.impl.AzureFileStorage;
import io.lakestream.ursa.storage.impl.GCSFileStorage;
import io.lakestream.ursa.storage.impl.LocalFileStorage;
import io.lakestream.ursa.storage.impl.S3FileStorage;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.exception.BackendStorageException;
import io.lakestream.ursa.storage.impl.exception.FileStorageException;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * FileStorage interface defines operations for a file-based storage system.
 * It provides methods for reading, writing, and managing files in various storage backends,
 * supporting local file systems and cloud storage services like S3.
 */
public interface FileStorage extends AutoCloseable {

    String LIFECYCLE_RULE_ID_PREFIX = "ursa-wal-delete-";

    /**
     * Enumeration of supported storage backend types.
     */
    enum Type {
        LOCAL,
        S3,
        GCS,
        AZUREBLOB
    }

    /**
     * Creates a FileStorage instance based on the provided configuration.
     *
     * @param config Configuration for the storage backend.
     * @param instrumentProvider Provider for metrics instrumentation.
     * @return A FileStorage instance.
     * @throws BackendStorageException.NotSupportException if the storage type is not supported.
     */
    static FileStorage create(StorageConfig config, InstrumentProvider instrumentProvider)
        throws BackendStorageException.NotSupportException {
        return create(Type.valueOf(config.getBackendStorageType().toUpperCase(Locale.ROOT)),
            config, instrumentProvider);
    }

    /**
     * Creates a FileStorage instance based on the specified type and configuration.
     *
     * @param type Type of storage backend to create.
     * @param config Configuration for the storage backend.
     * @param instrumentProvider Provider for metrics instrumentation.
     * @return A FileStorage instance.
     * @throws BackendStorageException.NotSupportException if the storage type is not supported.
     */
    static FileStorage create(Type type, StorageConfig config, InstrumentProvider instrumentProvider)
        throws BackendStorageException.NotSupportException {
        switch (type) {
            case LOCAL -> {
                return new LocalFileStorage(config, instrumentProvider);
            }
            case S3 -> {
                return new S3FileStorage(config, instrumentProvider);
            }
            case GCS -> {
                return new GCSFileStorage(config, instrumentProvider);
            }
            case AZUREBLOB -> {
                return new AzureFileStorage(config, instrumentProvider);
            }
            default -> throw new BackendStorageException.NotSupportException();
        }
    }

    /**
     * Constant key for creation time metadata.
     */
    String METADATA_CREATION_TIME = "creationTime";

    /**
     * Stores data at the specified location.
     *
     * @param data The data to store.
     * @param location The location to store the data.
     * @throws IOException if an I/O error occurs.
     */
    default void put(ByteBuf data, String location) throws IOException {
        try {
            putAsync(data, location).get();
        } catch (Exception e) {
            final String errorMsg = "Failed to put object with the key " + location;
            if (e instanceof ExecutionException) {
                throw new FileStorageException(errorMsg, e.getCause());
            } else {
                throw new FileStorageException(errorMsg, e);
            }
        }
    }


    CompletableFuture<Void> putAsync(ByteBuf data, String location);


    /**
     * Retrieves data from the specified location.
     *
     * @param location The location to retrieve data from.
     * @return The retrieved data.
     * @throws IOException if an I/O error occurs.
     */
    default ByteBuf get(String location) throws IOException {
        try {
            return getAsync(location).get();
        } catch (Exception e) {
            final String errorMsg = "Failed to get object with the key " + location;
            if (e instanceof ExecutionException) {
                throw new FileStorageException(errorMsg, e.getCause());
            } else {
                throw new FileStorageException(errorMsg, e);
            }
        }
    }

    /**
     * Retrieves data from the specified location asynchronously.
     *
     * @param location
     * @return A future that will be completed with the retrieved data.
     */
    CompletableFuture<ByteBuf> getAsync(String location);

    /**
     * Deletes data at the specified location.
     *
     * @param location The location to delete data from.
     * @throws IOException if an I/O error occurs.
     */
    void delete(String location) throws IOException;

    /**
     * Deletes multiple files in batch to improve performance.
     * The location should be the full path of the file to delete.
     *
     * @param locations List of file locations to delete
     * @throws IOException if deletion fails
     */
    CompletableFuture<Void> deleteAsync(List<String> locations);

    /**
     * Using lifecycle to delete data with the specified prefix.
     *
     * @param prefixes The prefixes of the data to delete.
     * @return A future that will be completed when the deletion is finished.
     */
    default CompletableFuture<Void> deleteWithDatePrefixes(Set<String> prefixes) throws IOException {
        throw new UnsupportedOperationException("deleteWithLifeCycleUsingPrefixes is not supported");
    }

}
