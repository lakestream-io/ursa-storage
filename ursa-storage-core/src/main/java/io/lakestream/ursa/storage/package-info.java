/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * The io.lakestream.ursa.storage package provides a high-performance, scalable storage system
 * for the Ursa engine, a lakehouse-native stream storage platform designed for
 * cloud and lakehouse environments. This package implements Ursa's stream storage, optimized for
 * high-throughput streaming on object storage with capabilities to compact data into lakehouse
 * formats such as Delta Lake and S3.
 *
 * Key components and features of this package include:
 *
 * 1. Layered Architecture:
 *    - StorageApi: High-level interface for client interactions, offering stream-based operations.
 *    - WalStorage: Intermediate layer implementing Write-Ahead Logging for data durability and fast writes.
 *    - FileStorage: Low-level interface for direct interaction with various storage backends.
 *
 * 2. Flexible Storage Backend Support:
 *    The package supports multiple storage backends, including:
 *    - Local file systems
 *    - Amazon S3
 *    - Google Cloud Storage (GCS)
 *    - Azure Blob Storage
 *    This flexibility allows for deployment in various cloud environments and on-premises setups.
 *
 * 3. High-Performance Design:
 *    - Asynchronous operations for improved throughput
 *    - Buffering and batching in WalStorage to reduce I/O operations
 *    - Composite caching mechanism for faster data access
 *
 * 4. Data Compaction:
 *    Built-in support for compacting data into lakehouse formats, enabling efficient long-term
 *    storage and analytics capabilities.
 *
 * 5. Comprehensive Metrics and Monitoring:
 *    Extensive instrumentation using both OpenTelemetry and Prometheus, allowing detailed
 *    performance tracking and analysis of various storage operations.
 *
 * 6. Key Components:
 *    - {@link io.lakestream.ursa.storage.StorageApi}: Main interface for storage operations.
 *    - {@link io.lakestream.ursa.storage.WalStorage}: Write-Ahead Log storage interface.
 *    - {@link io.lakestream.ursa.storage.FileStorage}: Interface for file-based storage operations.
 *    - {@link io.lakestream.ursa.storage.Key}: Represents a unique identifier for entries in the storage system.
 *    - {@link io.lakestream.ursa.storage.impl.PersistStorageApi}: Primary implementation of StorageApi.
 *    - {@link io.lakestream.ursa.storage.impl.ObjectWalStorageImpl}: Implementation of WalStorage.
 *    - {@link io.lakestream.ursa.storage.impl.LocalFileStorage}: Implementation for local file system storage.
 *    - {@link io.lakestream.ursa.storage.impl.S3FileStorage}: Implementation for Amazon S3 storage.
 *
 * 7. Error Handling and Exceptions:
 *    Custom exceptions for various storage-related errors, ensuring proper error handling and reporting.
 *
 * 8. Thread Safety:
 *    Implementations are designed to be thread-safe, allowing concurrent access in multi-threaded environments.
 *
 * This package is central to URSA's ability to handle high-volume data streams efficiently,
 * providing a robust foundation for building scalable, cloud-native streaming applications with
 * seamless integration into modern data lakehouse architectures.
 *
 * @see io.lakestream.ursa.storage.StorageApi
 * @see io.lakestream.ursa.storage.WalStorage
 * @see io.lakestream.ursa.storage.FileStorage
 * @see io.lakestream.ursa.storage.Key
 * @see io.lakestream.ursa.storage.impl.PersistStorageApi
 * @see io.lakestream.ursa.storage.impl.ObjectWalStorageImpl
 * @see io.lakestream.ursa.storage.impl.LocalFileStorage
 * @see io.lakestream.ursa.storage.impl.S3FileStorage
 */
package io.lakestream.ursa.storage;
