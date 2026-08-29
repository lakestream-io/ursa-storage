/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Metadata-only registry for logical streams whose lifecycle is controlled by an external system.
 *
 * <p>This interface deliberately excludes data-plane operations so an external control plane can
 * register stream metadata without loading a complete {@link StreamCatalog} implementation and its
 * storage runtime.
 */
public interface ExternalStreamRegistry extends AutoCloseable {

    /**
     * Idempotently registers a logical stream whose partition lifecycle is externally controlled.
     *
     * <p>This operation records the complete logical partition count and initial properties, but
     * does not create partition logs or partition metadata. If the stream is already registered,
     * its partition count may grow but never shrinks; its existing properties (including an empty
     * map) and any materialization policy are preserved. The returned future fails if the identity
     * was previously deleted with {@link #permanentlyDeleteExternalStream(StreamIdentifier)}.
     *
     * @param id the partition-stripped stream identity
     * @param partitionCount the complete logical partition count, which must be positive
     * @param properties stream properties to use when first registering the stream
     * @return a future that completes when the logical stream is registered
     * @throws io.lakestream.api.exception.AlreadyExistsException if the identifier belongs to a
     *     native stream or an incompatible registration lifecycle
     * @throws io.lakestream.api.exception.StreamPermanentlyDeletedException if the identifier has
     *     a durable permanent-deletion fence
     */
    CompletableFuture<Void> registerExternalStream(StreamIdentifier id, int partitionCount,
                                                   Map<String, String> properties);

    /**
     * Idempotently unregisters a logical stream whose lifecycle is externally controlled.
     *
     * <p>This operation removes only the logical stream registration. It does not delete partition
     * metadata, partition logs, or their data. The same identifier may be registered again later;
     * use {@link #permanentlyDeleteExternalStream(StreamIdentifier)} when an immutable external
     * identity must never be revived by a stale registration.
     *
     * @param id the partition-stripped stream identity
     * @return a future that completes when a durable unregistered state is recorded
     * @throws io.lakestream.api.exception.AlreadyExistsException if the identifier belongs to a
     *     native stream or has an in-progress external provisioning claim
     */
    CompletableFuture<Void> unregisterExternalStream(StreamIdentifier id);

    /**
     * Permanently deletes an externally managed stream identity.
     *
     * <p>Unlike {@link #unregisterExternalStream(StreamIdentifier)}, this operation retains a
     * durable deletion-tombstone record. Registrations that observe the fence are rejected. Any
     * partition metadata, keyed mappings, or logs written before the fence became visible are
     * cleaned through the partition lifecycle and are not synchronously rolled back by this
     * metadata-only operation. The tombstone may retain the prior partition count and properties as
     * cleanup context. Use this operation only when the external identity is immutable and will
     * never become valid again; a replacement resource must use a new identifier. Permanent
     * deletion also fences an in-progress external provisioning claim, providing a recovery path
     * when its provisioner has crashed. A stale claimant cannot finalize after this method
     * completes.
     *
     * @param id the immutable stream identity to delete permanently
     * @return a future that completes when the retained deletion fence is durable
     * @throws io.lakestream.api.exception.AlreadyExistsException if the identifier belongs to a
     *     native stream
     * @throws UnsupportedOperationException if the registry implementation does not support
     *     permanent deletion
     */
    default CompletableFuture<Void> permanentlyDeleteExternalStream(StreamIdentifier id) {
        throw new UnsupportedOperationException(
            "Permanent external stream deletion is not supported by this registry");
    }
}
