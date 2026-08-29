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
     * @return a future that completes when the logical stream registration is absent
     */
    CompletableFuture<Void> unregisterExternalStream(StreamIdentifier id);

    /**
     * Permanently deletes an externally managed stream identity.
     *
     * <p>Unlike {@link #unregisterExternalStream(StreamIdentifier)}, this operation records a
     * durable deletion fence. All subsequent and already in-flight registrations for the same
     * identifier are suppressed. Use this operation only when the external identity is immutable
     * and will never become valid again; a replacement resource must use a new identifier.
     *
     * @param id the immutable stream identity to delete permanently
     * @return a future that completes when the deletion fence is durable and the registration is
     *     absent
     */
    CompletableFuture<Void> permanentlyDeleteExternalStream(StreamIdentifier id);
}
