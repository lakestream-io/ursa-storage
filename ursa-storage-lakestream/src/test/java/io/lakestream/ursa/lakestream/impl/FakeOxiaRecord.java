/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * One Oxia record's conditional-write behaviour, for the tests that stub {@code AsyncOxiaClient}.
 *
 * <p>A test points its mock client's get, put and delete at {@link #applyGet}, {@link #applyPut}
 * and {@link #applyDelete}, which enforce the {@code IfRecordDoesNotExist} and
 * {@code IfVersionIdEquals} conditions the config store depends on and version the record the way
 * Oxia would. {@link #state()} is the record itself, so a test can plant a value or read one back
 * to arrange a race.
 */
final class FakeOxiaRecord {

    /** A stored value and the version Oxia would report for it. */
    record VersionedValue(byte[] value, Version version) {
    }

    private final String path;
    private final AtomicReference<VersionedValue> state;
    private final AtomicLong lastVersionId;

    /** A record starting at version 1 when it has a value, and at no version when it does not. */
    FakeOxiaRecord(String path, byte[] initialValue) {
        this(path, initialValue, initialValue == null ? 0L : 1L);
    }

    /**
     * @param lastVersionId the version the next write increments; the record's own initial value,
     *     if it has one, is at version 1 regardless
     */
    FakeOxiaRecord(String path, byte[] initialValue, long lastVersionId) {
        this.path = path;
        this.state = new AtomicReference<>(initialValue == null
            ? null : new VersionedValue(initialValue.clone(), version(1)));
        this.lastVersionId = new AtomicLong(lastVersionId);
    }

    /** The version Oxia reports for {@code id}; only the version ID is ever significant here. */
    static Version version(long id) {
        return new Version(id, 0, 0, 0, Optional.empty(), Optional.empty());
    }

    /** Wraps a conditional write in the future an {@code AsyncOxiaClient} would hand back. */
    static <T> CompletableFuture<T> settle(Callable<T> write) {
        try {
            return CompletableFuture.completedFuture(write.call());
        } catch (Exception failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    AtomicReference<VersionedValue> state() {
        return state;
    }

    GetResult applyGet() {
        VersionedValue current = state.get();
        return current == null ? null : new GetResult(path, current.value(), current.version());
    }

    /** Applies a put, throwing what Oxia reports when the write's condition does not hold. */
    PutResult applyPut(byte[] value, Set<PutOption> options)
            throws KeyAlreadyExistsException, UnexpectedVersionIdException {
        VersionedValue current = state.get();
        if (options.contains(PutOption.IfRecordDoesNotExist)) {
            if (current != null) {
                throw new KeyAlreadyExistsException(path);
            }
        } else if (current == null || !options.contains(
                PutOption.IfVersionIdEquals(current.version().versionId()))) {
            throw new UnexpectedVersionIdException(
                path, current == null ? -1L : current.version().versionId());
        }
        Version next = version(lastVersionId.incrementAndGet());
        state.set(new VersionedValue(value.clone(), next));
        return new PutResult(path, next);
    }

    /**
     * Applies a delete, throwing what Oxia reports when the version does not match.
     *
     * @return whether there was a record to delete
     */
    boolean applyDelete(Set<DeleteOption> options) throws UnexpectedVersionIdException {
        VersionedValue current = state.get();
        if (current == null) {
            return false;
        }
        if (!options.contains(DeleteOption.IfVersionIdEquals(current.version().versionId()))) {
            throw new UnexpectedVersionIdException(path, current.version().versionId());
        }
        state.set(null);
        return true;
    }
}
