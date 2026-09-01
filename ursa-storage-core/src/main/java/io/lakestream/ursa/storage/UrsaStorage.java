/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogStateManager;
import io.lakestream.api.Position;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.impl.PersistStorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.StorageFormat;
import io.lakestream.ursa.storage.impl.StreamStateManagerImpl;
import io.lakestream.ursa.storage.impl.WalStorageFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Ursa storage runtime with metadata-first, lazy data-plane initialization.
 *
 * <p>Constructing the runtime creates the Oxia-backed {@link StorageApi} facade, but does not
 * create {@link FileStorage}, {@link io.lakestream.ursa.storage.impl.ObjectWalStorageImpl}, or its
 * direct-memory caches. Stream-ID allocation and lifecycle fencing therefore remain metadata-only.
 * The WAL is initialized exactly once when a data-plane operation or {@link #getFileStorage()} is
 * first used.
 */
@Slf4j
public class UrsaStorage implements AutoCloseable {

    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(10);
    private static final long FAILED_DATA_PLANE_CLOSE_INITIAL_RETRY_MILLIS = 100L;
    private static final long FAILED_DATA_PLANE_CLOSE_MAX_RETRY_MILLIS =
        TimeUnit.SECONDS.toMillis(10);
    private static final ExecutorService FAILED_DATA_PLANE_CLOSE_EXECUTOR =
        new ThreadPoolExecutor(
            2, 2, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(64), task -> {
                Thread thread = new Thread(task, "ursa-failed-data-plane-close");
                thread.setDaemon(true);
                thread.setContextClassLoader(UrsaStorage.class.getClassLoader());
                return thread;
            }, new ThreadPoolExecutor.AbortPolicy());

    @FunctionalInterface
    interface DataPlaneFactory {
        DataPlane create(StorageConfig config, InstrumentProvider instrumentProvider,
                         AsyncOxiaClient oxiaClient, StorageFormat storageFormat,
                         LogStateManager streamStateManager) throws Exception;
    }

    record DataPlane(FileStorage fileStorage, WalStorage walStorage) {

        DataPlane {
            Objects.requireNonNull(fileStorage, "fileStorage");
            Objects.requireNonNull(walStorage, "walStorage");
        }
    }

    private final StorageConfig config;
    private final InstrumentProvider instrumentProvider;
    private final StorageFormat storageFormat;
    private final LogStateManager streamStateManager;
    private final DataPlaneFactory dataPlaneFactory;
    private final PersistStorageApi defaultStorageApi;
    private final LazyWalStorage defaultWalStorage;
    private final Object dataPlaneLock = new Object();
    private final long closeTimeoutNanos;

    private final boolean internalCreatedOxia;
    private final AsyncOxiaClient oxiaClient;
    private volatile DataPlane dataPlane;
    private volatile boolean closed;
    private boolean storageApiClosed;
    private boolean walStorageClosed;
    private boolean fileStorageClosed;
    private boolean oxiaClientClosed;
    private boolean resourcesClosed;

    public UrsaStorage(Properties properties, OpenTelemetry otel) throws Exception {
        this(StorageConfig.fromProperties(properties), otel);
    }

    public UrsaStorage(StorageConfig config, OpenTelemetry otel) throws Exception {
        this(config, otel, null);
    }

    public UrsaStorage(StorageConfig config, OpenTelemetry otel,
                       AsyncOxiaClient client) throws Exception {
        this(config, otel, client, UrsaStorage::createDataPlane);
    }

    UrsaStorage(StorageConfig config, OpenTelemetry otel, AsyncOxiaClient client,
                DataPlaneFactory dataPlaneFactory) throws Exception {
        this(config, otel, client, dataPlaneFactory, DEFAULT_CLOSE_TIMEOUT, false);
    }

    UrsaStorage(StorageConfig config, OpenTelemetry otel, AsyncOxiaClient client,
                DataPlaneFactory dataPlaneFactory, Duration closeTimeout,
                boolean ownsProvidedOxiaClient) throws Exception {
        this.config = Objects.requireNonNull(config, "config");
        Objects.requireNonNull(otel, "otel");
        this.dataPlaneFactory = Objects.requireNonNull(dataPlaneFactory, "dataPlaneFactory");
        this.closeTimeoutNanos = positiveDurationNanos(closeTimeout, "closeTimeout");
        this.internalCreatedOxia = client == null || ownsProvidedOxiaClient;
        this.oxiaClient = client == null ? createOxiaClient(config, otel) : client;
        this.instrumentProvider = new InstrumentProvider(otel);
        this.storageFormat = new StorageFormat(config);
        this.streamStateManager = new StreamStateManagerImpl();
        this.defaultWalStorage = new LazyWalStorage();
        this.defaultStorageApi = new PersistStorageApi(
            config, oxiaClient, defaultWalStorage, instrumentProvider,
            storageFormat, streamStateManager);
    }

    private static DataPlane createDataPlane(
            StorageConfig config, InstrumentProvider instrumentProvider,
            AsyncOxiaClient oxiaClient, StorageFormat storageFormat,
            LogStateManager streamStateManager) throws Exception {
        FileStorage fileStorage = FileStorage.create(config, instrumentProvider);
        WalStorage walStorage = null;
        try {
            IDGenerator idGenerator = IDGenerator.create(
                config.getIdGeneratorType(), "wal", oxiaClient);
            walStorage = WalStorageFactory.create(
                WalStorageFactory.Type.SIMPLE, config, PooledByteBufAllocator.DEFAULT,
                fileStorage, idGenerator, instrumentProvider, oxiaClient, storageFormat,
                streamStateManager);
            walStorage.initialize();
            return new DataPlane(fileStorage, walStorage);
        } catch (Exception | Error failure) {
            closeDataPlaneAfterFailure(fileStorage, walStorage, failure);
            throw failure;
        }
    }

    private DataPlane dataPlane() throws Exception {
        synchronized (dataPlaneLock) {
            if (closed || defaultWalStorage.isClosed()) {
                throw new IllegalStateException("Ursa storage runtime is closed");
            }
            if (dataPlane != null) {
                return dataPlane;
            }
            DataPlane created = Objects.requireNonNull(
                dataPlaneFactory.create(config, instrumentProvider, oxiaClient,
                    storageFormat, streamStateManager),
                "dataPlaneFactory returned null");
            dataPlane = created;
            defaultStorageApi.onWALDataPlaneAvailable(created.fileStorage());
            return created;
        }
    }

    private static void closeDataPlaneAfterFailure(
            FileStorage fileStorage, WalStorage walStorage, Throwable failure) {
        closeDataPlaneAfterFailure(
            fileStorage, walStorage, failure, DEFAULT_CLOSE_TIMEOUT);
    }

    static void closeDataPlaneAfterFailure(
            FileStorage fileStorage, WalStorage walStorage, Throwable failure,
            Duration closeTimeout) {
        Objects.requireNonNull(failure, "failure");
        long timeoutNanos = positiveDurationNanos(closeTimeout, "closeTimeout");
        if (walStorage == null) {
            closeFileStorageAfterFailure(fileStorage, failure);
            return;
        }

        FailedDataPlaneCleanup cleanup = new FailedDataPlaneCleanup(
            fileStorage, walStorage, failure);
        CompletableFuture<Void> cleanupFuture = cleanup.start();

        try {
            awaitWalClose(cleanupFuture, System.nanoTime() + timeoutNanos);
        } catch (TimeoutException | InterruptedException pendingClose) {
            addSuppressed(failure, pendingClose);
        } catch (Exception | Error closeFailure) {
            addSuppressed(failure, closeFailure);
        }
    }

    private static long positiveDurationNanos(Duration duration, String name) {
        Objects.requireNonNull(duration, name);
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        try {
            return duration.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(name + " is too large", overflow);
        }
    }

    private static void closeFileStorageAfterFailure(
            FileStorage fileStorage, Throwable failure) {
        if (fileStorage != null) {
            try {
                fileStorage.close();
            } catch (Exception | Error closeFailure) {
                addSuppressed(failure, closeFailure);
            }
        }
    }

    private static final class FailedDataPlaneCleanup {

        private final FileStorage fileStorage;
        private final WalStorage walStorage;
        private final Throwable initializationFailure;
        private final CompletableFuture<Void> completion = new CompletableFuture<>();
        private boolean cleanupFailureRecorded;

        private FailedDataPlaneCleanup(
                FileStorage fileStorage,
                WalStorage walStorage,
                Throwable initializationFailure) {
            this.fileStorage = fileStorage;
            this.walStorage = walStorage;
            this.initializationFailure = initializationFailure;
        }

        private CompletableFuture<Void> start() {
            attemptWalClose(0);
            return completion;
        }

        private void attemptWalClose(int retryAttempt) {
            try {
                FAILED_DATA_PLANE_CLOSE_EXECUTOR.execute(() -> {
                    final CompletableFuture<Void> walClose;
                    try {
                        walClose = Objects.requireNonNull(
                            walStorage.close(), "WAL close future");
                    } catch (Throwable closeFailure) {
                        scheduleWalCloseRetry(retryAttempt, closeFailure);
                        return;
                    }
                    walClose.whenComplete((ignored, closeFailure) -> {
                        if (closeFailure == null) {
                            attemptFileStorageClose(0);
                        } else {
                            scheduleWalCloseRetry(
                                retryAttempt, unwrapCompletionFailure(closeFailure));
                        }
                    });
                });
            } catch (RejectedExecutionException rejection) {
                scheduleWalCloseRetry(retryAttempt, rejection);
            } catch (Throwable schedulingFailure) {
                recordCleanupFailure(schedulingFailure);
                completion.completeExceptionally(schedulingFailure);
            }
        }

        private void attemptFileStorageClose(int retryAttempt) {
            if (fileStorage == null) {
                completion.complete(null);
                return;
            }
            try {
                FAILED_DATA_PLANE_CLOSE_EXECUTOR.execute(() -> {
                    try {
                        fileStorage.close();
                        completion.complete(null);
                    } catch (Throwable closeFailure) {
                        scheduleFileStorageCloseRetry(retryAttempt, closeFailure);
                    }
                });
            } catch (RejectedExecutionException rejection) {
                scheduleFileStorageCloseRetry(retryAttempt, rejection);
            } catch (Throwable schedulingFailure) {
                recordCleanupFailure(schedulingFailure);
                completion.completeExceptionally(schedulingFailure);
            }
        }

        private void scheduleWalCloseRetry(int retryAttempt, Throwable failure) {
            recordCleanupFailure(failure);
            int nextAttempt = nextRetryAttempt(retryAttempt);
            long delayMillis = failedDataPlaneCloseRetryDelayMillis(retryAttempt);
            log.warn("Failed to close WAL after data-plane initialization failure; "
                    + "retrying in {} ms (attempt {})", delayMillis, nextAttempt, failure);
            scheduleRetry(() -> attemptWalClose(nextAttempt), delayMillis, failure);
        }

        private void scheduleFileStorageCloseRetry(int retryAttempt, Throwable failure) {
            recordCleanupFailure(failure);
            int nextAttempt = nextRetryAttempt(retryAttempt);
            long delayMillis = failedDataPlaneCloseRetryDelayMillis(retryAttempt);
            log.warn("Failed to close file storage after WAL cleanup; "
                    + "retrying in {} ms (attempt {})", delayMillis, nextAttempt, failure);
            scheduleRetry(() -> attemptFileStorageClose(nextAttempt), delayMillis, failure);
        }

        private void scheduleRetry(Runnable retry, long delayMillis, Throwable failure) {
            try {
                CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS)
                    .execute(retry);
            } catch (Throwable schedulingFailure) {
                failure.addSuppressed(schedulingFailure);
                recordCleanupFailure(schedulingFailure);
                completion.completeExceptionally(failure);
            }
        }

        private synchronized void recordCleanupFailure(Throwable failure) {
            if (!cleanupFailureRecorded) {
                addSuppressed(initializationFailure, failure);
                cleanupFailureRecorded = true;
            }
        }
    }

    private static int nextRetryAttempt(int retryAttempt) {
        return retryAttempt == Integer.MAX_VALUE
            ? Integer.MAX_VALUE : retryAttempt + 1;
    }

    private static long failedDataPlaneCloseRetryDelayMillis(int retryAttempt) {
        int shift = Math.min(Math.max(retryAttempt, 0), 30);
        long exponentialDelay = FAILED_DATA_PLANE_CLOSE_INITIAL_RETRY_MILLIS << shift;
        return Math.min(exponentialDelay, FAILED_DATA_PLANE_CLOSE_MAX_RETRY_MILLIS);
    }

    private static Throwable unwrapCompletionFailure(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static void addSuppressed(Throwable failure, Throwable closeFailure) {
        if (failure != closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    AsyncOxiaClient createOxiaClient(StorageConfig config, OpenTelemetry otel) throws Exception {
        return OxiaClientFactory.create(
            config.getOxiaStorageUrl(), config.getOxiaStorageConfig(), otel);
    }

    public StorageApi getDefaultStorageApi() {
        return defaultStorageApi;
    }

    public WalStorage getDefaultWalStorage() {
        return defaultWalStorage;
    }

    public FileStorage getFileStorage() {
        try {
            return dataPlane().fileStorage();
        } catch (RuntimeException | Error failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalStateException("Failed to initialize Ursa storage data plane", failure);
        }
    }

    public AsyncOxiaClient getOxiaClient() {
        return oxiaClient;
    }

    public static Pair<String, String> validateOxiaUrl(String metadataURL) throws Exception {
        return OxiaClientFactory.validateOxiaUrl(metadataURL);
    }

    @Override
    public synchronized void close() throws Exception {
        if (resourcesClosed) {
            return;
        }
        long deadlineNanos = System.nanoTime() + closeTimeoutNanos;
        synchronized (dataPlaneLock) {
            if (!closed) {
                closed = true;
                defaultWalStorage.rejectNewOperationsForRuntimeClose();
            }
        }

        if (!storageApiClosed) {
            defaultStorageApi.close();
            storageApiClosed = true;
        }

        if (!walStorageClosed) {
            try {
                awaitWalClose(defaultWalStorage.close(), deadlineNanos);
            } catch (TimeoutException timeout) {
                throw new IOException(
                    "Timed out waiting for WAL operations and storage to close", timeout);
            }
            walStorageClosed = true;
        }

        if (!fileStorageClosed) {
            DataPlane initializedDataPlane;
            synchronized (dataPlaneLock) {
                initializedDataPlane = dataPlane;
            }
            if (initializedDataPlane != null) {
                initializedDataPlane.fileStorage().close();
                synchronized (dataPlaneLock) {
                    if (dataPlane == initializedDataPlane) {
                        dataPlane = null;
                    }
                }
            }
            fileStorageClosed = true;
        }

        if (!oxiaClientClosed) {
            if (internalCreatedOxia) {
                oxiaClient.close();
            }
            oxiaClientClosed = true;
        }
        resourcesClosed = true;
    }

    private static void awaitWalClose(
            CompletableFuture<Void> closeFuture, long deadlineNanos) throws Exception {
        Objects.requireNonNull(closeFuture, "WAL close future");
        try {
            if (closeFuture.isDone()) {
                closeFuture.get();
                return;
            }
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                throw new TimeoutException("Ursa storage runtime close deadline expired");
            }
            closeFuture.get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        } catch (ExecutionException failure) {
            rethrowCloseFailure(failure.getCause());
        }
    }

    private static void rethrowCloseFailure(Throwable failure) throws Exception {
        Throwable cause = failure;
        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof Error error) {
            throw error;
        }
        if (cause instanceof Exception exception) {
            throw exception;
        }
        throw new IllegalStateException("WAL close failed", cause);
    }

    private final class LazyWalStorage implements WalStorage {

        private boolean walClosed;
        private int activeWalOperations;
        private CompletableFuture<Void> walOperationsDrained;
        private CompletableFuture<Void> walCloseFuture;

        private void rejectNewOperationsForRuntimeClose() {
            synchronized (dataPlaneLock) {
                walClosed = true;
            }
        }

        private boolean isClosed() {
            synchronized (dataPlaneLock) {
                return walClosed;
            }
        }

        @Override
        public void initialize() throws Exception {
            walDataPlane();
        }

        @Override
        public CompletableFuture<AddResult> put(
                long id, int numberOfMessages, ByteBuf buf) {
            return withDataPlane(
                walStorage -> walStorage.put(id, numberOfMessages, buf));
        }

        @Override
        public CompletableFuture<AddResult> put(
                long id, int numberOfMessages, long initialOffset,
                long cumulativeSize, ByteBuf buf) {
            return withDataPlane(walStorage -> walStorage.put(
                id, numberOfMessages, initialOffset, cumulativeSize, buf));
        }

        @Override
        public CompletableFuture<AddResult> put(long id, ByteBuf buf) {
            return withDataPlane(walStorage -> walStorage.put(id, buf));
        }

        @Override
        public CompletableFuture<Entry> get(
                long id, long offset, EntryIndex compactedIndex) {
            return withDataPlane(walStorage -> walStorage.get(id, offset, compactedIndex));
        }

        @Override
        @Deprecated
        public CompletableFuture<Entry> get(long id, EntryIndex index) {
            return withDataPlane(walStorage -> walStorage.get(id, index));
        }

        @Override
        public CompletableFuture<Void> get(
                List<EntryIndex> indices, EntryList entryList) {
            return withDataPlane(walStorage -> walStorage.get(indices, entryList));
        }

        @Override
        public void preFetch(long id, List<Position> positions) {
            boolean operationStarted = false;
            try {
                WalStorage walStorage;
                synchronized (dataPlaneLock) {
                    walStorage = beginWalOperation();
                    operationStarted = true;
                }
                walStorage.preFetch(id, positions);
            } catch (RuntimeException | Error failure) {
                throw failure;
            } catch (Exception failure) {
                throw new CompletionException(failure);
            } finally {
                if (operationStarted) {
                    finishWalOperation();
                }
            }
        }

        @Override
        public CompletableFuture<Void> delete(long id, List<Position> positions) {
            return withDataPlane(walStorage -> walStorage.delete(id, positions));
        }

        @Override
        public FileStorage getFileStorage() {
            synchronized (dataPlaneLock) {
                DataPlane existing = dataPlane;
                return walClosed || existing == null ? null : existing.fileStorage();
            }
        }

        @Override
        public CompletableFuture<Void> close() {
            final DataPlane existing;
            final CompletableFuture<Void> drain;
            final CompletableFuture<Void> result;
            synchronized (dataPlaneLock) {
                if (walCloseFuture != null) {
                    return OwnedResultFutures.nonCancellableCompletion(walCloseFuture);
                }
                walClosed = true;
                existing = dataPlane;
                if (existing == null) {
                    walCloseFuture = CompletableFuture.completedFuture(null);
                    return OwnedResultFutures.nonCancellableCompletion(walCloseFuture);
                }
                if (activeWalOperations == 0) {
                    drain = CompletableFuture.completedFuture(null);
                } else {
                    if (walOperationsDrained == null) {
                        walOperationsDrained = new CompletableFuture<>();
                    }
                    drain = walOperationsDrained;
                }
                result = new CompletableFuture<>();
                walCloseFuture = result;
            }
            drain.thenCompose(ignored -> Objects.requireNonNull(
                    existing.walStorage().close(), "WAL close future"))
                .whenComplete((ignored, failure) -> {
                    if (failure == null) {
                        result.complete(null);
                    } else {
                        synchronized (dataPlaneLock) {
                            if (walCloseFuture == result) {
                                walCloseFuture = null;
                            }
                        }
                        result.completeExceptionally(failure);
                    }
                });
            return OwnedResultFutures.nonCancellableCompletion(result);
        }

        private <T> CompletableFuture<T> withDataPlane(
                WalOperation<T> operation) {
            final CompletableFuture<T> source;
            try {
                synchronized (dataPlaneLock) {
                    WalStorage walStorage = beginWalOperation();
                    try {
                        source = Objects.requireNonNull(
                            operation.apply(walStorage), "WAL operation returned null");
                    } catch (Exception | Error failure) {
                        finishWalOperation();
                        throw failure;
                    }
                }
            } catch (Exception | Error failure) {
                return CompletableFuture.failedFuture(failure);
            }
            source.whenComplete((ignored, failure) -> finishWalOperation());
            return OwnedResultFutures.nonCancellableCompletion(source);
        }

        private WalStorage beginWalOperation() throws Exception {
            WalStorage walStorage = walDataPlane().walStorage();
            activeWalOperations++;
            return walStorage;
        }

        private void finishWalOperation() {
            CompletableFuture<Void> drained = null;
            synchronized (dataPlaneLock) {
                if (activeWalOperations == 0) {
                    return;
                }
                activeWalOperations--;
                if (activeWalOperations == 0 && walOperationsDrained != null) {
                    drained = walOperationsDrained;
                    walOperationsDrained = null;
                }
            }
            if (drained != null) {
                drained.complete(null);
            }
        }

        private DataPlane walDataPlane() throws Exception {
            synchronized (dataPlaneLock) {
                if (walClosed) {
                    throw new IllegalStateException("WAL storage facade is closed");
                }
                return dataPlane();
            }
        }
    }

    @FunctionalInterface
    private interface WalOperation<T> {
        CompletableFuture<T> apply(WalStorage walStorage);
    }
}
