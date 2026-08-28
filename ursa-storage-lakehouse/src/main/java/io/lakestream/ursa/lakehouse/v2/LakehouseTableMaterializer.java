/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import io.lakestream.api.materialization.EvolutionPolicy;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.lakehouse.compact.FailureMessage;
import io.lakestream.ursa.materialization.CommitResult;
import io.lakestream.ursa.materialization.MaterializationContext;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.materialization.TableMaterializer;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Adapts the existing {@link AbstractLakehouseWriter} hierarchy to the new
 * {@link TableMaterializer} SPI introduced in T5.
 *
 * <p>The delegate's {@code write} buffers; {@code close} flushes and returns the
 * underlying {@link IWriteResult write results}. This adapter implements
 * {@link #commit()} by calling {@link AbstractLakehouseWriter#close()} and
 * aggregating the returned write results into a {@link CommitResult}. After
 * {@code commit()} the adapter is in a terminal state; a second {@code commit()}
 * or a {@code close()} after commit is a no-op.
 *
 * <p><b>Catalog commit is intentionally NOT done here.</b> The compacted/write
 * results are committed to the external Iceberg/Delta catalog by the existing
 * group-commit pipeline ({@code CompactedTaskRunner} → {@code AbstractCommitRunner}),
 * which reads {@code COMPACTED} tasks from Oxia, checks task status, and applies a
 * batched commit (preserving the upsert delete-before-data ordering). Committing
 * here one task at a time would bypass that machinery. See
 * {@code LakehouseCompactionWorker#completeCompaction} for the persistence step that
 * feeds the runner.
 *
 * <p>{@link ExceptionWithCode} raised by the delegate is re-thrown as an
 * unchecked {@link MaterializationException} preserving the original
 * {@link ExceptionCode} and message so framework code can pattern-match on
 * codes without unwrapping.
 */
@Slf4j
public final class LakehouseTableMaterializer implements TableMaterializer<GenericEntry> {

    private final AbstractLakehouseWriter delegate;
    private final EvolutionPolicy supportedEvolutions;
    /**
     * Dead-letter-table writer for records the delegate fails to serialize (bad/incompatible schema,
     * malformed payload). Registered as the delegate's failure-message handler so failed records are
     * captured instead of dropped. {@code null} for the managed path (no DLT). Closed in
     * {@link #commit()}/{@link #close()}; its results are exposed via {@link #lastDltWriteResults()}.
     */
    @Nullable
    private final LakehouseRecordWriter<FailureMessage> dltWriter;
    /** Set after {@link #commit()} or {@link #close()} so the operation is idempotent. */
    private volatile boolean terminated;
    /**
     * The write results produced by the last {@link #commit()}. The lakehouse materialization
     * service reads these to record them onto the {@code CompactStreamTask} (so the group-commit
     * runner finalizes the catalog snapshot) — this adapter does not commit to the catalog itself.
     */
    private volatile List<IWriteResult> lastWriteResults = Collections.emptyList();
    /** The DLT write results produced by the last {@link #commit()} (empty when no DLT writer). */
    private volatile List<IWriteResult> lastDltWriteResults = Collections.emptyList();

    public LakehouseTableMaterializer(AbstractLakehouseWriter delegate,
                                      EvolutionPolicy supportedEvolutions) {
        this(delegate, supportedEvolutions, null);
    }

    public LakehouseTableMaterializer(AbstractLakehouseWriter delegate,
                                      EvolutionPolicy supportedEvolutions,
                                      @Nullable LakehouseRecordWriter<FailureMessage> dltWriter) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.supportedEvolutions = Objects.requireNonNull(supportedEvolutions, "supportedEvolutions");
        this.dltWriter = dltWriter;
    }

    @Override
    public void write(GenericEntry record, MaterializationContext context) {
        Objects.requireNonNull(record, "record");
        if (context == null) {
            release(record);
            throw new NullPointerException("context");
        }
        if (terminated) {
            try {
                throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                        "write() after commit() or close() is not allowed");
            } finally {
                release(record);
            }
        }
        try {
            delegate.write(record);
        } catch (ExceptionWithCode e) {
            throw new MaterializationException(e.getExceptionCode(),
                    safeMessage(e, "write failed"), e);
        } catch (RuntimeException e) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    safeMessage(e, "write failed"), e);
        }
    }

    @Override
    public CommitResult commit() {
        if (terminated) {
            // Idempotent: a prior commit() has already returned the aggregated result.
            // We return an empty commit so the framework's retry path is a no-op.
            return new CommitResult(0L, 0L, Map.of());
        }
        terminated = true;
        try {
            // Close the main writer first (it flushes data files AND waits for any in-flight
            // failure-message sends to the DLT writer), then close the DLT writer to finalize its
            // files — mirroring LakehouseCompactionWorker's externalWriter→dltWriter close order.
            List<IWriteResult> writeResults = delegate.close();
            this.lastWriteResults = writeResults == null ? Collections.emptyList() : writeResults;
            this.lastDltWriteResults = closeDltWriter();
            return toCommitResult(writeResults);
        } catch (ExceptionWithCode e) {
            throw new MaterializationException(e.getExceptionCode(),
                    safeMessage(e, "commit failed"), e);
        } catch (RuntimeException e) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    safeMessage(e, "commit failed"), e);
        }
    }

    /** Closes the DLT writer (if any) and returns its write results; empty when there is no DLT. */
    private List<IWriteResult> closeDltWriter() {
        if (dltWriter == null) {
            return Collections.emptyList();
        }
        try {
            List<IWriteResult> dltResults = dltWriter.close();
            return dltResults == null ? Collections.emptyList() : dltResults;
        } catch (ExceptionWithCode e) {
            throw new MaterializationException(e.getExceptionCode(),
                    safeMessage(e, "DLT commit failed"), e);
        } catch (RuntimeException e) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    safeMessage(e, "DLT commit failed"), e);
        }
    }

    @Override
    public void close() {
        // commit() is what flushes the delegate; if it has been called the underlying writer is
        // already closed and a second close() would double-close.
        if (terminated) {
            return;
        }
        terminated = true;
        try {
            delegate.close();
            closeDltWriter();
        } catch (ExceptionWithCode e) {
            // Surface close-time failures so callers don't silently lose data.
            throw new MaterializationException(e.getExceptionCode(),
                    safeMessage(e, "close failed"), e);
        } catch (RuntimeException e) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    safeMessage(e, "close failed"), e);
        }
    }

    @Override
    public EvolutionPolicy supportedEvolutions() {
        return supportedEvolutions;
    }

    /** The {@link IWriteResult}s from the last {@link #commit()} (empty before the first commit). */
    public List<IWriteResult> lastWriteResults() {
        return lastWriteResults;
    }

    /**
     * The dead-letter-table {@link IWriteResult}s from the last {@link #commit()} — the files holding
     * records that failed serde. Empty when there is no DLT writer or nothing failed.
     */
    public List<IWriteResult> lastDltWriteResults() {
        return lastDltWriteResults;
    }

    /**
     * Aggregates the {@link IWriteResult} list into a {@link CommitResult}: reports the number of
     * write-result files produced. The actual catalog snapshot commit is performed downstream by
     * the group-commit runner (see the class Javadoc), so no bytes are reported here.
     */
    private CommitResult toCommitResult(List<IWriteResult> writeResults) {
        List<IWriteResult> safe = writeResults == null ? Collections.emptyList() : writeResults;
        Map<String, String> metadata = new HashMap<>();
        metadata.put("writeResultCount", Integer.toString(safe.size()));
        return new CommitResult(0L, 0L, metadata);
    }

    private static String safeMessage(Throwable t, String fallback) {
        String m = t.getMessage();
        return m == null || m.isEmpty() ? fallback : m;
    }

    private static void release(GenericEntry record) {
        if (record.entry() != null && record.entry().payload() != null) {
            record.entry().payload().release();
        }
    }
}
