/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.EvolutionPolicy;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.exception.LakehouseOptException;
import io.lakestream.ursa.lakehouse.compact.FailureMessage;
import io.lakestream.ursa.materialization.CommitResult;
import io.lakestream.ursa.materialization.MaterializationContext;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
class LakehouseTableMaterializerTest {

    private AbstractLakehouseWriter writer;
    private GenericEntry entry;
    private MaterializationContext context;

    @BeforeEach
    void setUp() {
        writer = mock(AbstractLakehouseWriter.class);
        entry = mock(GenericEntry.class);
        context = new MaterializationContext(
                StreamIdentifier.of("public/default", "test"),
                42L,
                0L,
                Optional.empty(),
                Map.of());
    }

    @Test
    void writeDelegatesToUnderlyingWriter() throws Exception {
        LakehouseTableMaterializer materializer =
                new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg());

        materializer.write(entry, context);

        verify(writer, times(1)).write(entry);
    }

    @Test
    void commitConvertsWriteResultsToCommitResult() throws Exception {
        IWriteResult r1 = mock(IWriteResult.class);
        IWriteResult r2 = mock(IWriteResult.class);
        when(writer.close()).thenReturn(List.of(r1, r2));

        LakehouseTableMaterializer materializer =
                new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg());

        CommitResult result = materializer.commit();

        assertThat(result.recordsCommitted()).isZero();
        assertThat(result.bytesCommitted()).isZero();
        assertThat(result.sinkMetadata()).containsEntry("writeResultCount", "2");
    }

    @Test
    void commitWithEmptyWriteResultsReportsZeroCount() throws Exception {
        when(writer.close()).thenReturn(List.of());

        LakehouseTableMaterializer materializer =
                new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg());

        CommitResult result = materializer.commit();

        assertThat(result.sinkMetadata()).containsEntry("writeResultCount", "0");
    }

    @Test
    void writeThrowingExceptionWithCodeWrapsInMaterializationException() throws Exception {
        doThrow(new LakehouseOptException(ExceptionCode.LAKEHOUSE_WRITE_ERROR, "boom"))
                .when(writer).write(any());

        LakehouseTableMaterializer materializer =
                new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg());

        assertThatThrownBy(() -> materializer.write(entry, context))
                .isInstanceOf(MaterializationException.class)
                .satisfies(t -> {
                    MaterializationException me = (MaterializationException) t;
                    assertThat(me.getExceptionCode()).isEqualTo(ExceptionCode.LAKEHOUSE_WRITE_ERROR);
                    assertThat(t.getMessage()).contains("boom");
                });
    }

    @Test
    void commitThrowingExceptionWithCodeWrapsInMaterializationException() throws Exception {
        when(writer.close()).thenAnswer(invocation -> {
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_COMMIT_ERROR, "commit-boom");
        });

        LakehouseTableMaterializer materializer =
                new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg());

        assertThatThrownBy(materializer::commit)
                .isInstanceOf(MaterializationException.class)
                .satisfies(t -> {
                    MaterializationException me = (MaterializationException) t;
                    assertThat(me.getExceptionCode()).isEqualTo(ExceptionCode.LAKEHOUSE_COMMIT_ERROR);
                    assertThat(t.getMessage()).contains("commit-boom");
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void commitClosesDltWriterAndExposesItsResults() throws Exception {
        // The external path registers a DLT writer for failed records; commit() must close it
        // (after the main writer) and surface its results so the service can persist them for the
        // group-commit runner.
        IWriteResult dataResult = mock(IWriteResult.class);
        when(writer.close()).thenReturn(List.of(dataResult));
        LakehouseRecordWriter<FailureMessage> dltWriter = mock(LakehouseRecordWriter.class);
        IWriteResult dltResult = mock(IWriteResult.class);
        when(dltWriter.close()).thenReturn(List.of(dltResult));

        LakehouseTableMaterializer materializer =
                new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg(), dltWriter);

        materializer.commit();

        verify(dltWriter).close();
        assertThat(materializer.lastWriteResults()).containsExactly(dataResult);
        assertThat(materializer.lastDltWriteResults()).containsExactly(dltResult);
    }

    @Test
    void noDltWriterYieldsEmptyDltResults() throws Exception {
        when(writer.close()).thenReturn(List.of());

        LakehouseTableMaterializer materializer =
                new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg());

        materializer.commit();

        assertThat(materializer.lastDltWriteResults()).isEmpty();
    }

    @Test
    void supportedEvolutionsReturnsConfiguredPolicy() {
        EvolutionPolicy policy = EvolutionPolicy.forClickHouse();
        LakehouseTableMaterializer materializer = new LakehouseTableMaterializer(writer, policy);

        assertThat(materializer.supportedEvolutions()).isSameAs(policy);
    }

    @Test
    void closeIsIdempotentAfterCommit() throws Exception {
        when(writer.close()).thenReturn(List.of());

        LakehouseTableMaterializer materializer =
                new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg());

        materializer.commit();
        materializer.close();

        // close() on the underlying writer must be called exactly once.
        verify(writer, times(1)).close();
    }

    @Test
    void closeWithoutCommitFlushesTheDelegate() throws Exception {
        when(writer.close()).thenReturn(List.of());

        LakehouseTableMaterializer materializer =
                new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg());

        materializer.close();

        verify(writer, times(1)).close();
    }

    @Test
    void secondCommitIsNoOp() throws Exception {
        when(writer.close()).thenReturn(List.of());

        LakehouseTableMaterializer materializer =
                new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg());

        CommitResult first = materializer.commit();
        CommitResult second = materializer.commit();

        assertThat(first.sinkMetadata()).containsEntry("writeResultCount", "0");
        assertThat(second.sinkMetadata()).isEmpty();
        verify(writer, times(1)).close();
    }

    @Test
    void writeAfterCommitIsRejected() throws Exception {
        when(writer.close()).thenReturn(List.of());
        // also pre-seat write() so we can prove it's not invoked
        doNothing().when(writer).write(any());

        LakehouseTableMaterializer materializer =
                new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg());

        materializer.commit();

        GenericEntry transferred = ownedEntry();
        ByteBuf payload = transferred.entry().payload();
        assertThatThrownBy(() -> materializer.write(transferred, context))
                .isInstanceOf(MaterializationException.class)
                .satisfies(t -> {
                    MaterializationException me = (MaterializationException) t;
                    assertThat(me.getExceptionCode()).isEqualTo(ExceptionCode.INTERNAL_ERROR);
                });
        assertThat(payload.refCnt()).isZero();
        verify(writer, never()).write(any());
    }

    @Test
    void writeAfterCloseReleasesTransferredEntry() throws Exception {
        when(writer.close()).thenReturn(List.of());
        LakehouseTableMaterializer materializer =
                new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg());
        materializer.close();
        GenericEntry transferred = ownedEntry();
        ByteBuf payload = transferred.entry().payload();

        assertThatThrownBy(() -> materializer.write(transferred, context))
                .isInstanceOf(MaterializationException.class);

        assertThat(payload.refCnt()).isZero();
        verify(writer, never()).write(any());
    }

    @Test
    void nullContextReleasesTransferredEntry() throws Exception {
        LakehouseTableMaterializer materializer =
                new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg());
        GenericEntry transferred = ownedEntry();
        ByteBuf payload = transferred.entry().payload();

        assertThatThrownBy(() -> materializer.write(transferred, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("context");

        assertThat(payload.refCnt()).isZero();
        verify(writer, never()).write(any());
    }

    @Test
    void unexpectedRuntimeExceptionOnWriteIsWrapped() throws Exception {
        doAnswer(inv -> {
            throw new IllegalStateException("kapow");
        }).when(writer).write(any());

        LakehouseTableMaterializer materializer =
                new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg());

        assertThatThrownBy(() -> materializer.write(entry, context))
                .isInstanceOf(MaterializationException.class)
                .satisfies(t -> {
                    MaterializationException me = (MaterializationException) t;
                    assertThat(me.getExceptionCode()).isEqualTo(ExceptionCode.INTERNAL_ERROR);
                });
    }

    @Test
    void exceptionCodeAndCauseChainArePreserved() throws Exception {
        Exception cause = new RuntimeException("network");
        ExceptionWithCode original =
                new ExceptionWithCode(ExceptionCode.LAKEHOUSE_WRITE_ERROR, "write failed", cause);
        doThrow(original).when(writer).write(any());

        LakehouseTableMaterializer materializer =
                new LakehouseTableMaterializer(writer, EvolutionPolicy.forIceberg());

        try {
            materializer.write(entry, context);
        } catch (MaterializationException e) {
            assertThat(e.getExceptionCode()).isEqualTo(ExceptionCode.LAKEHOUSE_WRITE_ERROR);
            assertThat(e.getMessage()).contains("write failed");
            // The original ExceptionWithCode is reachable as the cause; its own cause chain
            // surfaces the underlying error.
            assertThat(e.getRealException()).isNotNull();
            assertThat(e.getRealException().getExceptionCode())
                    .isEqualTo(ExceptionCode.LAKEHOUSE_WRITE_ERROR);
            // The original ExceptionWithCode (with the network cause inside) is reachable as
            // the cause of the new MaterializationException's wrapped ExceptionWithCode.
            assertThat(e.getRealException().getCause()).isSameAs(original);
            assertThat(original.getCause()).isSameAs(cause);
        }
    }

    private static GenericEntry ownedEntry() {
        return new GenericEntry(new Entry(
                EntryHeader.NOT_FOUND, Unpooled.buffer().writeByte(1)));
    }
}
