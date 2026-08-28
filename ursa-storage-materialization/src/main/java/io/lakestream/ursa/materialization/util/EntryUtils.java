/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.util;

import io.lakestream.api.LogEntry;
import io.lakestream.ursa.materialization.serde.EntryFormat;
import io.lakestream.ursa.materialization.serde.KafkaEntry;
import io.lakestream.ursa.materialization.serde.kafka.KafkaStorageEntryDecoder;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntryUtils {

    public static void entryToKafkaMessage(Entry entry,
                                           FormatRecordProcessor<KafkaMessage> processor) throws Exception {
        entryToKafkaMessage(entry, EntryFormat.URSA, processor);
    }

    public static void entryToKafkaMessage(Entry entry, EntryFormat entryFormat,
                                           FormatRecordProcessor<KafkaMessage> processor) throws Exception {
        entryPayloadToKafkaMessage(entry.payload(), entry.header().offset(), entry.header().numberOfMessages(),
                entry.header(), entryFormat, processor);
    }

    public static void entryToKafkaMessage(LogEntry entry,
                                           FormatRecordProcessor<KafkaMessage> processor) throws Exception {
        entryToKafkaMessage(entry, EntryFormat.URSA, processor);
    }

    public static void entryToKafkaMessage(LogEntry entry, EntryFormat entryFormat,
                                           FormatRecordProcessor<KafkaMessage> processor) throws Exception {
        entryPayloadToKafkaMessage(entry.payload(), entry.offset(), entry.numberOfRecords(),
                entry.offset(), entryFormat, processor);
    }

    private static void entryPayloadToKafkaMessage(ByteBuf messagePayload,
                                                   long startOffset,
                                                   int numberOfRecords,
                                                   Object logContext,
                                                   EntryFormat entryFormat,
                                                   FormatRecordProcessor<KafkaMessage> processor) throws Exception {
        try {
            if (entryFormat == EntryFormat.KAFKA) {
                KafkaEntry entry = KafkaEntry.fromByteBuf(messagePayload.duplicate());
                processor.handleRecord(new KafkaMessage(startOffset, entry.key(), entry.value()));
                return;
            }
            for (KafkaMessage message : KafkaStorageEntryDecoder.decode(
                    messagePayload.duplicate(), startOffset, numberOfRecords)) {
                processor.handleRecord(message);
            }
        } catch (Exception e) {
            log.error("Failed to parse entry: {} ", logContext, e);
            throw e;
        }
    }
}
