/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import java.util.Iterator;

public interface EntryDecoder<T> {

    void decode(String topic, Iterator<MaterializationRecord<T>> entry, ResultConsumer<GenericEntry> consumer);

}
