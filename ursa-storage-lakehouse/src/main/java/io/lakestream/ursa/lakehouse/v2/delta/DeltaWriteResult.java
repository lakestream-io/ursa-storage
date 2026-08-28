/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.delta;

import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class DeltaWriteResult implements IWriteResult {

    private List<ParquetFileStat> writeResult;

}
