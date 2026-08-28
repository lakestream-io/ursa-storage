/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.iceberg;

import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.iceberg.io.WriteResult;

@AllArgsConstructor
@Getter
public class IcebergWriteResult implements IWriteResult {
    WriteResult writeResult;
}
