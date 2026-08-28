/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Term;

@Data
@AllArgsConstructor
public class IcebergPartitionSpec {
    private PartitionSpec partitionSpec;
    private List<IcebergExpression> expressions;
}

record IcebergExpression(String targetName, Term term) {
}

