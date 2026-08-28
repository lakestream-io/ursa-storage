/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toSet;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;

@Slf4j
public class Utilities {

  public static TaskWriter<Record> createTableWriter(
          Table table, Schema schema, int partitionId, IcebergSinkConfig config) {
      if (log.isDebugEnabled()) {
          log.debug("Creating table writer for table {} with the schema {}", table.name(), schema);
      }
    Map<String, String> tableProps = Maps.newHashMap(table.properties() == null ? Map.of() : table.properties());
    tableProps.putAll(config.getWriteProps());

    String formatStr = tableProps.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat format = FileFormat.valueOf(formatStr.toUpperCase(Locale.ROOT));

    long targetFileSize =
        PropertyUtil.propertyAsLong(
            tableProps, WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    Set<Integer> identifierFieldIds = schema.identifierFieldIds();

    // override the identifier fields if the config is set
    List<String> idCols = config.getPrimaryKeys();
    if (!idCols.isEmpty()) {
      identifierFieldIds =
          idCols.stream()
              .filter(colName -> {
                Types.NestedField field = schema.findField(colName);
                return field != null && field.isRequired();
              })
              .map(colName -> schema.findField(colName).fieldId())
              .collect(toSet());
    }

    FileAppenderFactory<Record> appenderFactory;
    if (identifierFieldIds == null || identifierFieldIds.isEmpty()) {
      appenderFactory =
          new GenericAppenderFactory(schema, table.spec(), null, null, null)
              .setAll(tableProps);
    } else {
      appenderFactory =
          new GenericAppenderFactory(
                  schema,
                  table.spec(),
                  Ints.toArray(identifierFieldIds),
                  TypeUtil.select(schema, Sets.newHashSet(identifierFieldIds)),
                  null)
              .setAll(tableProps);
    }

    // (partition ID + task ID + operation ID) must be unique
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, partitionId, System.currentTimeMillis())
            .defaultSpec(table.spec())
            .operationId(UUID.randomUUID().toString())
            .format(format)
            .build();

    TaskWriter<Record> writer;
    if (table.spec().isUnpartitioned()) {
      if (identifierFieldIds == null
          || identifierFieldIds.isEmpty()
          || (config.getCdcField() == null && !config.isUpsertModeEnabled())) {
        writer =
            new UnpartitionedWriter<>(
                table.spec(), format, appenderFactory, fileFactory, table.io(), targetFileSize);
      } else {
        writer =
            new UnpartitionedDeltaWriter(
                table.spec(),
                format,
                appenderFactory,
                fileFactory,
                table.io(),
                targetFileSize,
                schema,
                identifierFieldIds,
                config.isUpsertModeEnabled());
      }
    } else {
      if (identifierFieldIds == null
          || identifierFieldIds.isEmpty()
          || (config.getCdcField() == null && !config.isUpsertModeEnabled())) {
        writer =
            new PartitionedAppendWriter(
                table.spec(),
                format,
                appenderFactory,
                fileFactory,
                table.io(),
                targetFileSize,
                schema);
      } else {
        writer =
            new PartitionedDeltaWriter(
                table.spec(),
                format,
                appenderFactory,
                fileFactory,
                table.io(),
                targetFileSize,
                schema,
                identifierFieldIds,
                config.isUpsertModeEnabled());
      }
    }
    return writer;
  }

  public static Object extractFromRecordValue(Record recordValue, String fieldName) {
    String[] fields = fieldName.split("\\.");
    return getValueFromRecord(recordValue, fields, 0);
  }

  public static Object getValueFromRecord(Record record, String[] fields, int idx) {
    checkArgument(idx < fields.length, "Invalid field index");
    Object value = record.getField(fields[idx]);

    if (value == null || idx == fields.length - 1) {
      return value;
    }

    // Fix: Use the current value as the new record if it's a Record type
    if (value instanceof Record) {
      return getValueFromRecord((Record) value, fields, idx + 1);
    }

    // Return null if intermediate value isn't a Record
    return null;
  }

  /**
   * Find a field in the schema by name, supporting nested field access with dot notation.
   * @param schema the Iceberg schema to search
   * @param fieldName the field name, can be nested with dot notation (e.g., "error.receivedAt")
   * @return the field type if found, null otherwise
   */
  private static org.apache.iceberg.types.Type findFieldType(org.apache.iceberg.Schema schema, String fieldName) {
    String[] parts = fieldName.split("\\.");
    org.apache.iceberg.types.Type currentType = schema.asStruct();

    for (String part : parts) {
      if (!(currentType instanceof org.apache.iceberg.types.Types.StructType)) {
        return null;
      }

      org.apache.iceberg.types.Types.StructType structType = (org.apache.iceberg.types.Types.StructType) currentType;
      org.apache.iceberg.types.Types.NestedField field = structType.field(part);
      if (field == null) {
        return null;
      }
      currentType = field.type();
    }

    return currentType;
  }

  public static IcebergPartitionSpec buildPartitionSpec(org.apache.iceberg.Schema icebergSchema,
                                                        List<IcebergPartitionConfig> partitionConfig) {
    if (log.isDebugEnabled()) {
      log.debug("Building partition spec for schema {} with config {}", icebergSchema, partitionConfig);
    }
    List<IcebergExpression> expressions = new ArrayList<>();

    if (partitionConfig == null || partitionConfig.isEmpty()) {
      return new IcebergPartitionSpec(PartitionSpec.unpartitioned(), List.of());
    }

    PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
    Set<String> sourceColumns = Sets.newHashSet();

    for (IcebergPartitionConfig partition : partitionConfig) {
      String sourceColumn = partition.getSourceColumn();

      if (StringUtils.isBlank(sourceColumn) || findFieldType(icebergSchema, sourceColumn) == null) {
        log.warn("Source column {} not found in schema {}", sourceColumn, icebergSchema);
        continue;
      }

      if (sourceColumns.contains(sourceColumn)) {
        log.warn("Duplicate source column {} in partition config", sourceColumn);
        continue;
      }

      if (StringUtils.isBlank(partition.getTransform())) {
        // No transform specified, use identity
        builder.identity(sourceColumn);
        expressions.add(new IcebergExpression(sourceColumn, Expressions.ref(sourceColumn)));
        sourceColumns.add(sourceColumn);
      } else {
        String transformStr = partition.getTransform().toLowerCase(Locale.ROOT).strip();
        boolean isTargetNameEmpty = StringUtils.isBlank(partition.getTargetName());
        String targetName = partition.getTargetName();

        if (transformStr.startsWith("bucket")) {
          try {
            if (!transformStr.strip().endsWith("]")) {
              throw new IllegalArgumentException("Invalid bucket transform");
            }

            // Extract parameter and trim whitespace: "bucket[10]" or "bucket[ 10 ]"
            String paramStr = transformStr.split("\\[")[1].replace("]", "").trim();
            int bucketParam = Integer.parseInt(paramStr);
            if (isTargetNameEmpty) {
              builder.bucket(sourceColumn, bucketParam);
              expressions.add(new IcebergExpression(null, Expressions.bucket(sourceColumn, bucketParam)));
            } else {
              builder.bucket(sourceColumn, bucketParam, targetName);
              expressions.add(new IcebergExpression(targetName, Expressions.bucket(sourceColumn, bucketParam)));
            }
            sourceColumns.add(sourceColumn);
          } catch (RuntimeException e) {
            log.warn("Invalid bucket parameter in transform {} for source column {}", transformStr, sourceColumn);
          }
        } else if (transformStr.startsWith("truncate")) {
          try {
            if (!transformStr.strip().endsWith("]")) {
              throw new IllegalArgumentException("Invalid truncate transform");
            }
            // Extract parameter and trim whitespace: "truncate[5]" or "truncate[ 5 ]"
            String paramStr = transformStr.split("\\[")[1].replace("]", "").trim();
            int width = Integer.parseInt(paramStr);
            if (isTargetNameEmpty) {
              builder.truncate(sourceColumn, width);
              expressions.add(new IcebergExpression(null, Expressions.truncate(sourceColumn, width)));
            } else {
              builder.truncate(sourceColumn, width, targetName);
              expressions.add(new IcebergExpression(targetName, Expressions.truncate(sourceColumn, width)));
            }
            sourceColumns.add(sourceColumn);
          } catch (RuntimeException e) {
            log.warn("Invalid truncate parameter in transform {} for source column {}", transformStr, sourceColumn);
          }
        } else {
          switch (transformStr) {
            case "year":
              if (isTargetNameEmpty) {
                builder.year(sourceColumn);
                expressions.add(new IcebergExpression(null, Expressions.year(sourceColumn)));
              } else {
                builder.year(sourceColumn, targetName);
                expressions.add(new IcebergExpression(targetName, Expressions.year(sourceColumn)));
              }
              sourceColumns.add(sourceColumn);
              break;
            case "month":
              if (isTargetNameEmpty) {
                builder.month(sourceColumn);
                expressions.add(new IcebergExpression(null, Expressions.month(sourceColumn)));
              } else {
                builder.month(sourceColumn, targetName);
                expressions.add(new IcebergExpression(targetName, Expressions.month(sourceColumn)));
              }
              sourceColumns.add(sourceColumn);
              break;
            case "day":
              if (isTargetNameEmpty) {
                builder.day(sourceColumn);
                expressions.add(new IcebergExpression(null, Expressions.day(sourceColumn)));
              } else {
                builder.day(sourceColumn, targetName);
                expressions.add(new IcebergExpression(targetName, Expressions.day(sourceColumn)));
              }
              sourceColumns.add(sourceColumn);
              break;
            case "hour":
              if (isTargetNameEmpty) {
                builder.hour(sourceColumn);
                expressions.add(new IcebergExpression(null, Expressions.hour(sourceColumn)));
              } else {
                builder.hour(sourceColumn, targetName);
                expressions.add(new IcebergExpression(targetName, Expressions.hour(sourceColumn)));
              }
              sourceColumns.add(sourceColumn);
              break;
            case "identity":
              try {
                // Note: identity() does not support custom target names in Iceberg API
                // The target name from config will be ignored for identity transforms
                builder.identity(sourceColumn);
                expressions.add(new IcebergExpression(sourceColumn, Expressions.ref(sourceColumn)));
                sourceColumns.add(sourceColumn);

                if (!isTargetNameEmpty) {
                  log.warn("Identity transform does not support custom target names. "
                          + "Ignoring target name '{}' for source column '{}'", targetName, sourceColumn);
                }
              } catch (IllegalArgumentException e) {
                log.warn("Identity transform failed for source column {}: {}", sourceColumn, e.getMessage());
              }
              break;
            case "void":
              if (isTargetNameEmpty) {
                builder.alwaysNull(sourceColumn);
                // Void transform always produces null, but we still reference the source column
                expressions.add(new IcebergExpression(null, Expressions.ref(sourceColumn)));
              } else {
                builder.alwaysNull(sourceColumn, targetName);
                expressions.add(new IcebergExpression(targetName, Expressions.ref(sourceColumn)));
              }
              sourceColumns.add(sourceColumn);
              break;
            default:
              log.warn("Unsupported transform {} for source column {}", transformStr, sourceColumn);
          }
        }
      }
    }

    if (sourceColumns.isEmpty()) {
      log.warn("No valid partition spec found, using unpartitioned spec");
      return new IcebergPartitionSpec(PartitionSpec.unpartitioned(), List.of());
    }

    return new IcebergPartitionSpec(builder.build(), expressions);
  }

  private Utilities() {}
}
