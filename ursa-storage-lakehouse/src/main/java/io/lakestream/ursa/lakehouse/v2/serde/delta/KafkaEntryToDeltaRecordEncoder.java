/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.serde.delta;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.VariantType;
import io.lakestream.ursa.exception.BadSchemaException;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.MessageSerDeException;
import io.lakestream.ursa.exception.RuntimeExceptionWithCode;
import io.lakestream.ursa.lakehouse.delta.AvroToDeltaConvert;
import io.lakestream.ursa.lakehouse.delta.DeltaVariantUtils;
import io.lakestream.ursa.lakehouse.delta.GenericRow;
import io.lakestream.ursa.lakehouse.delta.ProtobufToDeltaConverter;
import io.lakestream.ursa.lakehouse.utils.AvroSchemaUtilExtended;
import io.lakestream.ursa.lakehouse.utils.LakehouseFieldNames;
import io.lakestream.ursa.lakehouse.v2.delta.DeltaTableSchemaService;
import io.lakestream.ursa.materialization.serde.EntryEncoder;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.SchemaEvolutionManager;
import io.lakestream.ursa.materialization.serde.SchemaKey;
import io.lakestream.ursa.materialization.serde.SchemaResult;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.materialization.serde.exception.FatalException;
import io.lakestream.ursa.materialization.serde.kafka.KafkaEntryEncoder;
import io.lakestream.ursa.materialization.serde.utils.json.JsonAvroConverter;
import io.lakestream.ursa.materialization.serde.utils.json.schema.JsonSchema;
import io.lakestream.ursa.materialization.util.PBRecordReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

@Slf4j
public class KafkaEntryToDeltaRecordEncoder extends KafkaEntryEncoder<GenericRow> implements EntryEncoder<GenericRow> {

    private static final String PRIMITIVE_FIELD_NAME = "payload";

    public KafkaEntryToDeltaRecordEncoder(SchemaService schemaService) {
        super(schemaService);
    }

    @Override
    protected GenericRow transform(Object object, SchemaMetadata schemaMetadata, SchemaKey schemaKey,
                                   TableSchemaService tableSchemaService,
                                   EntryEncoderContext context)
        throws MessageSerDeException, RuntimeExceptionWithCode {

        if (object == null) {
            throw new MessageSerDeException(ExceptionCode.MESSAGE_NULL_VALUE,
                "null value is not supported to write to the external table");
        }
        try {
            SchemaResult<StructType> result;
            GenericRow convertedRecord = switch (schemaMetadata.getSchemaType()) {
                case "AVRO" -> {
                    Object record = object;
                    if (object instanceof GenericRecord avoRecord) {
                        schemaKey.setConvertedType(SchemaKey.ConvertedType.AVRO_TO_DELTA_RECORD);
                        result = loadDeltaSchema(schemaKey, schemaMetadata, tableSchemaService, context);
                        yield AvroToDeltaConvert.convert(avoRecord, result.tableSchema());
                    } else {
                        schemaKey.setConvertedType(SchemaKey.ConvertedType.AVRO_TO_DELTA_PRIMITIVE);
                        result = loadDeltaSchema(schemaKey, schemaMetadata, tableSchemaService, context);
                        Object o = AvroToDeltaConvert.convertValue(record, result.avroSchema(),
                            result.tableSchema().get(PRIMITIVE_FIELD_NAME).getDataType());
                        yield transformPrimitive(o, result.tableSchema());
                    }
                }
                case "JSON" -> {
                    schemaKey.setConvertedType(SchemaKey.ConvertedType.JSON_TO_DELTA_RECORD);
                    result = loadDeltaSchema(schemaKey, schemaMetadata, tableSchemaService, context);
                    var avroRecord = JsonAvroConverter.INSTANCE
                        .convertToGenericDataRecord((ObjectNode) object, result.avroSchema());
                    yield AvroToDeltaConvert.convert(avroRecord, result.tableSchema());
                }
                case "PROTOBUF" -> {
                    schemaKey.setConvertedType(SchemaKey.ConvertedType.PROTOBUF_TO_DELTA_RECORD);
                    result = loadDeltaSchema(schemaKey, schemaMetadata, tableSchemaService, context);
                    yield ProtobufToDeltaConverter
                        .convertToGenericRow((DynamicMessage) object, result.tableSchema());
                }
                case "PRIMITIVE" -> {
                    // all the unsupported schema type in kafka will be treated as byte[]
                    schemaKey.setConvertedType(SchemaKey.ConvertedType.KAFKA_PRIMITIVE_TO_DELTA);
                    result = loadDeltaSchema(schemaKey, schemaMetadata, tableSchemaService, context);
                    yield transformPrimitive(object, result.tableSchema());
                }
                default -> throw new UnsupportedOperationException("Unsupported schema type: "
                    + schemaMetadata.getSchemaType());

            };
            injectMetadataIfNeeded(convertedRecord, context, result.tableSchema());
            return convertedRecord;
        } catch (Throwable throwable) {
            if (throwable instanceof ExecutionException) {
                throwable = throwable.getCause();
            }
            if (throwable instanceof FatalException) {
                throw (FatalException) throwable;
            }
            if (throwable instanceof RuntimeExceptionWithCode rew) {
                if (rew.getRealException() instanceof MessageSerDeException) {
                    throw (MessageSerDeException) rew.getRealException();
                } else {
                    throw new MessageSerDeException(rew.getRealException().getExceptionCode(), rew.getRealException());
                }
            }
            throw new MessageSerDeException(ExceptionCode.MESSAGE_SERIALIZE_TO_LAKEHOUSE_ERROR, throwable);
        }
    }

    void injectMetadataIfNeeded(GenericRow record, EntryEncoderContext context, StructType schema) {
        if (context.isPersistExtraMetadata() && context.isVariantEnabled()) {
            var metadataIndex = schema.indexOf(LakehouseFieldNames.META);
            if (metadataIndex != -1) {
                record.put(metadataIndex, DeltaVariantUtils.fromJson(context.toMeta()));
            }
        }
        if (context.isPersistKey()) {
            var keyFieldIndex = schema.indexOf(LakehouseFieldNames.INTERNAL_KEY);
            if (keyFieldIndex != -1) {
                record.put(keyFieldIndex, context.keyBytesArray());
            }
        }
    }

    SchemaResult<StructType> loadDeltaSchema(SchemaKey schemaKey, SchemaMetadata schemaMetadata,
                                             TableSchemaService tableSchemaService,
                                             EntryEncoderContext context)
        throws Exception {
        var schemaResult = (SchemaResult) SCHEMA_CACHE.computeIfAbsent(schemaKey, () -> {
            try {
                var result = getDeltaSchema(schemaMetadata, context);
                var finalTableSchema = evolveDeltaSchema(tableSchemaService, schemaKey, result.tableSchema(), context);
                return new SchemaResult<>(result.avroSchema(), finalTableSchema);
            } catch (RuntimeExceptionWithCode runtimeExceptionWithCode) {
                return new SchemaResult<>(null, null, runtimeExceptionWithCode);
            } catch (Throwable throwable) {
                return new SchemaResult<>(null, null, new FatalException(throwable));
            }
        });
        if (schemaResult.exception() != null) {
            if (!(schemaResult.exception() instanceof RuntimeExceptionWithCode)) {
                SCHEMA_CACHE.invalidate(schemaKey);
            }
            throw schemaResult.exception();
        }
        return schemaResult;
    }

    SchemaResult<StructType> getDeltaSchema(SchemaMetadata schemaMetadata, EntryEncoderContext context) {
        var deltaSchema = convertSchemaMetadataToDeltaSchema(schemaMetadata, context);
        if (schemaMetadata.getSchemaType().equals("AVRO")) {
            var avroSchema = new org.apache.avro.Schema.Parser().parse(schemaMetadata.getSchema());
            if (!avroSchema.getType().equals(org.apache.avro.Schema.Type.RECORD)) {
                return new SchemaResult<>(avroSchema, deltaSchema);
            }
        } else if (schemaMetadata.getSchemaType().equals("JSON")) {
            var avroSchema = JsonSchema.of(schemaMetadata.getSchema()).toAvroSchema();
            return new SchemaResult<>(avroSchema, deltaSchema);
        }
        return new SchemaResult<>(deltaSchema);
    }

    void evolveDeltaSchema(TableSchemaService tableSchemaService,
                           SchemaKey schemaKey, EntryEncoderContext context) throws Exception {
        evolveDeltaSchema(tableSchemaService, schemaKey, null, context);
    }

    StructType evolveDeltaSchema(TableSchemaService tableSchemaService,
                           SchemaKey schemaKey, StructType schema, EntryEncoderContext context) throws Exception {
        if (tableSchemaService == null) {
            log.info("Table schema service is null, skip evolve schema for topic: {}", schemaKey.getTopicName());
            return schema;
        }
        if (!(tableSchemaService instanceof DeltaTableSchemaService)) {
            log.error("Table schema service is not instance of DeltaTableSchemaService");
            throw new IllegalArgumentException("Table schema service must be an instance of DeltaTableSchemaService");
        }

        return SchemaEvolutionManager.evolveSchema(tableSchemaService, schemaService, schemaKey,
            this::convertSchemaMetadataToDeltaSchema0, context);
    }

    StructType convertSchemaMetadataToDeltaSchema0(SchemaMetadata schemaMetadata, EntryEncoderContext context) {
        try {
            StructType deltaSchema = switch (schemaMetadata.getSchemaType()) {
                case "AVRO" -> {
                    var avroSchema = new org.apache.avro.Schema.Parser().parse(schemaMetadata.getSchema());
                    if (avroSchema.getType().equals(org.apache.avro.Schema.Type.RECORD)) {
                        AvroSchemaUtilExtended.validateNoEmptyRecords(avroSchema);
                        yield AvroSchemaUtilExtended.toDelta(avroSchema, context.isVariantEnabled());
                    } else {
                        DataType dataType =
                                AvroSchemaUtilExtended.schemaTypeToDeltaType(avroSchema, context.isVariantEnabled());
                        yield getPrimitiveTypeSchema(dataType);
                    }
                }
                case "JSON" -> {
                    var jsonToAvroSchema = JsonSchema.of(schemaMetadata.getSchema()).toAvroSchema();
                    AvroSchemaUtilExtended.validateNoEmptyRecords(jsonToAvroSchema);
                    yield AvroSchemaUtilExtended.toDelta(jsonToAvroSchema, context.isVariantEnabled());
                }
                case "PRIMITIVE" -> getPrimitiveTypeSchema(BinaryType.BINARY);
                case "PROTOBUF" -> {
                    var pbAvroSchema = PBRecordReader.convertPbSchemaToAvroByName(
                        schemaMetadata.getSchema(), getRequiredProtobufMessageName(context));
                    AvroSchemaUtilExtended.validateNoEmptyRecords(pbAvroSchema);
                    yield AvroSchemaUtilExtended.toDelta(pbAvroSchema, context.isVariantEnabled());
                }
                default ->
                    throw new IllegalArgumentException("Unsupported schema type: " + schemaMetadata.getSchemaType());
            };
            if (context.isPersistExtraMetadata() && context.isVariantEnabled()) {
                deltaSchema = processExtraMetaFields(deltaSchema);
            }
            if (context.isPersistKey()) {
                deltaSchema = processExtraKeyField(deltaSchema);
            }
            return deltaSchema;
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new RuntimeExceptionWithCode(new BadSchemaException(ExceptionCode.MESSAGE_BAD_SCHEMA, e));
        }
    }

    StructType convertSchemaMetadataToDeltaSchema(SchemaMetadata schemaMetadata, EntryEncoderContext context) {
        try {
            StructType deltaSchema = switch (schemaMetadata.getSchemaType()) {
                case "AVRO" -> {
                    var avroSchema = new org.apache.avro.Schema.Parser().parse(schemaMetadata.getSchema());
                    if (avroSchema.getType().equals(org.apache.avro.Schema.Type.RECORD)) {
                        AvroSchemaUtilExtended.validateNoEmptyRecords(avroSchema);
                        yield AvroSchemaUtilExtended.toDelta(avroSchema, context.isVariantEnabled());
                    } else {
                        DataType dataType =
                                AvroSchemaUtilExtended.schemaTypeToDeltaType(avroSchema, context.isVariantEnabled());
                        yield getPrimitiveTypeSchema(dataType);
                    }
                }
                case "JSON" -> {
                    var jsonToAvroSchema = JsonSchema.of(schemaMetadata.getSchema()).toAvroSchema();
                    AvroSchemaUtilExtended.validateNoEmptyRecords(jsonToAvroSchema);
                    yield AvroSchemaUtilExtended.toDelta(jsonToAvroSchema, context.isVariantEnabled());
                }
                case "PRIMITIVE" -> getPrimitiveTypeSchema(BinaryType.BINARY);
                case "PROTOBUF" -> {
                    var pbAvroSchema = PBRecordReader.convertPbSchemaToAvroByName(
                            schemaMetadata.getSchema(), getRequiredProtobufMessageName(context));
                    AvroSchemaUtilExtended.validateNoEmptyRecords(pbAvroSchema);
                    yield AvroSchemaUtilExtended.toDelta(pbAvroSchema, context.isVariantEnabled());
                }
                default ->
                    throw new IllegalArgumentException("Unsupported schema type: " + schemaMetadata.getSchemaType());
            };
            if (context.isPersistExtraMetadata() && context.isVariantEnabled()) {
                deltaSchema = processExtraMetaFields(deltaSchema);
            }
            if (context.isPersistKey()) {
                deltaSchema = processExtraKeyField(deltaSchema);
            }
            return deltaSchema;
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new RuntimeExceptionWithCode(new BadSchemaException(ExceptionCode.MESSAGE_BAD_SCHEMA, e));
        }
    }

    StructType processExtraMetaFields(StructType deltaSchema) {
        return deltaSchema.add(LakehouseFieldNames.META, VariantType.VARIANT, true);
    }

    StructType processExtraKeyField(StructType deltaSchema) {
        return deltaSchema.add(LakehouseFieldNames.INTERNAL_KEY,
                BinaryType.BINARY, true);
    }

    private StructType getPrimitiveTypeSchema(DataType primitiveType) {
        StructField structField = new StructField(PRIMITIVE_FIELD_NAME, primitiveType, true);
        return new StructType(Collections.singletonList(structField));
    }

    private GenericRow transformPrimitive(Object object, StructType deltaSchema) {
        Map<Integer, Object> ordinalToValue = new HashMap<>();
        ordinalToValue.put(0, object);
        return new GenericRow(deltaSchema, ordinalToValue);
    }

    private String getRequiredProtobufMessageName(EntryEncoderContext context) {
        return context.protobufMessageName().orElseThrow(
            () -> new IllegalArgumentException("Missing protobuf message name in context"));
    }

}
