/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.serde.iceberg;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.lakestream.ursa.exception.BadSchemaException;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.MessageSerDeException;
import io.lakestream.ursa.exception.RuntimeExceptionWithCode;
import io.lakestream.ursa.lakehouse.iceberg.AvroToIcebergConverter;
import io.lakestream.ursa.lakehouse.utils.AvroSchemaUtilExtended;
import io.lakestream.ursa.lakehouse.utils.IcebergSchemaToTypeUtil;
import io.lakestream.ursa.lakehouse.utils.LakehouseFieldNames;
import io.lakestream.ursa.lakehouse.v2.iceberg.IcebergTableSchemaService;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

@Slf4j
public class KafkaEntryToIcebergRecordEncoder extends KafkaEntryEncoder<Record> implements EntryEncoder<Record> {

    private static final String PRIMITIVE_FIELD_NAME = "payload";

    public KafkaEntryToIcebergRecordEncoder(SchemaService schemaService) {
        super(schemaService);
    }

    @Override
    protected Record transform(Object object, SchemaMetadata schemaMetadata, SchemaKey schemaKey,
                               TableSchemaService tableSchemaService,
                               EntryEncoderContext context) throws MessageSerDeException, RuntimeExceptionWithCode {

        if (object == null) {
            throw new MessageSerDeException(ExceptionCode.MESSAGE_NULL_VALUE,
                "null value is not supported to write to the external table");
        }

        try {
            SchemaResult<Schema> result;
            Record convertedRecord = switch (schemaMetadata.getSchemaType()) {
                case "AVRO" -> {
                    Object record = object;
                    if (record instanceof org.apache.avro.generic.GenericRecord avroRecord) {
                        schemaKey.setConvertedType(SchemaKey.ConvertedType.AVRO_TO_ICEBERG_RECORD);
                        result = loadIcebergSchema(schemaKey, schemaMetadata, tableSchemaService, context);
                        yield AvroToIcebergConverter.convert(avroRecord, result.tableSchema());
                    } else {
                        schemaKey.setConvertedType(SchemaKey.ConvertedType.AVRO_TO_ICEBERG_PRIMITIVE);
                        result = loadIcebergSchema(schemaKey, schemaMetadata, tableSchemaService, context);
                        Object o = AvroToIcebergConverter.convertValue(record, result.avroSchema(),
                                result.tableSchema().asStruct().fields().get(0).type());
                        yield transformPrimitive(o, result.tableSchema());
                    }
                }
                case "JSON" -> {
                    schemaKey.setConvertedType(SchemaKey.ConvertedType.JSON_TO_ICEBERG_RECORD);
                    result = loadIcebergSchema(schemaKey, schemaMetadata, tableSchemaService, context);
                    var avroRecord = JsonAvroConverter.INSTANCE
                            .convertToGenericDataRecord((ObjectNode) object, result.avroSchema());
                    yield  AvroToIcebergConverter.convert(avroRecord, result.tableSchema());
                }
                case "PROTOBUF" -> {
                    schemaKey.setConvertedType(SchemaKey.ConvertedType.PROTOBUF_TO_ICEBERG_RECORD);
                    result = loadIcebergSchema(schemaKey, schemaMetadata, tableSchemaService, context);
                    yield ProtobufNativeToIcebergConverter.convertProtobufToIcebergRecord(
                            (DynamicMessage) object, result.tableSchema());
                }
                case "PRIMITIVE" -> {
                    // all the unsupported schema type in kafka will be treated as byte[]
                    schemaKey.setConvertedType(SchemaKey.ConvertedType.KAFKA_PRIMITIVE_TO_ICEBERG);
                    result = loadIcebergSchema(schemaKey, schemaMetadata, tableSchemaService, context);
                    var payload = ByteBuffer.wrap((byte[]) object);
                    yield transformPrimitive(payload, result.tableSchema());
                }
                default ->
                    throw new UnsupportedOperationException("Unsupported schema type: "
                                                            + schemaMetadata.getSchemaType());
            };
            injectMetadataIfNeeded(convertedRecord, context, result.tableSchema());
            return convertedRecord;
        } catch (Throwable throwable) {
            if (throwable instanceof ExecutionException) {
                throwable =  throwable.getCause();
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

    void injectMetadataIfNeeded(Record record, EntryEncoderContext context, Schema schema) {
        if (context.isPersistExtraMetadata()) {
            var metadataField = schema.findField(LakehouseFieldNames.META);
            if (metadataField != null) {
                var metadata = context.toMetaVariant();
                record.setField(LakehouseFieldNames.META, metadata);
            }
        }
        if (context.isPersistKey()) {
            var keyField = schema.findField(LakehouseFieldNames.INTERNAL_KEY);
            if (keyField != null) {
                record.setField(LakehouseFieldNames.INTERNAL_KEY,
                        context.keyByteBuffer());
            }
        }
    }

    Schema convertSchemaMetadataToIcebergSchema0(SchemaMetadata schemaMetadata, EntryEncoderContext context) {
        try {
            Schema icebergSchema = switch (schemaMetadata.getSchemaType()) {
                case "AVRO" -> {
                    var avroSchema = new org.apache.avro.Schema.Parser().parse(schemaMetadata.getSchema());
                    if (avroSchema.getType().equals(org.apache.avro.Schema.Type.RECORD)) {
                        AvroSchemaUtilExtended.validateNoEmptyRecords(avroSchema);
                        yield AvroSchemaUtilExtended.toIceberg(avroSchema, context.isVariantEnabled(),
                            context.isUnityCatalog());
                    } else {
                        var type = IcebergSchemaToTypeUtil.primitive(avroSchema);
                        yield getPrimitiveTypeSchema(type);
                    }
                }
                case "JSON" -> {
                    var jsonToAvroSchema = JsonSchema.of(schemaMetadata.getSchema()).toAvroSchema();
                    AvroSchemaUtilExtended.validateNoEmptyRecords(jsonToAvroSchema);
                    yield AvroSchemaUtilExtended.toIceberg(jsonToAvroSchema, context.isVariantEnabled(),
                        context.isUnityCatalog());
                }
                case "PROTOBUF" -> {
                    var pbAvroSchema = PBRecordReader.convertPbSchemaToAvroByName(
                            schemaMetadata.getSchema(), getRequiredProtobufMessageName(context));
                    AvroSchemaUtilExtended.validateNoEmptyRecords(pbAvroSchema);
                    yield AvroSchemaUtilExtended.toIceberg(pbAvroSchema, context.isVariantEnabled(),
                        context.isUnityCatalog());
                }
                case "PRIMITIVE" -> getPrimitiveTypeSchema(Types.BinaryType.get());
                default ->
                    throw new IllegalArgumentException("Unsupported schema type: " + schemaMetadata.getSchemaType());
            };
            if (context.isPersistKey()) {
                icebergSchema = processExtraKeyField(icebergSchema);
            }
            if (context.isPersistExtraMetadata() && context.isVariantEnabled()) {
                icebergSchema = processExtraMetaField(icebergSchema);
            }
            return icebergSchema;
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new RuntimeExceptionWithCode(new BadSchemaException(ExceptionCode.MESSAGE_BAD_SCHEMA, e));
        }
    }

    Schema convertSchemaMetadataToIcebergSchema(SchemaMetadata schemaMetadata, EntryEncoderContext context) {
        try {
            Schema icebergSchema = switch (schemaMetadata.getSchemaType()) {
                case "AVRO" -> {
                    var avroSchema = new org.apache.avro.Schema.Parser().parse(schemaMetadata.getSchema());
                    if (avroSchema.getType().equals(org.apache.avro.Schema.Type.RECORD)) {
                        AvroSchemaUtilExtended.validateNoEmptyRecords(avroSchema);
                        yield AvroSchemaUtilExtended.toIceberg(avroSchema, context.isVariantEnabled(),
                            context.isUnityCatalog());
                    } else {
                        var type = IcebergSchemaToTypeUtil.primitive(avroSchema);
                        yield getPrimitiveTypeSchema(type);
                    }
                }
                case "JSON" -> {
                    var jsonToAvroSchema = JsonSchema.of(schemaMetadata.getSchema()).toAvroSchema();
                    AvroSchemaUtilExtended.validateNoEmptyRecords(jsonToAvroSchema);
                    yield  AvroSchemaUtilExtended.toIceberg(jsonToAvroSchema, context.isVariantEnabled(),
                        context.isUnityCatalog());
                }
                case "PROTOBUF" -> {
                    var pbAvroSchema = PBRecordReader.convertPbSchemaToAvroByName(
                            schemaMetadata.getSchema(), getRequiredProtobufMessageName(context));
                    AvroSchemaUtilExtended.validateNoEmptyRecords(pbAvroSchema);
                    yield AvroSchemaUtilExtended.toIceberg(pbAvroSchema, context.isVariantEnabled(),
                        context.isUnityCatalog());
                }
                case "PRIMITIVE" -> getPrimitiveTypeSchema(Types.BinaryType.get());
                default ->
                    throw new IllegalArgumentException("Unsupported schema type: " + schemaMetadata.getSchemaType());
            };
            if (context.isPersistKey()) {
                icebergSchema = processExtraKeyField(icebergSchema);
            }
            if (context.isPersistExtraMetadata() && context.isVariantEnabled()) {
                icebergSchema = processExtraMetaField(icebergSchema);
            }
            return icebergSchema;
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new RuntimeExceptionWithCode(new BadSchemaException(ExceptionCode.MESSAGE_BAD_SCHEMA, e));
        }
    }

    Schema processExtraKeyField(Schema icebergSchema) {
        int nextId = icebergSchema.highestFieldId() + 1;
        List<Types.NestedField> fields = new ArrayList<>(icebergSchema.columns());
        fields.add(Types.NestedField.optional(nextId,
                LakehouseFieldNames.INTERNAL_KEY, Types.BinaryType.get()));
        return new Schema(icebergSchema.schemaId(), fields,
                icebergSchema.identifierFieldIds());
    }

    Schema processExtraMetaField(Schema icebergSchema) {
        Types.NestedField metaField = Types.NestedField.optional(
                icebergSchema.highestFieldId() + 1,
                LakehouseFieldNames.META, Types.VariantType.get());
        List<Types.NestedField> fields = new ArrayList<>(icebergSchema.columns());
        fields.add(metaField);
        return new Schema(icebergSchema.schemaId(), fields,
                icebergSchema.identifierFieldIds());
    }

    SchemaResult getIcebergSchema(SchemaMetadata schemaMetadata, EntryEncoderContext context) {
        var icebergSchema = convertSchemaMetadataToIcebergSchema(schemaMetadata, context);
        if (schemaMetadata.getSchemaType().equals("AVRO")) {
            var avroSchema = new org.apache.avro.Schema.Parser().parse(schemaMetadata.getSchema());
            if (!avroSchema.getType().equals(org.apache.avro.Schema.Type.RECORD)) {
                return new SchemaResult(avroSchema, icebergSchema);
            }
        } else if (schemaMetadata.getSchemaType().equals("JSON")) {
            var avroSchema = JsonSchema.of(schemaMetadata.getSchema()).toAvroSchema();
            return new SchemaResult(avroSchema, icebergSchema);
        }
        return  new SchemaResult(icebergSchema);
    }

    SchemaResult<Schema> loadIcebergSchema(SchemaKey schemaKey, SchemaMetadata schemaInfo,
                                           TableSchemaService tableSchemaService,
                                           EntryEncoderContext context) throws Exception {

        var schemaResult = (SchemaResult) SCHEMA_CACHE.computeIfAbsent(schemaKey, () -> {
            try {
                var result = getIcebergSchema(schemaInfo, context);
                var finalTableSchema = evolveIcebergSchema(tableSchemaService, schemaKey,
                    (Schema) result.tableSchema(), context);
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

    Schema evolveIcebergSchema(TableSchemaService tableSchemaService,
                               SchemaKey schemaKey, Schema schema,
                               EntryEncoderContext context) throws Exception {
        if (tableSchemaService == null) {
            log.info("Table schema service is null, skip evolve schema for topic: {}", schemaKey.getTopicName());
            return schema;
        }
        if (!(tableSchemaService instanceof IcebergTableSchemaService)) {
            log.error("Table schema service is not instance of IcebergTableSchemaService");
            throw new IllegalArgumentException("Table schema service must be an instance of IcebergTableSchemaService");
        }

        return SchemaEvolutionManager.evolveSchema(tableSchemaService, schemaService, schemaKey,
            this::convertSchemaMetadataToIcebergSchema0, context);
    }

    private Schema getPrimitiveTypeSchema(Type primitiveType) {
        Types.NestedField nestedField = Types.NestedField.optional(1, PRIMITIVE_FIELD_NAME, primitiveType);
        return new Schema(Collections.singletonList(nestedField));
    }

    private Record transformPrimitive(Object object, Schema schema) {
        Record record = GenericRecord.create(schema);
        record.setField(PRIMITIVE_FIELD_NAME, object);
        return record;
    }

    private String getRequiredProtobufMessageName(EntryEncoderContext context) {
        return context.protobufMessageName().orElseThrow(
            () -> new IllegalArgumentException("Missing protobuf message name in context"));
    }


}
