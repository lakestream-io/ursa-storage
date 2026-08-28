/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.parquet;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.parquet.Preconditions;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * Refer to.
 * https://github.com/cloudera/parquet-mr/blob/master/parquet-avro
 * /src/main/java/parquet/avro/AvroIndexedRecordConverter.java
 */
class AvroRecordConverter extends GroupConverter {

    private final ParentValueContainer parent;
    protected GenericRecord currentRecord;
    private final Converter[] converters;

    private final Schema avroSchema;

    public AvroRecordConverter(MessageType parquetSchema, Schema avroSchema) {
        this(null, parquetSchema, avroSchema);
    }

    public AvroRecordConverter(ParentValueContainer parent, GroupType
            parquetSchema, Schema avroSchema) {
        this.parent = parent;
        this.avroSchema = avroSchema;
        int schemaSize = parquetSchema.getFieldCount();
        this.converters = new Converter[schemaSize];

        Map<String, Integer> avroFieldIndexes = new HashMap<String, Integer>();
        int avroFieldIndex = 0;
        for (Schema.Field field: avroSchema.getFields()) {
            avroFieldIndexes.put(field.name(), avroFieldIndex++);
        }
        int parquetFieldIndex = 0;
        for (Type parquetField: parquetSchema.getFields()) {
            Schema.Field avroField = avroSchema.getField(parquetField.getName());
            if (avroField == null) {
                throw new InvalidRecordException(
                        String.format("Parquet/Avro schema mismatch. Avro field '%s' not found.",
                                parquetField.getName()));
            }
            Schema nonNullSchema = AvroSchemaConverter.getNonNull(avroField.schema());
            final int finalAvroIndex = avroFieldIndexes.get(avroField.name());
            converters[parquetFieldIndex++] = newConverter(nonNullSchema, parquetField, new ParentValueContainer() {
                @Override
                void add(Object value) {
                    AvroRecordConverter.this.set(finalAvroIndex, value);
                }
            });
        }
    }

    private static Converter newConverter(Schema schema, Type type,
                                          ParentValueContainer parent) {
        // Handle UNION types specially - Parquet may represent them as optional fields
        if (schema.getType().equals(Schema.Type.UNION)) {
            // Check if Parquet type is actually a group (true union representation)
            // or if it's a primitive/optional field (union with null)
            if (type.isPrimitive()
                    || (type instanceof GroupType
                        && !isParquetUnionGroup((GroupType) type, schema))) {
                // Parquet represents this union as an optional field, not a union group
                // Unwrap the union and use the non-null schema
                Schema nonNullSchema = AvroSchemaConverter.getNonNull(schema);
                return newConverter(nonNullSchema, type, parent);
            } else {
                // Actual union representation in Parquet (group with multiple member types)
                return new AvroUnionConverter(parent, type, schema);
            }
        }

        if (schema.getType().equals(Schema.Type.BOOLEAN)) {
            return new FieldBooleanConverter(parent);
        } else if (schema.getType().equals(Schema.Type.INT)) {
            return new FieldIntegerConverter(parent);
        } else if (schema.getType().equals(Schema.Type.LONG)) {
            return new FieldLongConverter(parent);
        } else if (schema.getType().equals(Schema.Type.DOUBLE)) {
            return new FieldDoubleConverter(parent);
        } else if (schema.getType().equals(Schema.Type.FLOAT)) {
            return new FieldFloatConverter(parent);
        } else if (schema.getType().equals(Schema.Type.BYTES)) {
            return new FieldBytesConverter(parent);
        } else if (schema.getType().equals(Schema.Type.STRING)) {
            return new FieldStringConverter(parent);
        } else if (schema.getType().equals(Schema.Type.RECORD)) {
            return new AvroRecordConverter(parent, type.asGroupType(), schema);
        } else if (schema.getType().equals(Schema.Type.ENUM)) {
            return new FieldEnumConverter(parent, schema);
        } else if (schema.getType().equals(Schema.Type.ARRAY)) {
            return new AvroArrayConverter(parent, type, schema);
        } else if (schema.getType().equals(Schema.Type.MAP)) {
            return new MapConverter(parent, type, schema);
        } else if (schema.getType().equals(Schema.Type.FIXED)) {
            return new FieldFixedConverter(parent, schema);
        }
        throw new UnsupportedOperationException(
                String.format("Cannot convert Avro type: %s (Parquet type: %s) ", schema, type));
    }

    /**
     * Helper method to determine if a Parquet GroupType represents an actual union.
     * (as opposed to a record, array, or map)
     */
    private static boolean isParquetUnionGroup(GroupType groupType, Schema avroSchema) {
        // A Parquet union group should have fields corresponding to non-null union members
        int nonNullCount = 0;
        for (Schema memberSchema : avroSchema.getTypes()) {
            if (!memberSchema.getType().equals(Schema.Type.NULL)) {
                nonNullCount++;
            }
        }
        // If the group field count matches non-null union members, it's likely a union
        return groupType.getFieldCount() == nonNullCount && groupType.getFieldCount() > 1;
    }

    private void set(int index, Object value) {
        this.currentRecord.put(index, value);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    @Override
    public void start() {
        // Should do the right thing whether it is generic or specific
        this.currentRecord = new GenericData.Record(avroSchema);
    }

    @Override
    public void end() {
        if (parent != null) {
            parent.add(currentRecord);
        }
    }

    GenericRecord getCurrentRecord() {
        return currentRecord;
    }

    abstract static class ParentValueContainer {

        /**
         * Adds the value to the parent.
         */
        abstract void add(Object value);

    }

    static class FieldBooleanConverter extends PrimitiveConverter {

        private final ParentValueContainer parent;

        public FieldBooleanConverter(ParentValueContainer parent) {
            this.parent = parent;
        }

        @Override
        public void addBoolean(boolean value) {
            parent.add(value);
        }

    }

    static class FieldIntegerConverter extends PrimitiveConverter {

        private final ParentValueContainer parent;

        public FieldIntegerConverter(ParentValueContainer parent) {
            this.parent = parent;
        }

        @Override
        public void addInt(int value) {
            parent.add(value);
        }

    }

    static class FieldLongConverter extends PrimitiveConverter {

        private final ParentValueContainer parent;

        public FieldLongConverter(ParentValueContainer parent) {
            this.parent = parent;
        }

        @Override
        public void addLong(long value) {
            parent.add(value);
        }

    }

    static class FieldFloatConverter extends PrimitiveConverter {

        private final ParentValueContainer parent;

        public FieldFloatConverter(ParentValueContainer parent) {
            this.parent = parent;
        }

        @Override
        public void addFloat(float value) {
            parent.add(value);
        }

    }

    static class FieldDoubleConverter extends PrimitiveConverter {

        private final ParentValueContainer parent;

        public FieldDoubleConverter(ParentValueContainer parent) {
            this.parent = parent;
        }

        @Override
        public void addDouble(double value) {
            parent.add(value);
        }

    }

    static class FieldBytesConverter extends PrimitiveConverter {

        private final ParentValueContainer parent;

        public FieldBytesConverter(ParentValueContainer parent) {
            this.parent = parent;
        }

        @Override
        public void addBinary(Binary value) {
            parent.add(ByteBuffer.wrap(value.getBytes()));
        }

    }

    static class FieldStringConverter extends PrimitiveConverter {

        private final ParentValueContainer parent;

        public FieldStringConverter(ParentValueContainer parent) {
            this.parent = parent;
        }

        @Override
        public void addBinary(Binary value) {
            parent.add(value.toStringUsingUTF8());
        }

    }

    static class FieldEnumConverter extends PrimitiveConverter {

        private final ParentValueContainer parent;
        private final Class<? extends Enum> enumClass;

        public FieldEnumConverter(ParentValueContainer parent, Schema enumSchema) {
            this.parent = parent;
            this.enumClass = SpecificData.get().getClass(enumSchema);
        }

        @Override
        public void addBinary(Binary value) {
            Object enumValue = value.toStringUsingUTF8();
            if (enumClass != null) {
                enumValue = (Enum.valueOf(enumClass, (String) enumValue));
            }
            parent.add(enumValue);
        }
    }

    static class FieldFixedConverter extends PrimitiveConverter {

        private final ParentValueContainer parent;
        private final Schema avroSchema;
        private final Class<? extends GenericData.Fixed> fixedClass;
        private final Constructor fixedClassCtor;

        public FieldFixedConverter(ParentValueContainer parent, Schema avroSchema) {
            this.parent = parent;
            this.avroSchema = avroSchema;
            this.fixedClass = SpecificData.get().getClass(avroSchema);
            if (fixedClass != null) {
                try {
                    this.fixedClassCtor =
                            fixedClass.getConstructor(new Class[] { byte[].class });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                this.fixedClassCtor = null;
            }
        }

        @Override
        public void addBinary(Binary value) {
            if (fixedClass == null) {
                parent.add(new GenericData.Fixed(avroSchema, value.getBytes()));
            } else {
                if (fixedClassCtor == null) {
                    throw new IllegalArgumentException(
                            "fixedClass specified but fixedClassCtor is null.");
                }
                try {
                    Object fixed = fixedClassCtor.newInstance(value.getBytes());
                    parent.add(fixed);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    static class AvroArrayConverter<T> extends GroupConverter {

        private final ParentValueContainer parent;
        private final Schema avroSchema;
        private final Converter converter;
        private GenericArray<T> array;

        public AvroArrayConverter(ParentValueContainer parent, Type parquetSchema,
                                  Schema avroSchema) {
            this.parent = parent;
            this.avroSchema = avroSchema;
            Type elementType = parquetSchema.asGroupType().getType(0);
            Schema elementSchema = avroSchema.getElementType();
            converter = newConverter(elementSchema, elementType, new ParentValueContainer() {
                @Override
                @SuppressWarnings("unchecked")
                void add(Object value) {
                    array.add((T) value);
                }
            });
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converter;
        }

        @Override
        public void start() {
            array = new GenericData.Array<T>(0, avroSchema);
        }

        @Override
        public void end() {
            parent.add(array);
        }
    }

    static class AvroUnionConverter<T> extends GroupConverter {

        private final ParentValueContainer parent;
        private final Converter[] memberConverters;
        private Object memberValue = null;

        public AvroUnionConverter(ParentValueContainer parent, Type parquetSchema,
                                  Schema avroSchema) {
            this.parent = parent;
            GroupType parquetGroup = parquetSchema.asGroupType();
            this.memberConverters = new Converter[ parquetGroup.getFieldCount()];

            int parquetIndex = 0;
            for (int index = 0; index < avroSchema.getTypes().size(); index++) {
                Schema memberSchema = avroSchema.getTypes().get(index);
                if (!memberSchema.getType().equals(Schema.Type.NULL)) {
                    Type memberType = parquetGroup.getType(parquetIndex);
                    memberConverters[parquetIndex] = newConverter(memberSchema, memberType, new ParentValueContainer() {
                        @Override
                        void add(Object value) {
                            Preconditions.checkArgument(memberValue == null,
                                    "Union is resolving to more than one type");
                            memberValue = value;
                        }
                    });
                    parquetIndex++; // Note for nulls the parquetIndex id not increased
                }
            }
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return memberConverters[fieldIndex];
        }

        @Override
        public void start() {
            memberValue = null;
        }

        @Override
        public void end() {
            parent.add(memberValue);
        }
    }

    static class MapConverter<V> extends GroupConverter {

        private final ParentValueContainer parent;
        private final Converter keyValueConverter;
        private Map<String, V> map;

        public MapConverter(ParentValueContainer parent, Type parquetSchema,
                            Schema avroSchema) {
            this.parent = parent;
            this.keyValueConverter = new MapKeyValueConverter(parquetSchema, avroSchema);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return keyValueConverter;
        }

        @Override
        public void start() {
            this.map = new HashMap<String, V>();
        }

        @Override
        public void end() {
            parent.add(map);
        }

        final class MapKeyValueConverter extends GroupConverter {

            private String key;
            private V value;
            private Converter keyConverter;
            private Converter valueConverter;

            public MapKeyValueConverter(Type parquetSchema, Schema avroSchema) {
                keyConverter = new PrimitiveConverter() {
                    @Override
                    public void addBinary(Binary value) {
                        key = value.toStringUsingUTF8();
                    }
                };

                Type valueType = parquetSchema.asGroupType().getType(0).asGroupType().getType(1);
                Schema valueSchema = avroSchema.getValueType();
                valueConverter = newConverter(valueSchema, valueType, new ParentValueContainer() {
                    @Override
                    @SuppressWarnings("unchecked")
                    void add(Object value) {
                        MapKeyValueConverter.this.value = (V) value;
                    }
                });
            }

            @Override
            public Converter getConverter(int fieldIndex) {
                if (fieldIndex == 0) {
                    return keyConverter;
                } else if (fieldIndex == 1) {
                    return valueConverter;
                }
                throw new IllegalArgumentException("only the key (0) and value (1) fields expected: " + fieldIndex);
            }

            @Override
            public void start() {
                key = null;
                value = null;
            }

            @Override
            public void end() {
                map.put(key, value);
            }
        }
    }

}