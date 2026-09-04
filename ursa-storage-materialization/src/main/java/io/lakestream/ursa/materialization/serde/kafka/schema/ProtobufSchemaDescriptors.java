/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka.schema;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import com.squareup.wire.schema.internal.parser.TypeElement;
import io.apicurio.registry.utils.protobuf.schema.FileDescriptorUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Compiles Schema Registry Protobuf schema text ({@code .proto} source) into protobuf descriptors and
 * resolves the message type addressed by a payload.
 *
 * <p>A registered Protobuf schema is a single {@code .proto} file. Payloads reference a message inside that
 * file either positionally, through the wire-format message indexes (index into the file's message types,
 * then into each selected message's nested message types), or by fully qualified name. Serializers compute
 * the indexes from the schema text, so only <em>declared</em> messages count: enums and the map-entry
 * messages synthesized for {@code map<K, V>} fields are skipped. Compiled files are cached by schema text
 * because compilation links the file against the well-known Google types.
 */
public final class ProtobufSchemaDescriptors {

    /**
     * File name given to compiled schemas. It also determines the Avro namespace derived by
     * {@code org.apache.avro.protobuf.ProtobufData} when a schema declares neither
     * {@code java_outer_classname} nor {@code java_multiple_files}.
     */
    public static final String SCHEMA_FILE_NAME = "schema.proto";

    private static final int MAX_CACHED_SCHEMAS = Integer.getInteger("ursa.maxProtobufSchemaCacheSize", 1024);

    private static final Cache<String, FileDescriptor> FILE_DESCRIPTORS = CacheBuilder.newBuilder()
            .maximumSize(MAX_CACHED_SCHEMAS)
            .build();

    private ProtobufSchemaDescriptors() {
    }

    /**
     * Compiles the schema text into a {@link FileDescriptor}.
     *
     * @throws IllegalArgumentException when the text is not a valid, self-contained {@code .proto} file
     */
    public static FileDescriptor fileDescriptor(String schema) {
        Objects.requireNonNull(schema, "schema");
        try {
            return FILE_DESCRIPTORS.get(schema, () -> compile(schema));
        } catch (ExecutionException | UncheckedExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new IllegalArgumentException("Invalid Protobuf schema: " + cause.getMessage(), cause);
        }
    }

    /** Resolves the message selected by wire-format message indexes. */
    public static Descriptor messageByIndexes(String schema, List<Integer> messageIndexes) {
        return messageByIndexes(fileDescriptor(schema), messageIndexes);
    }

    /** Resolves the message selected by wire-format message indexes. */
    public static Descriptor messageByIndexes(FileDescriptor file, List<Integer> messageIndexes) {
        Objects.requireNonNull(messageIndexes, "messageIndexes");
        if (messageIndexes.isEmpty()) {
            throw new IllegalArgumentException("Message indexes must not be empty");
        }
        List<Descriptor> candidates = declaredMessages(file.getMessageTypes());
        Descriptor selected = null;
        for (int index : messageIndexes) {
            if (index < 0 || index >= candidates.size()) {
                throw new IllegalArgumentException("Invalid message indexes " + messageIndexes
                        + ": index " + index + " is out of range for "
                        + (selected == null ? "file " + file.getName() : "message " + selected.getFullName())
                        + " with " + candidates.size() + " declared message types");
            }
            selected = candidates.get(index);
            candidates = declaredMessages(selected.getNestedTypes());
        }
        return selected;
    }

    private static List<Descriptor> declaredMessages(List<Descriptor> messages) {
        List<Descriptor> declared = new ArrayList<>(messages.size());
        for (Descriptor message : messages) {
            if (!message.getOptions().getMapEntry()) {
                declared.add(message);
            }
        }
        return declared;
    }

    /** Returns the fully qualified name of the message selected by wire-format message indexes. */
    public static String messageNameByIndexes(String schema, List<Integer> messageIndexes) {
        return messageByIndexes(schema, messageIndexes).getFullName();
    }

    /**
     * Resolves a message by name. The name may be fully qualified ({@code pkg.Outer.Inner}) or relative to
     * the file's package ({@code Outer.Inner}).
     */
    public static Descriptor messageByName(String schema, String messageName) {
        return messageByName(fileDescriptor(schema), messageName);
    }

    /**
     * Resolves a message by name. The name may be fully qualified ({@code pkg.Outer.Inner}) or relative to
     * the file's package ({@code Outer.Inner}).
     */
    public static Descriptor messageByName(FileDescriptor file, String messageName) {
        Objects.requireNonNull(messageName, "messageName");
        String relativeName = messageName;
        String packageName = file.getPackage();
        if (!packageName.isEmpty() && messageName.startsWith(packageName + ".")) {
            relativeName = messageName.substring(packageName.length() + 1);
        }
        String[] path = relativeName.split("\\.");
        Descriptor descriptor = file.findMessageTypeByName(path[0]);
        for (int i = 1; descriptor != null && i < path.length; i++) {
            descriptor = descriptor.findNestedTypeByName(path[i]);
        }
        if (descriptor == null) {
            throw new IllegalArgumentException("Message " + messageName + " is not defined in the Protobuf schema"
                    + (packageName.isEmpty() ? "" : " of package " + packageName));
        }
        return descriptor;
    }

    private static FileDescriptor compile(String schema) throws DescriptorValidationException {
        ProtoFileElement element = ProtoParser.Companion.parse(Location.get(SCHEMA_FILE_NAME), schema);
        FileDescriptor compiled = FileDescriptorUtils.protoFileToFileDescriptor(
                schema, SCHEMA_FILE_NAME, Optional.ofNullable(element.getPackageName()));
        return restoreDeclarationOrder(compiled, element);
    }

    /**
     * The descriptor converter groups {@code oneof} members after the regular fields. Table columns are
     * derived from descriptor field order, so put every message's fields back into the order in which the
     * schema text declares them, as {@code protoc} would.
     */
    private static FileDescriptor restoreDeclarationOrder(FileDescriptor compiled, ProtoFileElement element)
            throws DescriptorValidationException {
        FileDescriptorProto.Builder file = compiled.toProto().toBuilder();
        Map<String, MessageElement> declared = messagesByName(element.getTypes());
        boolean reordered = false;
        for (int i = 0; i < file.getMessageTypeCount(); i++) {
            DescriptorProto.Builder message = file.getMessageTypeBuilder(i);
            MessageElement declaration = declared.get(message.getName());
            if (declaration != null) {
                reordered |= restoreDeclarationOrder(message, declaration);
            }
        }
        if (!reordered) {
            return compiled;
        }
        return FileDescriptor.buildFrom(file.build(), compiled.getDependencies().toArray(new FileDescriptor[0]));
    }

    private static boolean restoreDeclarationOrder(DescriptorProto.Builder message, MessageElement declaration) {
        Map<Integer, Location> declaredAt = new HashMap<>();
        for (FieldElement field : declaration.getFields()) {
            declaredAt.put(field.getTag(), field.getLocation());
        }
        for (OneOfElement oneOf : declaration.getOneOfs()) {
            for (FieldElement field : oneOf.getFields()) {
                declaredAt.put(field.getTag(), field.getLocation());
            }
        }
        List<FieldDescriptorProto> fields = message.getFieldList();
        List<Integer> order = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            order.add(i);
        }
        // Fields without a declaration (e.g. synthesized ones) keep their relative position at the end.
        Comparator<Integer> byDeclaration = Comparator
                .comparingInt((Integer i) -> declaredAt.containsKey(fields.get(i).getNumber()) ? 0 : 1)
                .thenComparingInt(i -> positionOf(declaredAt.get(fields.get(i).getNumber()), i).line())
                .thenComparingInt(i -> positionOf(declaredAt.get(fields.get(i).getNumber()), i).column());
        order.sort(byDeclaration);

        boolean reordered = false;
        List<FieldDescriptorProto> sorted = new ArrayList<>(fields.size());
        for (int i = 0; i < order.size(); i++) {
            reordered |= order.get(i) != i;
            sorted.add(fields.get(order.get(i)));
        }
        if (reordered) {
            message.clearField().addAllField(sorted);
        }

        Map<String, MessageElement> nested = messagesByName(declaration.getNestedTypes());
        for (int i = 0; i < message.getNestedTypeCount(); i++) {
            DescriptorProto.Builder nestedMessage = message.getNestedTypeBuilder(i);
            MessageElement nestedDeclaration = nested.get(nestedMessage.getName());
            if (nestedDeclaration != null) {
                reordered |= restoreDeclarationOrder(nestedMessage, nestedDeclaration);
            }
        }
        return reordered;
    }

    private record Position(int line, int column) {
    }

    private static Position positionOf(Location location, int fallbackIndex) {
        return location == null ? new Position(Integer.MAX_VALUE, fallbackIndex)
                : new Position(location.getLine(), location.getColumn());
    }

    private static Map<String, MessageElement> messagesByName(List<TypeElement> types) {
        Map<String, MessageElement> messages = new HashMap<>();
        for (TypeElement type : types) {
            if (type instanceof MessageElement message) {
                messages.put(message.getName(), message);
            }
        }
        return messages;
    }
}
