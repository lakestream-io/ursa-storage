/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProtobufSchemaDescriptorsTest {

    static final String SCHEMA = """
            syntax = "proto3";
            package test.schema;
            import "google/protobuf/timestamp.proto";
            option java_outer_classname = "TestProtos";

            enum Color { RED = 0; BLUE = 1; }

            message User {
              string name = 1;
              int32 age = 2;
              message Address { string city = 1; message Geo { double lat = 1; double lon = 2; } Geo geo = 2; }
              Address address = 3;
              map<string, int64> counters = 4;
              google.protobuf.Timestamp created_at = 5;
              oneof contact { string email = 6; string phone = 7; }
              uint32 flags = 8;
              Color color = 9;
              repeated string tags = 10;
            }

            message Order {
              string item = 1;
              double price = 2;
            }
            """;

    @Test
    void compilesFileAndResolvesMessagesByIndexes() {
        FileDescriptor file = ProtobufSchemaDescriptors.fileDescriptor(SCHEMA);
        assertThat(file.getName()).isEqualTo(ProtobufSchemaDescriptors.SCHEMA_FILE_NAME);
        assertThat(file.getPackage()).isEqualTo("test.schema");
        assertThat(file.getMessageTypes()).extracting(Descriptor::getName).containsExactly("User", "Order");

        // Indexes count message types only; the enum declared first does not shift them.
        assertThat(ProtobufSchemaDescriptors.messageNameByIndexes(SCHEMA, List.of(0))).isEqualTo("test.schema.User");
        assertThat(ProtobufSchemaDescriptors.messageNameByIndexes(SCHEMA, List.of(1))).isEqualTo("test.schema.Order");
        assertThat(ProtobufSchemaDescriptors.messageNameByIndexes(SCHEMA, List.of(0, 0)))
                .isEqualTo("test.schema.User.Address");
        assertThat(ProtobufSchemaDescriptors.messageNameByIndexes(SCHEMA, List.of(0, 0, 0)))
                .isEqualTo("test.schema.User.Address.Geo");
    }

    @Test
    void resolvesMessagesByQualifiedOrRelativeName() {
        assertThat(ProtobufSchemaDescriptors.messageByName(SCHEMA, "test.schema.User").getFullName())
                .isEqualTo("test.schema.User");
        assertThat(ProtobufSchemaDescriptors.messageByName(SCHEMA, "Order").getFullName())
                .isEqualTo("test.schema.Order");
        assertThat(ProtobufSchemaDescriptors.messageByName(SCHEMA, "test.schema.User.Address.Geo").getFullName())
                .isEqualTo("test.schema.User.Address.Geo");
        assertThat(ProtobufSchemaDescriptors.messageByName(SCHEMA, "User.Address").getFullName())
                .isEqualTo("test.schema.User.Address");
    }

    @Test
    void keepsDeclaredFieldOrderIncludingOneOfMembers() {
        Descriptor user = ProtobufSchemaDescriptors.messageByName(SCHEMA, "User");
        assertThat(user.getFields()).extracting(FieldDescriptor::getName).containsExactly(
                "name", "age", "address", "counters", "created_at", "email", "phone", "flags", "color", "tags");
        assertThat(user.findFieldByName("email").getContainingOneof().getName()).isEqualTo("contact");
        assertThat(user.findFieldByName("counters").isMapField()).isTrue();
        assertThat(user.findFieldByName("created_at").getMessageType().getFullName())
                .isEqualTo("google.protobuf.Timestamp");
        assertThat(user.findFieldByName("color").getEnumType().getFullName()).isEqualTo("test.schema.Color");
        assertThat(user.findFieldByName("flags").getType()).isEqualTo(FieldDescriptor.Type.UINT32);
        assertThat(user.getFile().getOptions().getJavaOuterClassname()).isEqualTo("TestProtos");
    }

    @Test
    void keepsDeclaredFieldOrderInNestedMessages() {
        String schema = """
                syntax = "proto3";
                message Outer {
                  message Inner {
                    oneof kind { int32 a = 1; int32 b = 2; }
                    string c = 3;
                    oneof other { int32 d = 4; }
                    string e = 5;
                  }
                  Inner inner = 1;
                }
                """;
        Descriptor inner = ProtobufSchemaDescriptors.messageByName(schema, "Outer.Inner");
        assertThat(inner.getFields()).extracting(FieldDescriptor::getName).containsExactly("a", "b", "c", "d", "e");
    }

    @Test
    void messageIndexesSkipSynthesizedMapEntryTypes() {
        // Serializers derive indexes from the declared messages; the MEntry type protoc synthesizes for the
        // map field must not shift the nested message N even though it precedes N in the descriptor.
        String schema = """
                syntax = "proto3";
                package p;
                message M {
                  map<string, int32> m = 1;
                  enum E { A = 0; }
                  message N { int32 b = 1; }
                  N n = 2;
                  map<string, N> others = 3;
                  message O { int32 c = 1; }
                }
                """;
        Descriptor m = ProtobufSchemaDescriptors.messageByName(schema, "p.M");
        assertThat(m.getNestedTypes()).extracting(Descriptor::getName).contains("MEntry", "N", "OthersEntry", "O");
        assertThat(ProtobufSchemaDescriptors.messageNameByIndexes(schema, List.of(0, 0))).isEqualTo("p.M.N");
        assertThat(ProtobufSchemaDescriptors.messageNameByIndexes(schema, List.of(0, 1))).isEqualTo("p.M.O");
        assertThatThrownBy(() -> ProtobufSchemaDescriptors.messageByIndexes(schema, List.of(0, 2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2 declared message types");
    }

    @Test
    void supportsSchemasWithoutPackageAndProto2Syntax() {
        String schema = """
                syntax = "proto2";
                message Event {
                  required string id = 1;
                  optional int32 count = 2 [default = 5];
                  repeated string labels = 3;
                }
                """;
        Descriptor event = ProtobufSchemaDescriptors.messageByIndexes(schema, List.of(0));
        assertThat(event.getFullName()).isEqualTo("Event");
        assertThat(event.findFieldByName("id").isRequired()).isTrue();
        assertThat(event.findFieldByName("count").getDefaultValue()).isEqualTo(5);
        assertThat(ProtobufSchemaDescriptors.messageByName(schema, "Event")).isSameAs(event);
    }

    @Test
    void cachesCompiledFiles() {
        assertThat(ProtobufSchemaDescriptors.fileDescriptor(SCHEMA))
                .isSameAs(ProtobufSchemaDescriptors.fileDescriptor(SCHEMA));
    }

    @Test
    void reportsInvalidIndexesNamesAndSchemas() {
        assertThatThrownBy(() -> ProtobufSchemaDescriptors.messageByIndexes(SCHEMA, List.of(2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("[2]");
        assertThatThrownBy(() -> ProtobufSchemaDescriptors.messageByIndexes(SCHEMA, List.of(1, 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("test.schema.Order");
        assertThatThrownBy(() -> ProtobufSchemaDescriptors.messageByIndexes(SCHEMA, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ProtobufSchemaDescriptors.messageByName(SCHEMA, "test.schema.Missing"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("test.schema.Missing");
        assertThatThrownBy(() -> ProtobufSchemaDescriptors.fileDescriptor("message {"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Protobuf schema");
        assertThatThrownBy(() -> ProtobufSchemaDescriptors.fileDescriptor(
                "syntax = \"proto3\"; message A { Missing m = 1; }"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
