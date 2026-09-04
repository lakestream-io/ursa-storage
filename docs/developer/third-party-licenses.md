# Third-party license notes

Ursa Storage itself is licensed under Apache License 2.0. This page records third-party
dependencies whose license differs from Apache 2.0 and how the repository keeps them out of the
binary distribution.

## Distribution scope

`ursa-storage-tools/bin.xml` assembles `ursa-storage-<version>-bin.tar.gz`. Every runtime
dependency of `ursa-storage-tools` (which pulls in `ursa-storage-lakehouse` and
`ursa-storage-compact`) is copied into `lib/`. The compactor and perf images
(`docker/perf.Dockerfile`, `performance/Dockerfile`) unpack that tarball into `/opt/ursa`, so
`lib/` becomes `/opt/ursa/lib` in the image.

## Confluent Schema Registry artifacts (`io.confluent`, 7.9.4)

Licenses below are taken from the `<licenses>` block of each artifact's Maven POM
(inherited from `common-parent` / `kafka-schema-registry-parent` where the child POM has none).

| Artifact | License | Scope in this repository | Shipped in `lib/` |
|----------|---------|--------------------------|-------------------|
| `kafka-schema-registry-client` | Apache 2.0 | compile (`ursa-storage-materialization`, `ursa-storage-containers`) | yes |
| `kafka-avro-serializer`, `kafka-schema-serializer` | Apache 2.0 | compile | yes |
| `common-utils`, `logredactor`, `logredactor-metrics` | Apache 2.0 | transitive via `kafka-avro-serializer` | yes |
| `kafka-json-schema-provider` | Confluent Community License 1.0 | **test only** (transitive of the test serializers) | **no** |
| `kafka-protobuf-provider`, `kafka-protobuf-types` | Confluent Community License 1.0 | **test only** (transitive of the test serializers) | **no** |
| `kafka-json-schema-serializer`, `kafka-protobuf-serializer` | Confluent Community License 1.0 | **test only** (`ursa-storage-lakehouse`) | **no** |

The Confluent Community License Agreement 1.0 is source-available, not OSI-approved open source.
Its grant excludes the "Excluded Purpose" (offering the software as a competing managed service)
and §1.2(b) requires the CCL notice on every distributed copy. Neither applies to the
distribution because no CCL artifact is shipped. Test-scope use inside the build is ordinary use
under the license and does not redistribute anything. Full text:
<https://www.confluent.io/confluent-community-license/>

Upstream metadata is not consistent for every module of `confluentinc/schema-registry` (checked at
tag `v7.9.4`). The repository README states that the license is given by the `LICENSE` file of
each subfolder:

| Upstream module | Artifact POM `<licenses>` | Subfolder `LICENSE` | Source file headers |
|-----------------|---------------------------|---------------------|---------------------|
| `protobuf-provider`, `json-schema-provider` | CCL | CCL | CCL |
| `protobuf-serializer`, `json-schema-serializer` | CCL | Apache 2.0 | Apache 2.0 |
| `protobuf-types` | CCL | Apache 2.0 | (generated code, `.proto` without header) |

The providers are unambiguously CCL, which is why they were removed from the runtime path. The
serializer and types artifacts are treated conservatively as CCL for dependency purposes (POM
metadata is what license scanners read) and kept in test scope; their *sources* are Apache 2.0 per
the subfolder `LICENSE`, which is what the attribution on the ported test files relies on.

## How JSON Schema and Protobuf are supported without the CCL artifacts

The registry protocol is still spoken through the Apache-2.0 `kafka-schema-registry-client`; only
the schema *parsing* side was replaced. Everything lives in
`ursa-storage-materialization/src/main/java/io/lakestream/ursa/materialization/serde/kafka/schema/`:

| Concern | Before | Now |
|---------|--------|-----|
| Registry client providers for `JSON` / `PROTOBUF` ids | `JsonSchemaProvider`, `ProtobufSchemaProvider` (CCL) | `RawSchemaProvider` / `RawParsedSchema` keep the schema text opaque |
| Payload framing and Protobuf message indexes | `MessageIndexes` (CCL) | `SchemaRegistryWireFormat` |
| `.proto` text → protobuf descriptors, message selection | `ProtobufSchema` (CCL) | `ProtobufSchemaDescriptors` on top of `io.apicurio:apicurio-registry-protobuf-schema-utilities` (Apache 2.0) and `com.squareup.wire:wire-schema-jvm` (Apache 2.0) |
| JSON payload decoding | forked `KafkaJsonSchemaDeserializer` | `SchemaRegistryJsonDeserializer` (Jackson) |
| Protobuf payload decoding | forked `KafkaProtobufDeserializer` | `SchemaRegistryProtobufDeserializer` (`DynamicMessage`) |
| JSON Schema → table schema | own `serde.utils.json.schema.JsonSchema` | unchanged |

Behavioural notes of the replacement:

- Message indexes, message-name resolution and Avro/Iceberg/Delta type mapping are identical to
  the Confluent implementation (verified against it, including enums and synthesized map-entry
  types being skipped when counting nested messages).
- Descriptor fields follow the declaration order of the `.proto` text, as `protoc` produces. The
  Confluent converter moved `oneof` members to the front; this only affects the column order of
  newly created tables (existing tables evolve by name).
- Schemas without `java_outer_classname` / `java_multiple_files` now derive their Avro namespace
  from the file name `schema.proto`; the Confluent path failed on them.
- Schema references (imports of other registry subjects) and Confluent-specific imports
  (`confluent/meta.proto`, `confluent/type/decimal.proto`) are not resolved, exactly as before.
  `FileDescriptorUtils` accepts dependency maps, so reference support can be added there later.
- JSON payloads are not validated against the JSON Schema, matching the previous default
  (`json.fail.invalid.schema=false`).

New Apache-2.0 / permissive runtime dependencies introduced by the parser: `apicurio-registry-
protobuf-schema-utilities`, `wire-schema-jvm`, `okio-fakefilesystem`, `kotlinx-datetime`,
`kotlinpoet-jvm`, `javapoet`. The unused `icu4j` dependency of the Apicurio module is excluded.
Removing the CCL providers also dropped the Kotlin scripting compiler, everit/json-sKema, joda-time
and commons-validator/collections from `lib/`.

## Guard rails

- `maven-enforcer-plugin` (root `pom.xml`, execution `ban-non-apache-runtime-dependencies`) fails
  the build if any `io.confluent:kafka-json-schema-*` or `io.confluent:kafka-protobuf-*` artifact
  appears in compile, runtime or provided scope of any module.
- `ursa-storage-lakehouse` tests keep the Confluent serializers in test scope to produce realistic
  payloads and to run the upstream serializer round-trip tests. Those tests give the serializer its
  own registry client with the Confluent providers; the code under test uses `RawSchemaProvider`.
- No Confluent-authored `.proto` definitions are kept in the source tree. Test fixtures that use the
  `confluent.field_meta` options or `confluent.type.Decimal` import `confluent/meta.proto` and
  `confluent/type/decimal.proto` straight from the test-scope `kafka-protobuf-types` jar:
  `protobuf-maven-plugin` extracts `.proto` files from dependency jars into
  `target/protoc-dependencies` and adds them to the protoc import path, and the generated
  `io.confluent.protobuf.*` classes come from the same jar.
- The ported upstream serializer tests (`ursa-storage-lakehouse/src/test/java/io/lakestream/ursa/kafka/serde/`)
  keep Confluent's Apache-2.0 file notice from the original test sources.
- The `.proto` fixtures in `ursa-storage-lakehouse/src/test/proto/` that were ported verbatim from
  `protobuf-serializer/src/test/proto` (all except `test_variant.proto` and `TestProtoMessages.proto`;
  only `package` / `java_package` were renamed) carry the Confluent Apache-2.0 attribution header
  defined in `resources/license-confluent-apache.template`. `license:check` accepts that header as
  an alternative to the repository header (`validHeaders` in the root `pom.xml`) and fails on any
  other header, so the attribution cannot silently drift.

## Re-verifying

```bash
# No CCL artifact may reach the distribution
mvn -B -ntp clean install -DskipTests
tar -tzf ursa-storage-tools/target/ursa-storage-*-bin.tar.gz | grep -E 'lib/kafka-(json-schema|protobuf)-' && echo FAIL

# Which io.confluent artifacts reach the distribution and through which module
mvn -B -ntp dependency:tree -pl ursa-storage-tools -Dincludes=io.confluent

# License declared by an artifact POM (falls back to the parent POM when absent)
sed -n '/<licenses>/,/<\/licenses>/p' \
  ~/.m2/repository/io/confluent/kafka-protobuf-provider/7.9.4/kafka-protobuf-provider-7.9.4.pom
```
