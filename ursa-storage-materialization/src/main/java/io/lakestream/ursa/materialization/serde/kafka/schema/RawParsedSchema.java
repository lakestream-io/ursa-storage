/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka.schema;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link ParsedSchema} that keeps the registry schema text opaque.
 *
 * <p>The schema registry client needs a {@code ParsedSchema} for every schema type it fetches, but the
 * materialization path only consumes the schema <em>text</em> (JSON Schema documents are converted with
 * {@code serde.utils.json.schema.JsonSchema}; Protobuf text is compiled by
 * {@link ProtobufSchemaDescriptors}). Keeping the text opaque avoids depending on the Confluent Community
 * License JSON Schema and Protobuf provider artifacts. Compatibility checks and tag editing are not
 * supported and throw {@link UnsupportedOperationException}.
 */
public final class RawParsedSchema implements ParsedSchema {

    private final String schemaType;
    private final String schema;
    private final List<SchemaReference> references;
    private final Metadata metadata;
    private final RuleSet ruleSet;
    private final Integer version;

    public RawParsedSchema(String schemaType, String schema, List<SchemaReference> references,
                           Metadata metadata, RuleSet ruleSet, Integer version) {
        this.schemaType = Objects.requireNonNull(schemaType, "schemaType");
        this.schema = Objects.requireNonNull(schema, "schema");
        this.references = references == null ? List.of() : List.copyOf(references);
        this.metadata = metadata;
        this.ruleSet = ruleSet;
        this.version = version;
    }

    @Override
    public String schemaType() {
        return schemaType;
    }

    /** Raw schemas carry no record name; subject name strategies that need one are not supported. */
    @Override
    public String name() {
        return null;
    }

    @Override
    public String canonicalString() {
        return schema;
    }

    @Override
    public Integer version() {
        return version;
    }

    @Override
    public List<SchemaReference> references() {
        return references;
    }

    @Override
    public Metadata metadata() {
        return metadata;
    }

    @Override
    public RuleSet ruleSet() {
        return ruleSet;
    }

    @Override
    public ParsedSchema copy() {
        return new RawParsedSchema(schemaType, schema, references, metadata, ruleSet, version);
    }

    @Override
    public ParsedSchema copy(Integer newVersion) {
        return new RawParsedSchema(schemaType, schema, references, metadata, ruleSet, newVersion);
    }

    @Override
    public ParsedSchema copy(Metadata newMetadata, RuleSet newRuleSet) {
        return new RawParsedSchema(schemaType, schema, references, newMetadata, newRuleSet, version);
    }

    @Override
    public ParsedSchema copy(Map<SchemaEntity, Set<String>> tagsToAdd, Map<SchemaEntity, Set<String>> tagsToRemove) {
        throw new UnsupportedOperationException("Tag editing is not supported for raw " + schemaType + " schemas");
    }

    @Override
    public List<String> isBackwardCompatible(ParsedSchema previousSchema) {
        throw new UnsupportedOperationException(
                "Compatibility checks are not supported for raw " + schemaType + " schemas");
    }

    /** The schema text; raw schemas have no structured representation. */
    @Override
    public Object rawSchema() {
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RawParsedSchema other)) {
            return false;
        }
        return schemaType.equals(other.schemaType)
                && schema.equals(other.schema)
                && references.equals(other.references)
                && Objects.equals(metadata, other.metadata)
                && Objects.equals(ruleSet, other.ruleSet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaType, schema, references, metadata, ruleSet);
    }

    @Override
    public String toString() {
        return schema;
    }
}
