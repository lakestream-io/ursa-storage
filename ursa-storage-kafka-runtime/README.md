# Ursa Kafka Runtime

This leaf artifact assembles the Ursa-owned runtime used when Kafka records are stored through
Lakestream:

- artifact: `io.lakestream:ursa-storage-kafka-runtime`
- Lakestream provider: `io.lakestream.ursa.kafka.runtime.UrsaKafkaStreamCatalogProvider`
- provider discovery: `java.util.ServiceLoader` through `lakestream-api`
- compacted data reader: `KafkaLakehouseReaderFactory`

The runtime owns storage, catalog metadata, compacted-reader, and OpenTelemetry bootstrap and
transfers their lifecycle to the returned `StreamCatalog`. It also translates generic storage
properties into the internal Kafka compacted-reader settings.

This module is deliberately a dependency-graph leaf. It depends on `lakestream-api`,
`ursa-storage-core`, `ursa-storage-lakestream`, and `ursa-storage-lakehouse-kafka-reader`; none of
those modules depends back on this runtime or on the Kafka reader. The artifact does not depend on
any `org.apache.kafka` artifact. Kafka brokers consume only `lakestream-api` at compile time and
place this runtime, including its transitive implementation dependencies, on their isolated Ursa
runtime classpath.
