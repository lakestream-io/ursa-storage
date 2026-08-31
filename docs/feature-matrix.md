# Ursa Feature Matrix

Each feature below lists supported options as columns. ✔ = Supported  ✖ = Not Supported

## Data Formats

### Stored Entry Payload

|                | Kafka MemoryRecords | Arrow |
|:---------------|:-------------------:|:-----:|
| **Supported**  |          ✔          |  ✖    |


### WO Format

|                  | Ursa WAL Format |
|:-----------------|:---------------:|
| **Supported**    |        ✔        |

### CO Format

|                  | Ursa WAL Format | Parquet | Kafka Segment Format |
|:-----------------|:---------------:|:-------:|:--------------------:|
| **Supported**    |        ✔        |   ✔     |         ✖            |

## Storage

### WAL Storage

Currently Ursa Storage supports the following storage as WAL storage.

**Latency Optimized**

- AWS FSx (✖)
- Distributed filesystem (✖)
- Regional disks (✖)

**Cost Optimized**

|                  | AWS S3 | GCS | Azure Blob Store |
|:-----------------|:------:|:---:|:----------------:|
| **Supported**    |  ✔     | ✔   |   ✔              |

### Lakehouse Storage

|                  | AWS S3 | GCS | Azure Blob Store | HDFS |
|:-----------------|:------:|:---:|:----------------:|:----:|
| **Supported**    |  ✔     | ✔   |   ✔              | ✖    |

## Schema

Ursa Storage supports Kafka schemas for materialization.

### Kafka Schema

The supported Kafka schemas are listed as below.

|                | AVRO | JSON | PROTOBUF |
|:---------------|:----:|:----:|:--------:|
| **Supported**  | ✔    | ✔    |  ✔       |

The Kafka schema integration works with the following schema registry:

- Lakestream Kafka Schema Registry
- Confluent Schema Registry

## Lakehouse Tables

### Table Format

|               | Apache Iceberg | Delta Lake | Apache Hudi | Apache Paimon | Lance |
|:--------------|:--------------:|:----------:|:-----------:|:-------------:|:-----:|
| **Supported** |       ✔        |     ✔      |     ✖       |      ✖        |  ✖    |

### Lakehouse Catalog

|                        | Databricks Unity Catalog | Apache Iceberg REST Catalog | Snowflake Open Catalog | AWS Glue Catalog |
|:-----------------------|:-----------------------:|:--------------------------:|:---------------------:|:---------------:|
| **Supported**          |           ✔             |            ✔               |          ✔            |        ✖        |
