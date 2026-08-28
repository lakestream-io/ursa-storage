# Protobuf Schema In Ursa

Kafka supports Protobuf schema.

Protobuf schema allows define multiple messages in one proto file. Like this:

```
syntax = "proto3";

package io.lakestream.ursa.test;

message A {
    ...
}

message B {
    ...
}
```

When you sending messages with B in Kafka, it will generate a MessageIndex to identify which message type it is.

This brings a huge challenge to Ursa to do the schema evolution. It means we can not get the real topic schema from the Schema registry without reading data.

But if we only allow the topic message type defined at the first place, it will allow us to do the schema evolution without reading data.

For example, you can define the protobuf message like this:

```
syntax = "proto3";

package io.lakestream.ursa.test;

message A {
    B b = 1;
}

message B {
    ...
}
```
message A is the one you used to send message for topic `test-topic`. But you can not use message B to send message for topic `other-topic`. If you want to send message B for topic `other-topic`, you need to define a new proto file.

```
syntax = "proto3";

package io.lakestream.ursa.test;

message B {
    ...
}
```