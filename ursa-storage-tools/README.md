# Ursa Storage Tools

This module provides a comprehensive suite of command-line tools for operating and testing Ursa Storage. It includes administrative utilities for managing streams and compaction tasks, performance benchmarking tools for measuring throughput and latency, and diagnostic tools for troubleshooting storage issues.

## Pre-requisites
- Java 17
- Maven 3.6.3+
- Oxia Cluster

## Set up Oxia Cluster
You can follow the instructions to set up a local Oxia Cluster [here](https://github.com/oxia-db/oxia/blob/main/docs/bare-metal-deploy.md)

## Build
```bash
$ git clone git@github.com:lakestream-io/ursa-storage.git
$ cd ursa-storage
$ mvn -T 3C clean install -DskipTests
```

After build, you can find the performance test tools in the `ursa-storage-tools/target` directory.

## Performance Test Tools
The Performance test tool contains the following components:
- Performance Producer: A tool to produce messages to Ursa storage.
- Performance Consumer: A tool to consume messages from Ursa storage.

### Performance Producer
Performance Producer Usage:
```
$ bin/ursa produce --help
Usage: ursa-storage-produce-perf [options]
  Options:
    -b, --bucket
      Bucket name
    -ef, --exit-on-failure
      Exit from the process on publish failure (default: disable)
      Default: false
    -h, --help
      Help message
    --histogram-file
      HdrHistogram output file
    -size, --message-size
      Size of the message in bytes. Default is 1024
      Default: 1024
    -m, --num-messages
      Number of messages to write in total. If <= 0, it will keep writing.
      Default is 0
      Default: 0
    -s, --num-streams
      Number of streams. Default is 1
      Default: 1
  * -o, --oxia-url
      Oxia URL
    --port
      Port for Prometheus metrics. Default is 8099
      Default: 8099
    -p, --prefix
      Bucket storage prefix
      Default: storage-test
    -r, --rate
      Write rate msg/s across streams. Default is 10_000
      Default: 10000
    -rg, --region
      Bucket region
    -sp, --storagePath
      File storage path
    -time, --test-duration
      Test duration in secs. If <= 0, it will keep publishing. Default is 0
      Default: 0
    -th, --threads
      Number of threads to use. Default is 1
      Default: 1
    -w, --warmup-time
      Warmup time in seconds. Default is 30
      Default: 30
```

You can configure the ursa-storage lib configurations in `conf/ursa-storage.conf` file.

One example to run the performance producer:
```bash
$ bin/ursa produce -sp data -o localhost:6648 -th 3 -s 10 -r 20000
```

### Performance Consumer
Performance Consumer Usage:
```
$ bin/ursa consume --help
Usage: ursa-storage-consume-perf [options]
  Options:
    -bs, --batch-size
      Batch entries in one request. Default is 1000.
      Default: 1000
    -b, --bucket
      Bucket name
    -bfs, --buffer-size
      Buffer size for each consumer. Default is 1MB.
      Default: 1048576
    -e, --end-streamId
      End stream id
      Default: 9223372036854775807
    -ef, --exit-on-failure
      Exit from the process on publish failure (default: disable)
      Default: false
    -h, --help
      Help message
    -hf, --histogram-file
      HdrHistogram output file
  * -o, --oxia-url
      Oxia URL
    -port, --port
      Port for Prometheus metrics. Default is 8099
      Default: 8098
    -p, --prefix
      Bucket storage prefix
      Default: storage-test
    -r, --rate
      Fetch rate msg/s across streams. Default is 10_000
      Default: 100000
    -rg, --region
      Bucket region
    -s, --start-streamId
      Start stream id
      Default: 0
    -sp, --storagePath
      File storage path
    -th, --threads
      Number of threads to fetch messages. Default is 1.
      Default: 1
    -t, --time
      Time to run in seconds. If <= 0, it will keep consuming
      Default: 0
```

Supported features:
- Auto-detect new streamIds and start consuming from them.
- Fetch messages from multiple streams concurrently.
- Keeps track of the last consumed message and start consuming from there.

You can configure the ursa-storage lib configurations in `conf/ursa-storage.conf` file.

One example to run the performance producer:
```bash
$ bin/ursa consume -o localhost:6648 -sp data -th 3
```
