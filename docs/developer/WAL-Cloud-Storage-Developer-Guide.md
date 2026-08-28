# Developer Documentation for Ursa Storage WAL System

## Overview

The Ursa Storage Write-Ahead Log (WAL) is a Java library that provides a high-performance, durable storage solution designed to use cloud storage (such as Amazon S3, Google Cloud Storage, or Azure Blob Storage) as the backend. The library implements a write-ahead logging pattern to ensure data durability while optimizing for performance through intelligent buffering, caching, and batching strategies.

This WAL library serves as the underlying storage layer for various distributed systems including:
- Apache Kafka integration: For durable message storage
- Kafka integrations: For event-streaming data persistence
- Other distributed systems requiring durable, high-throughput storage

The WAL library is a critical component that provides:
- Durable storage of data using cloud object stores
- High throughput write and read operations
- Efficient metadata management using Oxia
- Configurable performance characteristics to adapt to different workloads

## Architecture

The Ursa Storage WAL system follows a layered architecture:

1. **Interface Layer**: Defines the contract for WAL operations through the `WalStorage` interface
2. **Implementation Layer**: Provides concrete implementations like `SimpleStorageImpl`
3. **Storage Layer**: Abstracts the underlying storage backends through the `FileStorage` interface
4. **Metadata Layer**: Uses Oxia as a metadata service to track indexes and stream state
5. **Cache Layer**: Implements write and read caching for performance optimization

### High-Level Architecture Diagram

```
+---------------+     +---------------+     +---------------+
|               |     |               |     |               |
|  Kafka Integration       |     | Other Adapters|     | Other Systems |
+-------+-------+     +-------+-------+     +-------+-------+
        |                     |                     |
        |                     |                     |
        +---------------------+---------------------+
                              |
                              v
                     +-------+-------+     +---------------+
                     |               |     |               |
                     | Ursa Storage  |<--->| Oxia Metadata |
                     |  WAL Library  |     |   Service     |
                     +-------+-------+     +---------------+
                              |
                              |
                     +-------+-------+
                     |  Write/Read   |
                     |    Cache      |
                     +-------+-------+
                              |
                              |
                     +-------+-------+
                     |  FileStorage  |
                     | Implementation|
                     +-------+-------+
                              |
                              |
                     +-------+-------+
                     | Cloud Storage |
                     | (S3/GCS/Azure)|
                     +---------------+
```

The diagram shows how the Ursa Storage WAL library serves as the underlying storage layer for various systems like Kafka integrations and other streaming systems. These systems interact with the WAL library through its API, while the library handles all the complexities of data persistence, caching, and cloud storage integration.

## Key Components

### WalStorage Interface

The `WalStorage` interface defines the core operations for the WAL system:

- `initialize()`: Sets up the WAL storage system
- `put(id, buf)`: Writes data to the WAL
- `get(id, offset, compactedIndex)`: Retrieves data from the WAL
- `delete(id, positions)`: Deletes entries from the WAL
- `preFetch(id, positions)`: Preloads data into the read cache
- `close()`: Cleans up resources

#### Understanding the Stream ID

The `id` parameter in the WalStorage interface methods represents a stream ID, which is a critical concept in the WAL system:

- **Stream ID Definition**: A stream ID uniquely identifies a logical sequence of related entries in the WAL. In the context of streaming services:
  - In a Kafka integration, a stream ID typically corresponds to a topic partition
  - Other adapters map their native partition or shard identifier to a stream ID

- **Stream ID Characteristics**:
  - Stream IDs are string identifiers that group related entries together
  - Entries within a stream are ordered sequentially
  - Each stream maintains its own position tracking
  - Streams can be independently compacted or deleted

- **Stream ID Usage**:
  - When writing data with `put(id, buf)`, the stream ID determines which logical stream the entry belongs to
  - When reading data with `get(id, offset, compactedIndex)`, the stream ID identifies which stream to read from
  - The WAL system maintains separate metadata for each stream ID
  - The `WriteCache` may batch operations across multiple stream IDs for efficiency, but maintains ordering within each stream

- **Stream ID Generation**:
  - Stream IDs are typically generated by the streaming services (Kafka integrations)
  - They often incorporate information like topic name, partition ID, or other identifiers
  - The WAL library itself is stream ID agnostic and treats them as opaque identifiers

### SimpleStorageImpl

The primary implementation of the `WalStorage` interface that:

1. Manages write buffers through the `WriteCache`
2. Processes write requests asynchronously
3. Flushes data to the underlying storage
4. Indexes metadata in Oxia
5. Manages read caching for performance

### FileStorage Interface

Abstracts the underlying storage backends with implementations for:

- Amazon S3 (`S3FileStorage`)
- Google Cloud Storage (`GCSFileStorage`)
- Azure Blob Storage (`AzureFileStorage`)
- Local file system (`LocalFileStorage`)

### PersistCache

Buffers data before writing to storage and provides:

- Efficient memory management
- Serialization of data for storage
- Integration with Oxia for metadata indexing

### WriteCache

Manages write buffers for performance optimization:

- Maintains a pool of buffer segments
- Implements in-memory caching of recently written data
- Controls flushing behavior based on size and time thresholds

#### WriteCache Architecture and Implementation

The WriteCache is a sophisticated buffering system that optimizes write performance by batching and efficiently managing write operations:

1. **Buffer Management**:
   - Uses a segmented buffer approach with multiple buffer segments
   - Each segment has a configurable size (typically 1-10MB)
   - Maintains a pool of reusable buffer segments to minimize allocation overhead
   - Implements reference counting for efficient memory management

2. **Write Operation Flow**:
   - Incoming writes are first added to the current active buffer segment
   - When a segment reaches its capacity, it's queued for flushing
   - A new segment becomes active for subsequent writes
   - Flushing occurs based on configurable triggers (size, time, or manual)

3. **Stream ID Handling**:
   - Tracks entries by stream ID within each buffer segment
   - Maintains ordering guarantees within each stream ID
   - Can batch entries from multiple stream IDs in the same segment for efficiency
   - Records position information for each entry

#### WriteCache Operations

1. **Write Operation**:
   ```java
   // Internal pseudocode for write operation
   public CompletableFuture<Long> write(String streamId, ByteBuf data) {
       // Create a promise to be completed when the write is acknowledged
       CompletableFuture<Long> promise = new CompletableFuture<>();
       
       // Get or create the current active segment
       BufferSegment segment = getCurrentSegment();
       
       // Check if we need a new segment
       if (!segment.hasCapacity(data.readableBytes())) {
           queueSegmentForFlush(segment);
           segment = createNewSegment();
       }
       
       // Write the data to the segment
       long position = segment.write(streamId, data);
       
       // Register the promise to be completed after flush
       segment.registerPromise(streamId, position, promise);
       
       // Check if we need to trigger a flush
       checkFlushTriggers();
       
       return promise;
   }
   ```

2. **Flush Operation**:
   ```java
   // Internal pseudocode for flush operation
   private void flushSegment(BufferSegment segment) {
       // Write the segment data to the underlying storage
       CompletableFuture<Void> storageFuture = fileStorage.write(segment.getData());
       
       // After storage write completes, update metadata in Oxia
       storageFuture.thenCompose(v -> {
           Map<String, List<EntryPosition>> positions = segment.getPositions();
           return oxiaClient.updatePositions(positions);
       }).whenComplete((result, ex) -> {
           if (ex != null) {
               // Handle error, fail all promises
               segment.failAllPromises(ex);
           } else {
               // Complete all promises with their positions
               segment.completeAllPromises();
           }
           
           // Return the segment to the pool
           recycleSegment(segment);
       });
   }
   ```

3. **Flush Triggers**:
   ```java
   // Internal pseudocode for flush triggers
   private void checkFlushTriggers() {
       // Check size-based trigger
       if (currentSize >= flushThresholdSize) {
           triggerFlush();
       }
       
       // Check time-based trigger if not already scheduled
       if (!flushTimerScheduled) {
           flushTimerScheduled = true;
           flushScheduler.schedule(() -> {
               flushTimerScheduled = false;
               if (hasUnflushedData()) {
                   triggerFlush();
               }
           }, flushIntervalMs, TimeUnit.MILLISECONDS);
       }
   }
   ```

#### WriteCache Tuning

The WriteCache offers several configuration options for performance tuning, as defined in runtime storage configuration:

1. **Write Buffer Configuration**:
   - `writeBufferSize`: Size of each buffer segment in bytes (default: 4MB)
   - `writeBufferSegment`: Number of buffer segments to maintain (default: calculated as 25% of max direct memory divided by writeBufferSize)
   - `writeBufferMaxStreamIds`: Maximum number of stream IDs in a single write buffer (default: 4)

2. **Flush Behavior**:
   - `writeBufferFlushSize`: Data size threshold that triggers a flush (default: 256MB)
   - `writeBufferFlushIntervalMs`: Time interval for periodic flush in milliseconds (default: 250ms)
   - `writeCacheEnabled`: Enable or disable the write cache (default: true)

3. **Request Handling**:
   - `numAddWorkerThreads`: Number of worker threads for processing add operations (default: 1)
   - `maxPendingAddRequestsUsedBytes`: Maximum memory for pending add requests (default: 15% of max direct memory)
   - `addEntryMaxThrottleTimeMs`: Maximum throttle time for add operations in milliseconds (default: 500ms)
   - `addEntryTimeoutMs`: Timeout for add operations in milliseconds (default: 30,000ms)

#### WriteCache Implementation Details

1. **Concurrency Handling**:
   - Thread-safe implementation for concurrent writes
   - Lock-free write path for high throughput
   - Synchronized flush operations to maintain consistency

2. **Durability Guarantees**:
   - Two-phase commit process (storage write + metadata update)
   - Acknowledgment only after both phases complete successfully
   - Recovery mechanism for handling failures during flush

3. **Memory Efficiency**:
   - Reuse of buffer segments to reduce allocation overhead
   - Efficient serialization of entries to minimize memory usage
   - Adaptive buffer sizing based on workload patterns

### ReadCache

Caches read operations for performance:

- Implements a loading cache pattern
- Uses eviction policies to manage memory usage
- Prefetches data for anticipated reads

#### ReadCache Architecture and Implementation

The ReadCache is a sophisticated in-memory caching system that optimizes read performance by keeping frequently accessed data readily available:

1. **Data Structure**:
   - Uses a concurrent hash map for fast lookups
   - Entries are indexed by a composite key of stream ID and position
   - Implements a `SlidingWindowPercentileEvictionPolicy` (not a simple LRU)
   - Maintains detailed statistics per entry for intelligent eviction decisions

2. **Cache Entry Lifecycle**:
   - **Insertion**: Entries are added to the cache when:
     - Data is read from storage and not found in cache
     - Data is explicitly prefetched using the `preFetch` method
   - **Access**: Cache lookups use the stream ID and position as the key
   - **Eviction**: Entries are evicted based on statistical analysis:
     - Read count compared to the 99th percentile target
     - Idle duration compared to adaptive thresholds
     - Cache size approaching configured maximum (95% threshold)
     - Statistical tracking of access patterns

3. **Memory Management**:
   - Configurable maximum cache size (in bytes)
   - Configurable maximum entries per stream ID
   - Adaptive sizing based on access patterns
   - Off-heap storage option for large caches to reduce GC pressure

#### ReadCache Operations

1. **Cache Lookup**:
   ```java
   // Internal pseudocode for cache lookup
   public ByteBuf get(String streamId, long position) {
       CacheKey key = new CacheKey(streamId, position);
       CacheEntry entry = cache.get(key);
       if (entry != null) {
           // Track read statistics for the sliding window percentile policy
           entry.incrementReadCount();
           entry.updateLastReadTimestamp();
           metrics.recordCacheHit(streamId);
           return entry.getData();
       }
       metrics.recordCacheMiss(streamId);
       return null;
   }
   ```

2. **Cache Insertion**:
   ```java
   // Internal pseudocode for cache insertion
   public void put(String streamId, long position, ByteBuf data) {
       CacheKey key = new CacheKey(streamId, position);
       CacheEntry entry = new CacheEntry(data, System.currentTimeMillis());
       
       // Check if we need to evict entries using the percentile-based policy
       if (currentSize + data.readableBytes() > maxCacheSize * CACHE_LOAD_TO_TRY_EVICTION) {
           slidingWindowPolicy.tryEvict(cache, maxCacheSize);
       }
       
       cache.put(key, entry);
       currentSize += data.readableBytes();
       metrics.recordCacheInsertion(streamId, data.readableBytes());
   }
   ```

3. **Prefetching**:
   ```java
   // Internal pseudocode for prefetching
   public void prefetch(String streamId, List<Long> positions) {
       // Filter out positions already in cache
       List<Long> missingPositions = positions.stream()
           .filter(pos -> !cache.containsKey(new CacheKey(streamId, pos)))
           .collect(Collectors.toList());
       
       if (missingPositions.isEmpty()) {
           return;
       }
       
       // Fetch data from storage asynchronously
       CompletableFuture.runAsync(() -> {
           for (Long position : missingPositions) {
               ByteBuf data = fetchFromStorage(streamId, position);
               put(streamId, position, data);
           }
       }, prefetchExecutor);
   }
   ```

#### ReadCache Tuning

The ReadCache offers several configuration options for performance tuning, as defined in runtime storage configuration:

1. **Cache Size and Memory Management**:
   - `readCacheMemorySize`: Maximum memory size for the cache in bytes (default: 15% of max direct memory)
   - `readCacheSpillableDiskDir`: Directory for spillable disk cache when memory cache is full (default: "/tmp")
   - `readCacheToDiskCompressionEnable`: Enable compression for disk-spilled cache entries (default: true)

2. **Thread Management**:
   - `readThreadNum`: Number of worker threads for read operations (default: 4)

3. **Entry Index Caching**:
   - `maxEntryIndexCacheSize`: Maximum size of the entry index cache (default: 1% of max direct memory divided by 1KB)
   - `entryIndexCacheTTLInSecs`: Time-to-live for entry index cache entries in seconds (default: 600s)
   - `maxIndexesCacheBuildDelayInMillis`: Maximum delay when scanning and caching entry indexes (default: 1000ms)

#### ReadCache Eviction Policy

The ReadCache implements a sophisticated eviction policy called `SlidingWindowPercentileEvictionPolicy` to manage memory usage efficiently. This is not a simple LRU cache but a statistically-driven adaptive eviction mechanism:

1. **Sliding Window Percentile-Based Eviction**:
   - Uses a sliding window of statistics to track read patterns
   - Maintains two key metrics:
     - `readCounts`: Number of times an entry is read
     - `readDurations`: Duration of read operations
   - Computes the 99th percentile of these metrics to determine eviction thresholds
   - Default window size is 100 entries (defined as `WINDOW_SIZE = 100`)

2. **Intelligent Eviction Criteria**:
   ```java
   // From SlidingWindowPercentileEvictionPolicy.java
   public <T> CompletableFuture<Integer> tryEvict(
           LoadingCache<T, CompletableFuture<PersistCache>> cache,
           long cacheMaxSize) {
       if (cache.size() >= cacheMaxSize * CACHE_LOAD_TO_TRY_EVICTION
               && !isEvicting.get()
               && System.currentTimeMillis() - lastEvictionTimestamp >= EVICTION_DELAY_IN_MILLIS) {
           return CompletableFuture.supplyAsync(() -> doEvict(cache));
       }
       return CompletableFuture.completedFuture(0);
   }
   ```
   - Eviction is triggered when cache size reaches 95% of maximum capacity (`CACHE_LOAD_TO_TRY_EVICTION = 0.95f`)
   - Entries are evaluated for eviction based on:
     - Read count compared to the 99th percentile target
     - Idle duration (time since last read)
     - Read duration patterns
   - Entries with low read counts relative to the target and high idle times are prioritized for eviction

3. **Adaptive Idle Duration Calculation**:
   ```java
   // From SlidingWindowPercentileEvictionPolicy.java
   int globalIdleDuration =
           Math.max(MIN_TARGET_IDLE_DURATION_IN_MILLIS, toInt(targetReadDuration * 2L));
   // ...
   int targetIdleDuration = Math.max(readDuration, globalIdleDuration);
   ```
   - Global idle duration is calculated as `max(MIN_TARGET_IDLE_DURATION_IN_MILLIS, targetReadDuration * 2)`
   - Minimum idle duration is 1000ms (1 second)
   - Each entry's target idle duration is the maximum of its read duration and the global idle duration
   - This adaptive approach ensures frequently accessed entries remain in cache longer

4. **Statistical Tracking**:
   ```java
   // From SlidingWindowPercentileEvictionPolicy.java
   public void onRemoval(String key, PersistCache cache) {
       int readCount = toInt(cache.getReadCount());
       int readDuration = toInt(cache.getReadDurationInMillis());
       removed.put(key, new Stat(readCount, readDuration));
   }
   ```
   - When entries are removed, their statistics are temporarily stored
   - If an entry is accessed again before statistics expire, it's not recorded (preventing thrashing)
   - Statistics expire after a configurable delay (default: 5x the percentile compute delay)
   - Periodic cleanup of removed entries' statistics

5. **Throttled Eviction**:
   ```java
   // Constants from SlidingWindowPercentileEvictionPolicy.java
   private static final int EVICTION_DELAY_IN_MILLIS = 100;
   private final AtomicBoolean isEvicting = new AtomicBoolean(false);
   ```
   - Eviction operations are throttled with a minimum delay between evictions (100ms)
   - Eviction runs asynchronously to avoid blocking read operations
   - Concurrent eviction operations are prevented using atomic flags

This sophisticated eviction policy ensures that the cache retains the most valuable entries based on actual usage patterns, adapting to changing workloads while preventing cache thrashing.

#### ReadCache Implementation Details

1. **Concurrency Handling**:
   - Thread-safe implementation using concurrent data structures
   - Lock-free reads for high throughput
   - Fine-grained locking for updates to minimize contention

2. **Memory Efficiency**:
   - Reference counting for ByteBuf instances to avoid memory leaks
   - Shared buffer pools to reduce memory fragmentation
   - Incremental cleanup to avoid long GC pauses

3. **Integration with Storage Layer**:
   - Coordinates with FileStorage for efficient data retrieval
   - Handles data format conversions if needed
   - Manages buffer lifecycle between storage and application

### Oxia Integration

Oxia serves as the metadata service that:

- Tracks entry indexes and positions
- Maintains stream state
- Provides ordering guarantees
- Generates unique IDs for WAL entries

## Data Flow

### Write Path

1. Streaming services (Kafka integrations) call `put(id, buf)` on the `WalStorage` implementation
   - `id`: The stream ID to write to (for example, a topic partition)
   - `buf`: The data buffer containing the message/event to be written
2. The request is added to the pending requests queue
   - Requests are tagged with their stream ID for proper ordering
3. The request processor batches requests for efficiency
   - Batching may combine requests from different stream IDs for storage efficiency
   - The system maintains ordering guarantees within each stream ID
4. Data is written to the `WriteCache`
   - The cache maintains separate logical sequences for each stream ID
5. When the cache reaches a threshold or a time interval elapses:
   - The cache is flushed to the underlying `FileStorage`
   - A new position is assigned to each entry within its stream
   - Metadata is indexed in Oxia, mapping the logical position (stream ID + position) to the physical storage location
6. The streaming services are notified of the successful write with the entry's position, allowing them to acknowledge the message/event to producers

#### Stream ID in Write Operations

The stream ID serves several important functions in write operations:

1. **Logical Grouping**: Entries with the same stream ID form a logical sequence
2. **Position Assignment**: Each entry gets a position that is unique and monotonically increasing within its stream ID
3. **Metadata Indexing**: The system maintains metadata per stream ID in Oxia
4. **Write Ordering**: The system ensures that entries within the same stream ID are written in order
5. **Parallel Processing**: Different stream IDs can be processed in parallel for higher throughput
6. **Storage Optimization**: While maintaining logical separation, entries from different stream IDs may be physically stored together for efficiency

#### Write Path Data Flow Diagram

```
+----------------+                +----------------+                +----------------+
|                |                |                |                |                |
| Streaming      |---put(id,buf)-->  WalStorage    |--------------->  Pending Queue  |
| Services       |                | Implementation |                |                |
| (integration adapters)   |                |                |                |                |
+----------------+                +----------------+                +--------+-------+
                                                                            |
                                                                            | Process
                                                                            v
+-------------------------------------------------------------------------+
|                                                                         |
|                              WriteCache                                 |
|                                                                         |
|  +-------------+  +-------------+  +-------------+  +-------------+     |
|  | Buffer      |  | Buffer      |  | Buffer      |  | Buffer      |     |
|  | Segment 1   |  | Segment 2   |  | Segment 3   |  | Segment N   |     |
|  +-------------+  +-------------+  +-------------+  +-------------+     |
|                                                                         |
+-----------------------------------------+-----------------------------------+
                                          |
                                          | Flush (based on size/time threshold)
                                          |
                                          v
                               +----------------+
                               |                |
                               | FileStorage    |
                               | Implementation |
                               +-------+--------+
                                       |
                                       | Write Data
                                       |
                                       v
                               +----------------+
                               |                |
                               | Cloud Storage  |
                               | (S3/GCS/Azure) |
                               +-------+--------+
                                       |
                                       | After successful write
                                       |
                                       v
                               +----------------+
                               |                |
                               | Oxia Metadata  |
                               | Service        |
                               +----------------+
                                 Index metadata
```

In this flow:
1. The streaming services (Kafka integrations) call `put(id, buf)` on the WAL library to persist messages/events
2. Data is first written to the WriteCache buffer segments
3. When flush conditions are met (size or time threshold):
   - Data is first written to cloud storage via the FileStorage implementation
   - After the successful write to cloud storage, metadata is indexed in Oxia
4. The streaming services are notified once both operations complete successfully, allowing them to acknowledge the message/event to producers

### Read Path

1. Streaming services (Kafka integrations) call `get(id, offset, compactedIndex)` on the `WalStorage` implementation
   - `id`: The stream ID to read from (for example, a topic partition)
   - `offset`: The position within the stream to read from
   - `compactedIndex`: Used for handling compacted entries
2. The system checks the `ReadCache` for the requested data
   - The cache lookup uses a composite key of stream ID and position
3. If found in cache, the data is returned immediately
4. If not in cache:
   - Metadata is retrieved from Oxia to locate the data
     - The system queries Oxia using the stream ID to find the mapping between the logical position and physical storage location
   - Data is fetched from the underlying `FileStorage`
     - The system uses the physical location information to efficiently retrieve the data without scanning
   - The data is added to the `ReadCache` for future reads
   - The data is returned to the streaming services, which can then deliver it to consumers

#### Stream ID in Read Operations

The stream ID is crucial for read operations because:

1. **Logical Addressing**: The stream ID + position combination provides a logical address that the WAL system translates to a physical storage location
2. **Isolation**: Reading from one stream ID doesn't affect or interfere with other streams
3. **Efficiency**: The metadata system allows for direct access to the requested entry without scanning through unrelated data
4. **Caching Strategy**: The read cache can be optimized based on access patterns per stream ID
5. **Prefetching**: The `preFetch(id, positions)` method allows for optimized prefetching of entries from specific stream IDs

#### Read Path Data Flow Diagram

```
+----------------+                +----------------+
|                |                |                |
| Streaming      |---get(id,pos)-->  WalStorage    |
| Services       |                | Implementation |
| (integration adapters)   |                |                |
+----------------+                +-------+--------+
                                      |
                                      v
                             +----------------+
                             |                |  Yes (Cache Hit)
                             | Check ReadCache+------------+
                             |                |            |
                             +-------+--------+            |
                                     | No (Cache Miss)     |
                                     |                     |
                                     v                     |
                             +----------------+            |
                             |                |            |
                             | Oxia Metadata  |            |
                             | Service        |            |
                             +-------+--------+            |
                                     |                     |
                                     | Get Location        |
                                     |                     |
                                     v                     |
                             +----------------+            |
                             |                |            |
                             | FileStorage    |            |
                             | Implementation |            |
                             +-------+--------+            |
                                     |                     |
                                     | Fetch Data          |
                                     |                     |
                                     v                     |
                             +----------------+            |
                             |                |            |
                             | Cloud Storage  |            |
                             | (S3/GCS/Azure) |            |
                             +-------+--------+            |
                                     |                     |
                                     |                     |
                                     v                     |
                             +----------------+            |
                             |                |            |
                             | Update         |            |
                             | ReadCache      |            |
                             +-------+--------+            |
                                     |                     |
                                     +---------------------+
                                               |
                                               v
                                     +----------------+
                                     |                |
                                     | Return Data    |
                                     | to Client      |
                                     +----------------+
```

In this flow:
1. The streaming services (Kafka integrations) call `get(id, pos)` to retrieve messages/events
2. The system first checks if the requested data is in the ReadCache
3. If it's a cache hit, the data is returned immediately
4. If it's a cache miss:
   - The system retrieves metadata from Oxia to locate the data
   - The data is fetched from cloud storage via FileStorage
   - The ReadCache is updated with the fetched data
5. Finally, the data is returned to the streaming services, which can then deliver it to consumers

### Delete Path

1. Streaming services (Kafka integrations) call `delete(id, positions)` on the `WalStorage` implementation
   - `id`: The stream ID from which to delete entries
   - `positions`: A list of positions within the stream to delete
2. The system processes the delete request:
   - Updates the metadata in Oxia to mark the specified positions as deleted
   - Removes the entries from caches if present
3. For cloud storage backends:
   - Physical deletion may be deferred and handled through lifecycle policies
   - The system may use a garbage collection process to reclaim space
4. This operation is typically used during:
   - Compaction: When older messages with the same key are replaced by newer ones
   - Expiration: When messages/events reach their time-to-live (TTL)
   - Topic deletion: When an entire stream (topic partition) is removed

#### Stream ID in Delete Operations

The stream ID plays a key role in delete operations:

1. **Targeted Deletion**: The stream ID allows for deleting specific entries from a particular stream without affecting others
2. **Metadata Updates**: The system updates the metadata for the specific stream ID in Oxia
3. **Compaction Support**: Stream-specific deletion enables efficient compaction strategies
4. **Garbage Collection**: The system can track which physical storage blocks can be reclaimed when entries from multiple streams are stored together
5. **Stream Lifecycle**: When all entries in a stream are deleted, the stream can be marked as empty while preserving its metadata

## Performance Tuning

The WAL system offers numerous configuration options to optimize performance for different workloads:

### Write Buffer Configuration

- `writeBufferSize`: Size of each write buffer segment (default: 4MB)
- `writeBufferSegment`: Number of write buffer segments (default: 25% of direct memory)
- `writeBufferFlushSize`: Threshold for flushing the write buffer (default: 256MB)
- `writeBufferMaxStreamIds`: Maximum number of stream IDs in a single batch (default: 4)
- `writeBufferFlushIntervalMs`: Maximum time interval between flushes (default: 250ms)
- `writeCacheEnabled`: Whether to enable in-memory caching of written data

### Read Cache Configuration

- `readCacheMemorySize`: Maximum memory size for the read cache (default: 15% of direct memory)
- `readCacheSpillableDiskDir`: Directory for spilling read cache to disk
- `readCacheToDiskCompressionEnable`: Whether to compress spilled read cache

### Cloud Storage Configuration

- `cloudStorageMaxConcurrencyRequest`: Maximum concurrent requests to cloud storage
- `s3MaxPendingConnectionAcquires`: Maximum pending connection acquires for S3
- `s3ConnectionAcquisitionTimeoutMs`: Connection acquisition timeout for S3
- `s3OpsRateLimitPerSecond`: Rate limit for S3 operations

## Interacting with Oxia

Oxia is used as a metadata service to track entry indexes and stream state. This section details how the WAL system interacts with Oxia for metadata management.

### ID Generation

The `OxiaIDGenerator` uses Oxia to generate unique IDs for WAL entries:

```java
// From OxiaIDGenerator.java
public String generate() throws IDGeneratorException {
    try {
        PutResult putResult = client.put(key, new byte[0]).get();
        return String.valueOf(putResult.version().modificationsCount());
    } catch (InterruptedException | ExecutionException e) {
        throw new IDGeneratorException("Failed to generate id", e);
    }
}
```

### Entry Metadata Indexing

#### Write Path: Indexing Entries in Oxia

When data is written to cloud storage, the WAL system indexes metadata about these entries in Oxia. This process involves:

1. **Preparing Index Data**: The system creates index entries that map stream IDs to entry positions
2. **Storing in Oxia**: The index data is stored in Oxia with appropriate keys

Here's how the indexing process works:

```java
// From EntryCache.java
@Override
public CompletableFuture<Map<Long, Optional<PutResult>>> index(AsyncOxiaClient oxiaClient,
    StreamStateManager streamStateManager) {
    lock.readLock().lock();
    try {
        validateState();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        Map<Long, Optional<PutResult>> result = new ConcurrentHashMap<>();
        for (var request : indexRequests) {
            if (streamStateManager.getState(request.streamId) == StreamState.FENCED) {
                result.put(request.streamId, Optional.empty());
            } else {
                futures.add(oxiaClient.put(request.key, request.value, request.options)
                        .thenAccept(putResult -> result.put(request.streamId, Optional.of(putResult))));
            }
        }
        return FutureUtil.waitForAll(futures).thenApply(__ -> result);
    } finally {
        lock.readLock().unlock();
    }
}
```

The complete flow for writing and indexing entries:

1. Data is written to the WriteCache
2. WriteCache is flushed to cloud storage (S3/GCS/Azure)
3. After successful storage, metadata is indexed in Oxia
4. The index contains:
   - Stream ID
   - Entry positions
   - File location in cloud storage
   - Entry sizes and offsets

#### Read Path: Retrieving Entry Metadata from Oxia

When reading data, the system first queries Oxia to locate the entries, then retrieves the data from the ReadCache or cloud storage:

```java
// From SimpleStorageImpl.java
public CompletableFuture<Entry> get(long id, long offset, EntryIndex compactedIndex) {
    return getPersistCache(id, compactedIndex.position()).thenCompose(c -> {
        // 1. Try to get the entry from the cache first
        var entry = c.get(id, offset, compactedIndex);
        if (entry != null) {
            return CompletableFuture.completedFuture(entry);
        }
        
        // 2. If not in cache, retrieve from cloud storage
        return fileStorage.getAsync(compactedIndex.position().location()).thenApply(byteBuf -> {
            PersistCache readFromStorage = PersistCacheFactory.deserialize(allocator, byteBuf,
                    config.getIndexSerializeFormatVersion());
            var entry2 = c.get(id, offset, compactedIndex);
            readFromStorage.close();
            return entry2;
        });
    });
}
```

The complete read flow involves:

1. Querying Oxia to get the `EntryIndex` for the requested stream ID and offset
2. Using the `EntryIndex` to locate the data in the ReadCache
3. If the data is not in the cache, retrieving it from cloud storage
4. For compacted indexes, using binary search to find the specific entry within the compacted file

### Compacted Index Mechanism

The WAL system uses a compacted index mechanism to efficiently manage metadata for multiple entries in a stream. This optimization significantly reduces the Oxia load and improves read performance.

#### How Compacted Indexes Work

1. **Index Types** (defined in `storage_format.proto`):
   - `NORMAL`: Each entry has its own index in Oxia
   - `COMPACT`: Multiple entries from the same stream ID share a single index

2. **Compacted Index Structure** (from `storage_format.proto`):
   ```protobuf
   message EntryIndex {
     optional Version version = 1;
     optional string location = 2;
     optional FileType file_type = 3;
     optional IndexType index_type = 4;
     optional int32 message_count = 5;
     optional int64 entry_size_bytes = 6;
     optional int64 file_size = 7;
     optional int64 offset_in_file = 8;
     optional int32 entry_count = 9;
     optional EntryOffsets entry_offsets = 10;
   }

   message EntryOffsets {
     optional CompressionType compression_type = 1;
     optional int32 uncompressed_size = 2;
     optional bytes compressed_payload = 3;
   }
   ```

3. **Key Components**:
   - `EntryHeader`: Contains metadata for all entries (offset, message count, timestamp, size)
   - `Position`: Location in cloud storage (file path and offset)
   - `EntryCount`: Number of entries in this index
   - `IndexType`: COMPACT or NORMAL
   - `EntryOffsets`: Compressed array of offsets for each entry within the file
   - `EntryHeaders`: Runtime-generated map of offset to header pairs for efficient lookup

4. **Benefits of Compacted Indexes**:
   - **Reduced Oxia Load**: Instead of creating an index for each entry, a single index is created for all entries of the same stream ID
   - **Fewer Oxia Operations**: Batch writes require only one Oxia operation per stream ID
   - **Improved Read Performance**: Sequential access is optimized
   - **Reduced Storage Overhead**: Fewer index records in Oxia

#### Compacted Index Example

Consider a WAL file with entries from a single stream ID:

**Before Compaction (NORMAL mode)**:
```
Stream ID 1: 5 entries with 10 messages each (100 bytes per entry)

WAL: 1:[10 msgs, 10 msgs, 10 msgs, 10 msgs, 10 msgs]

→ Persist one WAL file
→ Put entry index: key: 1-10-100, value: location.0.RAW, 10, 100
→ Put entry index: key: 1-20-200, value: location.1.RAW, 10, 100
→ Put entry index: key: 1-30-300, value: location.2.RAW, 10, 100
→ Put entry index: key: 1-40-400, value: location.3.RAW, 10, 100
→ Put entry index: key: 1-50-500, value: location.4.RAW, 10, 100
```

**After Compaction (COMPACT mode)**:
```
Stream ID 1: 5 entries with 10 messages each (100 bytes per entry)

WAL: 1:[10 msgs, 10 msgs, 10 msgs, 10 msgs, 10 msgs]

→ Persist one WAL file
→ Put entry index: key: 1-50-500, value: location.5.COMPACT, 50, 500
  (With EntryOffsets containing the offsets [10, 20, 30, 40, 50])
```

#### Implementation Details

When multiple entries are written to the same stream ID:

```java
// From EntryCache.java
var indexType = streamIdCount == 1 ? COMPACT : NORMAL;
// ...
final EntryCache.IndexRequest request;
var indexValue = new io.lakestream.ursa.storage.Value(numberOfMessages, size, count, indexType,
        new Position(new FileInfo(location, fileSize), -1, fileType),
        Optional.ofNullable(offsets)).toBytes(format.getIndexSerializeFormatVersion());
```

#### Reading from Compacted Indexes

When reading from a compacted index, the system needs to locate the specific entry containing the requested offset:

1. **Entry Lookup Process**:
   ```java
   // From EntryIndex.java
   public EntryHeader searchEntryHeader(long targetOffset) {
       var header = tryGetCommonHeader();
       if (header != null) {
           validateSearchedOffset(targetOffset, header);
           return header;
       }
       var offset = searchEntryOffset(targetOffset);
       header = doGetEntryHeader(offset);
       validateSearchedOffset(targetOffset, header);
       return header;
   }
   ```

2. **Binary Search for Efficient Lookup**:
   ```java
   // From EntryIndex.java
   private long searchEntryOffset(long offset) {
       int target = (int) (offset - header.offset());
       if (target < 0) {
           throw new IllegalStateException("offset not found at" + offset + " target " + target);
       }
       if (entryOffsets.isEmpty()) {
           throw new IllegalStateException("entryOffsets not found at" + offset + " target " + target);
       }
       // Binary search to find the entry
       var arr = entryOffsets.get();
       int index = Arrays.binarySearch(arr, target);
       // Process search result...
   }
   ```

3. **Optimization for Repeated Access**:
   - The system builds a mapping of `(streamId, offset) → (EntryHeader, EntryId)` 
   - This mapping is built lazily (on first access) to avoid unnecessary computation
   - Subsequent accesses to the same offset are much faster using this mapping

### Stream State Management

The `StreamStateManager` interface provides methods to manage stream state in Oxia:

- Track entry positions
- Manage offsets
- Handle compaction state

#### Stream ID and Metadata Management

The WAL system uses Oxia to store and manage metadata for each stream ID:

1. **Metadata Structure**:
   - Each stream ID has its own metadata entry in Oxia
   - The metadata includes:
     - Current head position (newest entry)
     - Current tail position (oldest entry)
     - Compaction information
     - Entry positions and their corresponding storage locations

2. **Position Tracking**:
   - When a new entry is written to a stream, its position is recorded in the stream's metadata
   - Positions are monotonically increasing within a stream
   - The position is returned to the streaming service after a successful write

3. **Storage Organization**:
   - Entries from multiple streams may be batched together in the same physical storage file for efficiency
   - The metadata maintains the mapping between logical positions in a stream and their physical locations
   - This allows for efficient retrieval without scanning the entire storage

4. **Stream Isolation**:
   - Despite physical batching, streams are logically isolated
   - Operations on one stream do not affect the ordering or consistency of other streams
   - This allows for independent scaling of different streams

5. **Stream Lifecycle**:
   - Streams can be created on-demand simply by writing to a new stream ID
   - Streams can be deleted when no longer needed
   - Stream metadata persists even when there are no active entries (after deletion or compaction)

## Ordering Guarantees

The WAL system provides the following ordering guarantees:

1. **Write Order Preservation**: Entries are written to the WAL in the order they are received within a stream ID
2. **Read Consistency**: Reads will return entries in the same order they were written
3. **Metadata Consistency**: Oxia ensures that metadata operations are consistent
4. **Position Monotonicity**: Entry positions within a stream ID are monotonically increasing
5. **Durability**: Once a write is acknowledged, the data is guaranteed to be durable and retrievable

### Stream ID and Ordering

Stream IDs play a crucial role in the ordering guarantees of the WAL system:

1. **Per-Stream Ordering**:
   - The WAL system guarantees strict ordering only within the same stream ID
   - Entries with different stream IDs may be interleaved in the physical storage
   - This allows for parallel processing of different streams without ordering constraints

2. **Position Semantics**:
   - Each entry in a stream has a unique position
   - Positions are assigned in strictly increasing order within a stream
   - Positions are used by streaming services to track consumption progress
   - The position is returned as part of the write acknowledgment

3. **Concurrent Stream Operations**:
   - Operations on different stream IDs can proceed concurrently
   - This allows for high throughput when working with multiple streams
   - The system maintains ordering guarantees per stream even during concurrent operations

4. **Stream ID Partitioning**:
   - Streaming services often use stream IDs to implement partitioning
   - Each partition (in Kafka integrations) typically maps to a distinct stream ID
   - This allows the WAL system to maintain the same partitioning scheme as the streaming service

5. **Stream ID and Recovery**:
   - During recovery, entries can be replayed per stream ID
   - This allows for parallel recovery of multiple streams
   - The position information allows for precise recovery from specific points in each stream

## Error Handling and Recovery

The WAL system implements several strategies for error handling and recovery:

1. **Write Failures**: Failed writes are retried with exponential backoff
2. **Read Failures**: Failed reads attempt to refresh metadata and retry
3. **Metadata Inconsistency**: The system can recover from metadata inconsistencies by rebuilding indexes

## Monitoring and Metrics

The WAL system exposes numerous metrics for monitoring:

### Write Metrics

- `ursa.storage.wal.putEntry.count`: Number of put entry requests
- `ursa.storage.wal.putEntry.rejected.count`: Number of rejected put entry requests
- `ursa.storage.wal.putEntry.pending.count`: Number of pending put entry requests
- `ursa.storage.wal.putEntry.duration`: Latency of put entry operations
- `ursa.storage.wal.putEntry.pending.duration`: Latency of pending put entry operations
- `ursa.storage.wal.putEntry.cache.duration`: Latency of putting entries to cache

### Cache Metrics

- `ursa.storage.wal.writeCache.segment.count`: Number of write cache segments
- `ursa.storage.wal.writeCache.bufferSegment.used`: Number of used buffer segments
- `ursa.storage.wal.writeCache.cacheSegment.used`: Number of used cache segments
- `ursa.storage.wal.writeCache.used`: Size of write cache in bytes
- `ursa.storage.wal.writeCache.capacity`: Capacity of write cache in bytes
- `ursa.storage.wal.writeCache.flushCallback.pending.count`: Number of pending write cache flush callbacks
- `ursa.storage.wal.writeCache.flush.duration`: Latency of write cache flush operations

### Read Metrics

- `ursa.storage.wal.readCache.loading.count`: Number of read cache loading operations
- `ursa.storage.wal.readCache.eviction.count`: Number of read cache eviction operations
- `ursa.storage.wal.readCache.loading.duration`: Duration of read cache loading operations
- `ursa.storage.wal.readCache.size`: Size of read cache in bytes
- `ursa.storage.wal.read.cache.missed`: Number of read cache misses
- `ursa.storage.wal.getEntry.duration`: Duration of get entry operations
- `ursa.storage.wal.getEntries.duration`: Duration of get entries operations

### Storage Backend Metrics

- `ursa.storage.backend.storage.request`: Number of requests to the backend storage
- `ursa.storage.backend.write.duration`: Latency of write operations to backend storage
- `ursa.storage.backend.read.duration`: Latency of read operations from backend storage
- `ursa.storage.backend.metadata.read.duration`: Latency of metadata read operations
- `ursa.storage.backend.delete.duration`: Latency of delete operations
- `ursa.storage.backend.write.bytes.count`: Number of bytes written to backend storage
- `ursa.storage.backend.read.bytes.count`: Number of bytes read from backend storage

## Conclusion

The Ursa Storage WAL library provides a robust, high-performance Java solution for using cloud storage as a backend for write-ahead logging. By leveraging intelligent buffering, caching, and metadata management, it achieves both durability and performance, making it an ideal storage layer for systems like Kafka integrations and other streaming systems.

### Key takeaways for developers:

1. The library is highly configurable to adapt to different workloads and can be tuned for specific use cases in Kafka and other systems
2. Performance can be optimized through buffer and cache settings to meet the demands of high-throughput messaging systems
3. Oxia integration provides reliable metadata management, ensuring consistency across distributed deployments
4. The architecture supports multiple cloud storage backends (S3, GCS, Azure), allowing flexibility in deployment environments
5. The system is designed for both high throughput and low latency, critical for messaging and streaming platforms

### When integrating the WAL library with Kafka and other systems, consider:

- Properly sizing buffers and caches based on your expected message throughput and size
- Monitoring metrics to identify performance bottlenecks in your specific deployment
- Understanding the ordering guarantees provided by the system to ensure your application's consistency requirements are met
- Leveraging the prefetch capabilities for read-heavy workloads to optimize consumer performance
- Configuring cloud storage parameters based on your cloud provider's recommendations and limitations

The Ursa Storage WAL library abstracts away the complexities of durable storage, allowing messaging and streaming systems to focus on their core functionality while benefiting from reliable, scalable, and efficient data persistence.
