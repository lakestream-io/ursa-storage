# Common Pitfalls

Hard-won knowledge to avoid repeated failures when working on ursa-storage.

## Build Pitfalls

### License Headers
**Problem**: Every new `.java` file needs a license header. CI fails on `license:check`.
**Fix**: Run `mvn license:format` to auto-add headers, or copy from an existing file.
**Template**: `resources/license.template`

### Package-info.java
**Problem**: Every new Java package needs a `package-info.java` file. Checkstyle `JavadocPackage` rule.
**Fix**: Create the file with license header + package declaration. Test packages are exempt (via suppressions.xml).
**Example**:
```java
/**
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.newpackage;
```

### Test File Naming
**Problem**: Test files named `*Tests.java` (plural) are rejected by checkstyle.
**Fix**: Always use `*Test.java` (singular). E.g., `StorageApiTest.java`, not `StorageApiTests.java`.

### Import Ordering
**Problem**: Checkstyle enforces strict import order.
**Rules**:
1. Static imports first, sorted alphabetically
2. Blank line
3. Regular imports, sorted alphabetically
4. `Preconditions` must be static: `import static com.google.common.base.Preconditions.checkArgument`

### Line Length
**Problem**: Max 120 characters per line.
**Exempt**: Import statements and URLs in comments.
**Fix**: Break long lines at logical points (after commas, before operators).

## SpotBugs Pitfalls

### Generated Code False Positives
**Problem**: LightProto (core) and Protobuf (lakehouse) generate code that triggers SpotBugs violations.
**Rule**: These packages are already excluded in `resources/findbugsExclude.xml`. If you add new `.proto` files in a new package, add the package to the exclude filter.

### Lombok False Positives
**Problem**: Lombok `@Data`, `@Getter`, `@Setter`, `@Builder` generate getters/setters/constructors that trigger `EI_EXPOSE_REP` and `EI_EXPOSE_REP2` (mutable object exposure).
**Rule**: These are excluded globally. Do not add per-class suppressions for Lombok-generated code.

### Vendor Code False Positives
**Problem**: Patched upstream code (`io.delta.kernel`, `org.apache.iceberg.avro`) triggers violations we cannot fix.
**Rule**: These packages are excluded. If you add new vendor packages, add them to the exclude filter.

### Encoding and Locale Bugs
**Problem**: SpotBugs flags `new String(byte[])` (DM_DEFAULT_ENCODING) and `toUpperCase()` without locale (DM_CONVERT_CASE).
**Fix**: Always use `new String(bytes, StandardCharsets.UTF_8)` and `str.toUpperCase(Locale.ROOT)`. These are real bugs — platform-default encoding/locale causes different behavior across environments.

## Concurrency Pitfalls

### Netty Buffer Reference Counting
**Problem**: Netty `ByteBuf` uses reference counting. Forgetting `release()` causes memory leaks; double-release causes crashes.
**Rules**:
- Always release in a `finally` block
- When passing buffers to other methods, document ownership transfer
- Use `ReferenceCountUtil.safeRelease()` for defensive cleanup
- Enable Netty leak detection in ownership-sensitive tests

### CompletableFuture Callbacks
**Problem**: Callbacks on `CompletableFuture` run on the completing thread by default, which can be a Netty I/O thread.
**Rules**:
- Use `*Async` variants with explicit executor for expensive operations
- Never block (wait/join) on a Netty I/O thread
- Chain futures rather than nesting callbacks

### Write/Read Cache Concurrency
**Problem**: `WriteCache` and `EntryCache` are accessed from multiple threads.
**Rules**:
- Review any `PersistCache` changes for thread safety
- `flushSucceed`/`flushFailed` must handle concurrent access correctly

## Dependency Management

### BOM-Managed Versions
**Problem**: Dependency versions are managed by the root dependency-management section and imported BOMs.
**Rule**: Never specify versions for dependencies that are BOM-managed. Only specify versions for dependencies not in any BOM (check root `pom.xml` `<dependencyManagement>` section).

### Maven GitHub Packages Authentication
**Problem**: Build fails with 401/403 on `maven.pkg.github.com`.
**Fix**: Configure `~/.m2/settings.xml` with a GitHub token:
```xml
<server>
  <id>github</id>
  <username>YOUR_GITHUB_USERNAME</username>
  <password>YOUR_GITHUB_TOKEN</password>
</server>
```
Token needs `read:packages` scope.

## Lakehouse-Specific Pitfalls

### v2 vs v1 Code Paths
**Problem**: The lakehouse module has two architectures:
- `io.lakestream.ursa.lakehouse.v2.*` — current, active development
- `io.lakestream.ursa.lakehouse.*` (root packages) — legacy v1
**Rule**: All new lakehouse code goes in `v2/` packages. Don't add to root packages.

### Vendor Code
**Problem**: Lakehouse contains patched upstream code:
- `io.delta.kernel` — Delta Kernel patches
- `org.apache.iceberg.avro` — Iceberg Avro patches
**Rule**: Modify only when necessary to fix upstream bugs. Document why. Prefer upstream fixes.

### Iceberg/Delta Isolation
**Problem**: Iceberg and Delta table format packages must not cross-reference.
**Rule**: Keep `*.iceberg.*` and `*.delta.*` packages isolated. Shared logic goes in common packages.

### Schema Conversion Edge Cases
**Problem**: Schema conversion between Kafka records and Parquet/Avro/Delta/Iceberg formats has many edge cases.
**Rule**: Always test with multiple schema types (Avro, JSON, Protobuf). Check null handling and schema evolution.

## Test Infrastructure

### Docker Required
**Problem**: Integration tests use Testcontainers for Oxia, object storage, and Kafka-facing scenarios.
**Rule**: Docker must be running for integration tests. This repository does not maintain a Docker
Compose stack; Testcontainers starts the required services. Verify the daemon before running tests:
```bash
docker info
```

### Stress Tests in CI
**Problem**: CI runs `stress-ng` alongside integration tests to simulate resource pressure.
**Rule**: Tests must be resilient to CPU/memory pressure. Use `Awaitility` with generous timeouts.

### Parallel Test Execution
**Problem**: JUnit runs test classes concurrently (parallelism=4).
**Rule**: Tests must be independent — no shared mutable state between test classes. Use unique stream/topic names.
