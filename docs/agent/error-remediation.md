# Error Remediation Guide

Common build failures and their fixes.

## License Header Missing

**Error**: `[ERROR] Some files do not have the expected license header`
```
[ERROR] Missing header in: ursa-storage-core/src/main/java/.../NewFile.java
```

**Fix**:
```bash
mvn license:format
```
This auto-adds the license header from `resources/license.template` to all source files.

## Checkstyle Violations

### Missing package-info.java
**Error**: `[ERROR] JavadocPackage: Missing package-info.java file`

**Fix**: Create `package-info.java` in the package directory:
```java
/**
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.newpackage;
```

### Star Import
**Error**: `[ERROR] AvoidStarImport: Using the '.*' form of import should be avoided`

**Fix**: Replace `import java.util.*` with specific imports:
```java
import java.util.List;
import java.util.Map;
import java.util.Optional;
```

### Import Order
**Error**: `[ERROR] CustomImportOrder: Import ... should be in the ... group`

**Fix**: Reorder imports:
```java
// Static imports first (alphabetical)
import static com.google.common.base.Preconditions.checkArgument;
import static org.junit.jupiter.api.Assertions.assertEquals;

// Regular imports (alphabetical)
import io.lakestream.ursa.storage.StorageApi;
import java.util.List;
import org.junit.jupiter.api.Test;
```

### Non-Static Preconditions Import
**Error**: `[ERROR] RegexpSinglelineJava: import com.google.common.base.Preconditions must be static`

**Fix**: Change:
```java
// Wrong
import com.google.common.base.Preconditions;

// Right
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
```

### Line Too Long
**Error**: `[ERROR] LineLength: Line is longer than 120 characters`

**Fix**: Break the line. Common patterns:
```java
// Method parameters
public void longMethod(
        String param1,
        String param2,
        String param3) {
}

// Chained calls
CompletableFuture<Void> future = storage.putAsync(key, value)
        .thenCompose(v -> cache.invalidate(key))
        .exceptionally(ex -> handleError(ex));
```

### Test File Naming
**Error**: `[ERROR] RegexpSinglelineJava: Test class name should end with Test, not Tests`

**Fix**: Rename `FooTests.java` → `FooTest.java`.

## Maven Authentication Errors

**Error**: `[ERROR] Failed to execute goal ... Could not resolve dependencies ... Return code is: 401`

**Fix**: Add GitHub Packages authentication to `~/.m2/settings.xml`:
```xml
<settings>
  <servers>
    <server>
      <id>github</id>
      <username>YOUR_GITHUB_USERNAME</username>
      <password>ghp_YOUR_TOKEN_HERE</password>
    </server>
  </servers>
</settings>
```
Token needs `read:packages` scope. Generate at: GitHub → Settings → Developer Settings → Personal Access Tokens.

## SpotBugs Violations

**Error**: `[ERROR] failed with N bugs and 0 errors`

**Investigate**:
```bash
# Check a single module
mvn -B -ntp spotbugs:check -pl ursa-storage-core

# View detailed report in GUI
mvn spotbugs:gui -pl ursa-storage-core

# View XML report
cat ursa-storage-core/target/spotbugsXml.xml
```

**Decision tree**:
1. **Generated code?** (LightProto, Protobuf) → Already excluded in `findbugsExclude.xml`
2. **Vendor code?** (Delta Kernel, Iceberg Avro) → Already excluded
3. **Lombok false positive?** (EI_EXPOSE_REP on @Data/@Getter) → Already excluded globally
4. **Real bug?** → **Fix it** (see common fixes below)
5. **Other false positive?** → Add class-specific exclusion to `findbugsExclude.xml`

**Common fixes**:

| Bug Pattern | Fix |
|-------------|-----|
| `DM_DEFAULT_ENCODING` | `new String(bytes)` → `new String(bytes, StandardCharsets.UTF_8)` |
| `DM_CONVERT_CASE` | `.toUpperCase()` → `.toUpperCase(Locale.ROOT)` |
| `WMI_WRONG_MAP_ITERATOR` | Use `entrySet()` instead of `keySet()` + `get()` |
| `ICAST_INTEGER_MULTIPLY_CAST_TO_LONG` | Cast to `(long)` before multiplication |
| `MS_SHOULD_BE_FINAL` | Add `final` modifier to static field |
| `ES_COMPARING_PARAMETER_STRING_WITH_EQ` | Use `.equals()` instead of `==` |
| `IT_NO_SUCH_ELEMENT` | Throw `NoSuchElementException` in `next()` when empty |

**Adding an exclusion** to `resources/findbugsExclude.xml`:
```xml
<!-- Explain why this is a false positive -->
<Match>
  <Class name="io.lakestream.ursa.storage.impl.SomeClass"/>
  <Bug pattern="THE_BUG_PATTERN"/>
</Match>
```

## Test Failures

### Read Surefire Reports
```bash
# Find failed test reports
find . -path "*/surefire-reports/*.txt" -exec grep -l "FAILURE\|ERROR" {} \;

# Read a specific report
cat ursa-storage-core/target/surefire-reports/io.lakestream.ursa.storage.impl.SomeTest.txt
```

### Docker Not Running
**Error**: `Could not connect to Docker daemon` or Testcontainers timeout

**Fix**:
```bash
# Start your Docker daemon (for example Docker Desktop), then verify it:
docker info
```

Rerun the integration test after the daemon is available; Testcontainers provisions the required
services for the test.

### OutOfMemoryError in Tests
**Error**: `java.lang.OutOfMemoryError: Java heap space`

**Fix**: Tests are configured with `-Xmx1024M`. If a specific test needs more:
```bash
mvn test -pl ursa-storage-core -Dtest=LargeTest -DtestMaxHeapSize=2048M
```

## Compilation Errors

### Missing Generated Sources
**Error**: `cannot find symbol` for proto-generated classes

**Fix**: Ensure code generation runs first:
```bash
# For lightproto (core)
mvn generate-sources -pl ursa-storage-core

# For protobuf (lakehouse)
mvn generate-sources -pl ursa-storage-lakehouse
```

### Lombok Not Processing
**Error**: `cannot find symbol` for `@Slf4j` log field, `@Getter` methods, etc.

**Fix**: Ensure Lombok annotation processor is configured. IDE may need Lombok plugin installed.
```bash
# Verify build works from CLI
mvn clean compile -pl ursa-storage-core
```
