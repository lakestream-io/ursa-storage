# Build and run locally

## Prerequisites

- Java 17+
- Maven 3.6.3+
- Docker for integration tests
- A GitHub token with `read:packages` when private packages are required

Configure the Maven `github` server in `~/.m2/settings.xml` if needed:

```xml
<servers>
  <server>
    <id>github</id>
    <username>YOUR_GITHUB_ACCOUNT</username>
    <password>YOUR_GITHUB_TOKEN</password>
  </server>
</servers>
```

## Build

```bash
mvn -B -ntp clean install -DskipTests
```

## Quality gates

```bash
mvn -B -ntp license:check
mvn -B -ntp checkstyle:check
mvn -B -ntp spotbugs:check
```

## Local dependencies

This repository does not maintain a Docker Compose stack. Integration tests use Testcontainers to
provision Oxia, object storage, and other required services. Start your Docker daemon and verify it:

```bash
docker info
```

Protocol-facing services are not required to run core unit tests.

## Tests

```bash
# Unit tests
mvn -B -ntp test

# One module
mvn -B -ntp test -pl ursa-storage-core

# Full reactor verification
mvn -B -ntp verify
```

Integration tests use Testcontainers and require a running Docker daemon.
