# Performance Test

This module provides a self-contained Helm deployment for running Ursa Storage performance tests. It deploys a single-node Oxia instance, an Ursa benchmark pod, and the configuration that connects them.

Oxia runs in `standalone` mode and initializes its `default` namespace automatically. This is suitable for development and benchmark orchestration, but it is not a production Oxia topology because it has no replication.

## Prerequisites

- Kubernetes and Helm 3.
- Docker or another tool that can build the Ursa benchmark image.

Build the Ursa tools archive and the benchmark image from the repository root. The default image tag matches `Chart.appVersion`:

```bash
mvn -pl ursa-storage-tools -am -DskipTests package
docker build -f performance/Dockerfile \
  -t ursa-storage-performance:1.0.0 .
```

Load that image into the local Kubernetes cluster when the cluster does not share the Docker daemon. Alternatively, set `ursa.image.repository` and `ursa.image.tag` to a published image.

## Run the performance test

1. Deploy Oxia and the Ursa benchmark pod from the repository root:

```bash
helm upgrade --install perf ./performance
kubectl rollout status statefulset/perf-oxia
kubectl rollout status statefulset/perf-ursa
```

2. Find the benchmark pod:

```bash
kubectl get pods -l app.kubernetes.io/name=ursa-storage-performance
```

3. Run the performance test. The generated configuration is mounted at `/opt/ursa/conf/ursa-storage.conf` and contains both `metadataStoreUrl` and `oxiaStorageUrl` pointing to the in-cluster Oxia service:

```bash
kubectl exec -it perf-ursa-0 -- \
  /opt/ursa/bin/ursa perf -c /opt/ursa/conf/ursa-storage.conf -h

kubectl exec -it perf-ursa-0 -- \
  /opt/ursa/bin/ursa perf -c /opt/ursa/conf/ursa-storage.conf
```

The default chart uses `LOCAL` storage at `/var/lib/ursa/storage`, backed by a per-pod persistent volume. For a durable or multi-pod benchmark, set `ursa.config.backendStorageType` to the object-store backend and provide the corresponding settings through `ursa.config.extraProperties`. Do not use multiple Ursa replicas with the default local backend because each pod has its own volume.
