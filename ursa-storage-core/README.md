# Ursa Storage Core

Core storage engine that transforms any file, block, and cloud object storage into high peformance stream storage.

## Overview

This module provides the foundational storage layer for Ursa Storage. It implements a write-ahead log on top of cloud object storage (S3, GCS, Azure) with intelligent caching, batching, and indexing to achieve low-latency reads and high-throughput writes. The core engine separates data (stored in object storage) from metadata (stored in Oxia) for optimal performance and scalability.