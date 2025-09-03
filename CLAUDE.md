# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Building and Testing
- `sbt compile` - Compile all modules
- `sbt test` - Run unit tests for all modules  
- `sbt it:test` - Run integration tests (requires Docker)
- `sbt "project <module>" test` - Run tests for specific module (e.g., `sbt "project common" test`)
- `sbt publishLocal` - Publish artifacts locally
- `sbt Docker/publishLocal` - Build Docker images locally (for stream applications)

### Module-specific Commands
- `sbt "project kinesis" Docker/publishLocal` - Build Kinesis Docker image
- `sbt "project kafka" Docker/publishLocal` - Build Kafka Docker image  
- `sbt "project pubsub" Docker/publishLocal` - Build PubSub Docker image
- `sbt "project nsq" Docker/publishLocal` - Build NSQ Docker image

### Code Quality
- `sbt scalafmtAll` - Format all Scala code
- `sbt scalafmtCheck` - Check code formatting
- Integration tests require Docker to be running and may modify it automatically

## Architecture Overview

Snowplow Enrich is a multi-module Scala project that processes raw Snowplow events into enriched events. The architecture follows a modular design:

### Core Modules

**common** (`modules/common/`) - Core enrichment logic shared across all applications
- `EtlPipeline` - Main event processing pipeline
- `adapters/` - Event adapters for different sources (Google Analytics, HubSpot, etc.)
- `enrichments/` - Event enrichment implementations (IP lookup, user agent parsing, etc.)
- `loaders/` - Payload loading and parsing
- `outputs/` - Enriched event output models

**core** (`modules/core/`) - Base application framework for stream processing
- `EnrichApp` - Abstract base class for all streaming applications
- `Config` - Configuration parsing and validation
- `Run` - Main application runner with CLI argument parsing
- `Processing` - Core event processing orchestration

### Stream Applications

Each streaming application extends `EnrichApp` and provides platform-specific implementations:

- **kinesis** (`modules/kinesis/`) - AWS Kinesis Streams integration
- **kafka** (`modules/kafka/`) - Apache Kafka integration  
- **pubsub** (`modules/pubsub/`) - Google Cloud Pub/Sub integration
- **nsq** (`modules/nsq/`) - NSQ message queue integration

### Cloud Utilities

**cloudutils** (`modules/cloudutils/`) - Cloud provider abstractions
- `core/` - Common interfaces for blob storage
- `aws/` - AWS S3 implementations
- `gcp/` - Google Cloud Storage implementations  
- `azure/` - Azure Blob Storage implementations

### Key Processing Flow

1. Raw collector payloads are consumed from input streams
2. `AdapterRegistry` converts payloads to `RawEvent`s based on source
3. `EnrichmentManager` applies configured enrichments to events
4. Valid events become `EnrichedEvent`s, invalid ones become `BadRow`s
5. Results are published to appropriate output streams (good/bad/failed)

### Configuration System

Applications use HOCON configuration files with three main sections:
- **Input/Output streams** - Platform-specific source/sink configuration
- **Enrichments** - JSON configuration files for individual enrichments  
- **Iglu resolver** - Schema registry configuration for validation

### Testing Strategy

- **Unit tests** - Test individual components and enrichments
- **Integration tests** - Full end-to-end testing with Docker containers
- Each stream application has IT tests that spin up the relevant infrastructure
- Tests use Specs2 framework with Cats Effect integration

## Development Notes

- Project uses Scala 2.12.20 with SBT build system
- Applications are packaged as Docker images for deployment
- Both focal and distroless Docker variants are built
- Integration tests automatically pull required Docker images
- All stream applications share the same core enrichment logic
- Configuration follows a type-safe approach using Circe decoders