---
title: ZeaOS Documentation
---

# ZeaOS

The interactive data shell that takes you from raw file to Apache Iceberg warehouse — no Spark, no Glue, no pipeline code.

## Getting Started

- [Getting Started with Docker and MinIO](getting-started-docker) — Spin up ZeaOS + MinIO with Docker Compose, load data, push as Iceberg, query with DuckDB
- [Installation Guide](installation) — Homebrew install, ZeaDrive setup, S3 cloud backends

## Tutorials

- [NYC Taxi Analysis](tutorial-nyc-taxi) — End-to-end walkthrough: load, filter, group, pivot, visualize, save
- [Publish NYC Taxi Analysis to dbt](tutorial-nyc-taxi-publish) — Promote, validate, and publish the taxi session to GitHub as a dbt Core project
- [Home Energy Monitoring with MinIO and Iceberg](homelab-minio-iceberg) — Day-over-day Iceberg append workflow with a real-world sensor dataset

## Reference

- [dbt Export](dbt-export) — `model promote`, `model validate`, `model export`: workflow and generated bundle reference
- [GitHub Publishing](git-publish) — Push dbt bundles directly to GitHub: existing repos, new repos, pull requests, PAT management

## Whitepaper

- [From Exploration to Production: Analysis That Knows Where It Came From](whitepaper-exploration-to-production) — The full path from interactive DuckDB analysis to dbt Core to MotherDuck production, with provenance intact at every step.

Source and README: [github.com/open-tempest-labs/zeaos](https://github.com/open-tempest-labs/zeaos)
