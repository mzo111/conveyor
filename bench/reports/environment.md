# Benchmark environment

Collected automatically by `bench/env.py` during the run. Not hand-written.

## Host

| | |
|---|---|
| CPU | 13th Gen Intel(R) Core(TM) i7-13700KF |
| Logical cores | 24 |
| Memory | 15.5 GiB |
| Kernel | 5.15.133.1-microsoft-standard-WSL2 |
| Platform | Linux-5.15.133.1-microsoft-standard-WSL2-x86_64-with-glibc2.39 |
| Docker | Docker version 29.2.1, build a5c7197 |
| Docker storage driver | overlayfs |
| Root disk | 1006.9 GiB total, 943.7 GiB free |
| Python | 3.12.3 |
| psycopg | 3.3.5 |

This is WSL2. The Postgres container writes through the WSL2 virtual disk, so fsync latency is not the same as bare metal on the same SSD, and the workers share these cores with the database.

## PostgreSQL

```
PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit
```

Settings that matter for this workload:

| Setting | Value |
|---|---|
| `autovacuum` | on |
| `checkpoint_timeout` | 300 s |
| `commit_delay` | 0 |
| `effective_cache_size` | 524288 8kB |
| `fsync` | on |
| `full_page_writes` | on |
| `max_connections` | 100 |
| `max_wal_size` | 1024 MB |
| `random_page_cost` | 4 |
| `shared_buffers` | 16384 8kB |
| `synchronous_commit` | on |
| `track_io_timing` | off |
| `wal_buffers` | 512 8kB |
| `wal_level` | replica |
| `work_mem` | 4096 kB |

Every setting not at its compiled-in default (that is, everything the image or Compose file sets):

| Setting | Value |
|---|---|
| `DateStyle` | ISO, MDY |
| `default_text_search_config` | pg_catalog.english |
| `dynamic_shared_memory_type` | posix |
| `lc_messages` | en_US.utf8 |
| `lc_monetary` | en_US.utf8 |
| `lc_numeric` | en_US.utf8 |
| `lc_time` | en_US.utf8 |
| `listen_addresses` | * |
| `log_timezone` | Etc/UTC |
| `max_connections` | 100 |
| `max_wal_size` | 1024 MB |
| `min_wal_size` | 80 MB |
| `shared_buffers` | 16384 8kB |
| `TimeZone` | Etc/UTC |

## Compose service

```yaml
name: conveyor

services:
  db:
    image: postgres:16
    restart: unless-stopped
    environment:
      POSTGRES_USER: conveyor
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-conveyor}
      POSTGRES_DB: conveyor
    ports:
      # 5433 on the host: 5432 is commonly taken by another local Postgres.
      - "5433:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./db/schema.sql:/docker-entrypoint-initdb.d/01-schema.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U conveyor -d conveyor"]
      interval: 5s
      timeout: 5s
      retries: 20

volumes:
  pgdata:
```
