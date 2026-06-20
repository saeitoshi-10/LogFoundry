-- LogFoundry Schema
-- Partitioned logs table with full-text search and composite indexes.
--
-- Design decisions:
--   1. PARTITION BY RANGE (timestamp) enables partition pruning on time-range queries.
--      The query layer uses >= and < (not BETWEEN) to avoid scanning boundary partitions.
--   2. Primary key is (id, timestamp) because PostgreSQL requires the partition key
--      in the primary key for partitioned tables. This also enables ON CONFLICT DO NOTHING
--      for idempotent at-least-once consumer inserts.
--   3. GIN index on to_tsvector('english', message) powers sub-millisecond full-text search.
--   4. Composite index on (service, level, timestamp DESC) accelerates filtered queries
--      and leverages the partition key for further pruning.

-- ============================================================
-- Main partitioned table
-- ============================================================
CREATE TABLE IF NOT EXISTS logs (
    id          UUID        NOT NULL,
    service     TEXT        NOT NULL,
    level       TEXT        NOT NULL,
    message     TEXT        NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    trace_id    TEXT,
    metadata    JSONB,
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

-- ============================================================
-- Monthly partitions (6 months of runway)
-- ============================================================
CREATE TABLE IF NOT EXISTS logs_2026_04 PARTITION OF logs
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE TABLE IF NOT EXISTS logs_2026_05 PARTITION OF logs
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE IF NOT EXISTS logs_2026_06 PARTITION OF logs
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE TABLE IF NOT EXISTS logs_2026_07 PARTITION OF logs
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

CREATE TABLE IF NOT EXISTS logs_2026_08 PARTITION OF logs
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

CREATE TABLE IF NOT EXISTS logs_2026_09 PARTITION OF logs
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');

-- ============================================================
-- Indexes (created on the parent table — PostgreSQL propagates to partitions)
-- ============================================================

-- Full-text search index for message content
CREATE INDEX IF NOT EXISTS logs_message_gin
    ON logs USING GIN (to_tsvector('english', message));

-- Service + level filtered queries with timestamp ordering
CREATE INDEX IF NOT EXISTS logs_service_level
    ON logs (service, level, timestamp DESC);

-- Trace ID lookups for distributed tracing correlation
CREATE INDEX IF NOT EXISTS logs_trace_id
    ON logs (trace_id)
    WHERE trace_id IS NOT NULL;
