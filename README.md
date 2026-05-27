# @noetaris/harness-store-postgres

PostgreSQL session store for [@noetaris/harness](https://github.com/noetaris-lab/harness). Provides `PostgresSessionStore` — a fully-featured database-backed implementation with optimistic locking, full session history, branching, and distributed claim/lease support.

## Overview

`@noetaris/harness-store-postgres` implements session persistence for the Harness agent framework using PostgreSQL.

Key characteristics:
- **Optimistic locking** — `save()` uses a serializable transaction with `FOR UPDATE`; throws `ConcurrentModificationError` on version mismatch
- **Full history** — `loadHistory()` returns all runs in chronological order
- **Branching** — `branch()` creates a new session from any historical run
- **Distributed claim/lease** — `claim()`/`release()`/`extendClaim()` backed by a `session_claims` table

## Installation

```bash
pnpm add @noetaris/harness-store-postgres
```

Peer dependencies — install these alongside the package:

```bash
pnpm add @noetaris/harness pg
```

## Quick Start

```typescript
import pg from 'pg'
import { PostgresSessionStore } from '@noetaris/harness-store-postgres'

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL })
const store = new PostgresSessionStore({ pool })

// Create tables on first use
await store.migrate()
```

Pass the store to your harness instance:

```typescript
import { createHarness } from '@noetaris/harness'

const harness = createHarness({ store, /* ...other options */ })
```

Close the pool when done:

```typescript
await pool.end()
```

## Database Schema

`migrate()` creates two tables if they do not already exist. Run it once on startup (it is idempotent).

### `session_runs`

Stores every `StoredRun` for every session, one row per run.

| Column | Type | Description |
|--------|------|-------------|
| `agent_id` | `TEXT` | Agent identifier |
| `session_id` | `TEXT` | Session identifier |
| `run_id` | `TEXT` | Unique run identifier |
| `version` | `INTEGER` | Monotonically increasing version (starts at 0) |
| `run` | `TEXT` | JSON-serialized `StoredRun` |

Primary key: `(agent_id, session_id, version)`

### `session_claims`

Tracks active distributed leases, one row per active claim.

| Column | Type | Description |
|--------|------|-------------|
| `agent_id` | `TEXT` | Agent identifier |
| `session_id` | `TEXT` | Session identifier |
| `nonce` | `TEXT` | UUID generated at claim time; guards release and extend |
| `instance_id` | `TEXT` | Optional caller-supplied instance identifier |
| `expires_at` | `BIGINT` | Expiry as Unix milliseconds |

Primary key: `(agent_id, session_id)`

## API Reference

### `PostgresSessionStore`

```typescript
new PostgresSessionStore(options: PostgresSessionStoreOptions)
```

| Option | Type | Description |
|--------|------|-------------|
| `pool` | `Pool` | A pre-constructed `pg` `Pool` instance. The store does not manage the pool lifecycle — create the pool before use and call `pool.end()` after. |

#### `migrate(): Promise<void>`

Creates `session_runs` and `session_claims` tables if they do not exist. Must be called once before the store is used. Safe to call on every startup (idempotent).

#### `load(agentId, sessionId): Promise<StoredRun | null>`

Returns the latest run for the session (highest version), or `null` if none exists.

#### `save(agentId, sessionId, run): Promise<void>`

Writes the run inside a transaction with `FOR UPDATE` row locking. Throws `ConcurrentModificationError` if `run.version` does not equal `storedVersion + 1`.

#### `loadHistory(agentId, sessionId): Promise<StoredRun[]>`

Returns all runs for the session in chronological order (version ascending). Returns an empty array if no runs exist.

#### `branch(agentId, sessionId, runId): Promise<string>`

Creates a new session by forking from a specific run. The new session is initialised with the source run's `finalState`. Returns the new session UUID.

```typescript
try {
  const newSessionId = await store.branch('my-agent', 'session-original', 'run-abc123')
  console.log(`Branched to: ${newSessionId}`)
} catch (err) {
  if (err instanceof BranchNotFoundError) {
    console.error('Run not found in session history')
  }
}
```

#### `claim(agentId, sessionId, options): Promise<Lease | null>`

Acquires a distributed lease using an `INSERT … ON CONFLICT DO NOTHING` pattern. Automatically evicts stale (expired) claims before retrying. Returns `null` if another instance holds an active claim.

#### `release(lease): Promise<void>`

Releases the lease using a nonce-guarded `DELETE`. Never throws — safe to call from `finally` blocks.

#### `extendClaim(lease, options): Promise<Lease>`

Extends the lease by updating `expires_at` in `session_claims`, verified by nonce. Throws `LeaseNotFoundError` if the row is absent or the nonce does not match.

## Error Handling

### `ConcurrentModificationError`

Thrown by `save()` when the stored version does not match the expected version.

```typescript
import { ConcurrentModificationError } from '@noetaris/harness-store-postgres'

try {
  await store.save('my-agent', 'session-123', run)
} catch (err) {
  if (err instanceof ConcurrentModificationError) {
    // err.sessionId        — the session that conflicted
    // err.attemptedVersion — the version the caller tried to write
    // err.storedVersion    — the version currently in the store
    console.error(`Concurrent write: ${err.message}`)
  }
}
```

### `BranchNotFoundError`

Thrown by `branch()` when the specified `runId` is not found in the session's history.

```typescript
import { BranchNotFoundError } from '@noetaris/harness-store-postgres'

try {
  await store.branch('my-agent', 'session-123', 'nonexistent-run')
} catch (err) {
  if (err instanceof BranchNotFoundError) {
    console.error(`Branch failed: ${err.message}`)
  }
}
```

### `LeaseNotFoundError`

Thrown by `extendClaim()` when the claim row is absent or the nonce does not match.

```typescript
import { LeaseNotFoundError } from '@noetaris/harness-store-postgres'

try {
  const newLease = await store.extendClaim(lease, { ttlMs: 10_000 })
} catch (err) {
  if (err instanceof LeaseNotFoundError) {
    // err.sessionId — the session whose claim was not found
    console.error(`Lease gone: ${err.message}`)
  }
}
```

The framework's `ctx.keepAlive()` handles `LeaseNotFoundError` internally — callers of `ctx.keepAlive()` do not need to catch it.

## Distributed Concurrency

`PostgresSessionStore` implements the two-layer hybrid concurrency model:

- **Layer 1 — Optimistic locking (mandatory):** `save()` uses a transaction with `SELECT … FOR UPDATE` and a version check. Concurrent writes are detected and rejected with `ConcurrentModificationError`.
- **Layer 2 — Claim/lease (optional):** `claim()` uses `INSERT … ON CONFLICT DO NOTHING` plus stale-claim eviction; `release()` and `extendClaim()` use nonce-guarded queries. A random UUID nonce is generated at claim time — stale leases cannot release or extend a new holder's lock.

See [Distributed Deployment](https://github.com/noetaris-lab/harness-doc/blob/main/distributed-deployment.md) for the full concurrency model.

## When to Use `PostgresSessionStore`

**Good for:**
- Production systems requiring durable, queryable session history
- Multi-process and multi-machine deployments
- Workloads needing session branching (e.g. experiments, replays, forks)
- Teams already running PostgreSQL who want a single storage backend

**Not suitable for:**
- Single-process development (use `InMemorySessionStore` or `LocalFileSessionStore` from `@noetaris/harness-store` instead)
- Ultra-high-throughput workloads where database round-trips per step are a bottleneck (consider `@noetaris/harness-store-redis`)

## License

MIT
