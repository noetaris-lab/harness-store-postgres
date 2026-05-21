import { randomUUID } from 'node:crypto'
import type { Pool } from 'pg'
import type { SessionStore, StoredRun, ClaimOptions, Lease } from '@noetaris/harness'
import { ConcurrentModificationError, BranchNotFoundError, LeaseNotFoundError } from '../errors.js'

export interface PostgresSessionStoreOptions {
  /**
   * A pre-constructed `pg` Pool instance.
   *
   * The store does not create or manage the Pool lifecycle — the caller
   * is responsible for creating the pool before use and calling `pool.end()`
   * after.
   *
   * Run `migrate()` once before using the store to ensure tables exist.
   */
  readonly pool: Pool
}

// Represents one row in session_runs.
type RunRow = {
  agent_id: string
  session_id: string
  run_id: string
  version: number
  run: string
}

// Token stored in Lease.token — identifies a session_claims row by ownership
type ClaimToken = {
  agentId: string
  sessionId: string
  nonce: string
}

export class PostgresSessionStore implements SessionStore {
  private readonly pool: Pool

  constructor(options: PostgresSessionStoreOptions) {
    this.pool = options.pool
  }

  /** Create `session_runs` and `session_claims` tables if they do not exist. */
  async migrate(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS session_runs (
        agent_id    TEXT      NOT NULL,
        session_id  TEXT      NOT NULL,
        run_id      TEXT      NOT NULL,
        version     INTEGER   NOT NULL,
        run         TEXT      NOT NULL,
        PRIMARY KEY (agent_id, session_id, version)
      )
    `)
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS session_claims (
        agent_id    TEXT        NOT NULL,
        session_id  TEXT        NOT NULL,
        nonce       TEXT        NOT NULL,
        instance_id TEXT,
        expires_at  BIGINT      NOT NULL,
        PRIMARY KEY (agent_id, session_id)
      )
    `)
  }

  async load(agentId: string, sessionId: string): Promise<StoredRun | null> {
    const res = await this.pool.query(
      'SELECT run FROM session_runs WHERE agent_id = $1 AND session_id = $2 ORDER BY version DESC LIMIT 1',
      [agentId, sessionId],
    )
    if (res.rows.length === 0) return null
    // as: pg types rows as any[]; RunRow is the known shape for session_runs
    return JSON.parse((res.rows[0] as RunRow).run) as StoredRun
  }

  async save(agentId: string, sessionId: string, run: StoredRun): Promise<void> {
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      const res = await client.query(
        'SELECT version FROM session_runs WHERE agent_id = $1 AND session_id = $2 ORDER BY version DESC LIMIT 1 FOR UPDATE',
        [agentId, sessionId],
      )
      // as: pg types rows as any[]; RunRow is the known shape for session_runs
      const storedVersion = res.rows.length > 0 ? (res.rows[0] as RunRow).version : -1
      const expectedVersion = storedVersion + 1
      if (run.version !== expectedVersion) {
        await client.query('ROLLBACK')
        throw new ConcurrentModificationError(sessionId, run.version, storedVersion)
      }
      await client.query(
        'INSERT INTO session_runs (agent_id, session_id, run_id, version, run) VALUES ($1, $2, $3, $4, $5)',
        [agentId, sessionId, run.runId, run.version, JSON.stringify(run)],
      )
      await client.query('COMMIT')
    } catch (err) {
      await client.query('ROLLBACK').catch(() => { /* ignore rollback error on already-rolled-back tx */ })
      throw err
    } finally {
      client.release()
    }
  }

  async loadHistory(agentId: string, sessionId: string): Promise<StoredRun[]> {
    const res = await this.pool.query(
      'SELECT run FROM session_runs WHERE agent_id = $1 AND session_id = $2 ORDER BY version ASC',
      [agentId, sessionId],
    )
    // as: pg types rows as any[]; RunRow is the known shape for session_runs
    return res.rows.map((row) => JSON.parse((row as RunRow).run) as StoredRun)
  }

  async branch(agentId: string, sessionId: string, runId: string): Promise<string> {
    const res = await this.pool.query(
      'SELECT run FROM session_runs WHERE agent_id = $1 AND session_id = $2 AND run_id = $3 LIMIT 1',
      [agentId, sessionId, runId],
    )
    if (res.rows.length === 0) {
      throw new BranchNotFoundError(sessionId, runId)
    }
    // as: pg types rows as any[]; RunRow is the known shape for session_runs
    const source = JSON.parse((res.rows[0] as RunRow).run) as StoredRun
    const newSessionId = randomUUID()
    const now = new Date().toISOString()
    const synthetic: StoredRun = {
      agentId,
      runId: randomUUID(),
      sessionId: newSessionId,
      version: 0,
      phase: 'completed',
      initialState: source.finalState,
      finalState: source.finalState,
      startedAt: now,
      settledAt: now,
    }
    await this.save(agentId, newSessionId, synthetic)
    return newSessionId
  }

  async claim(agentId: string, sessionId: string, options: ClaimOptions): Promise<Lease | null> {
    const nonce = randomUUID()
    const expiresAt = Date.now() + options.ttlMs

    const insertRes = await this.pool.query(
      'INSERT INTO session_claims (agent_id, session_id, nonce, instance_id, expires_at) VALUES ($1, $2, $3, NULL, $4) ON CONFLICT (agent_id, session_id) DO NOTHING',
      [agentId, sessionId, nonce, expiresAt],
    )

    if ((insertRes.rowCount ?? 0) === 1) {
      return { expiresAt, agentId, sessionId, token: { agentId, sessionId, nonce } }
    }

    // Conflict — check if existing row has expired
    const existing = await this.pool.query(
      'SELECT expires_at FROM session_claims WHERE agent_id = $1 AND session_id = $2',
      [agentId, sessionId],
    )

    if (
      existing.rows.length === 0 ||
      // as: pg types rows as any[]; session_claims rows carry expires_at as a number
      (existing.rows[0] as { expires_at: number }).expires_at <= Date.now()
    ) {
      // Stale or already gone — delete and retry once
      await this.pool.query(
        'DELETE FROM session_claims WHERE agent_id = $1 AND session_id = $2 AND expires_at <= $3',
        [agentId, sessionId, Date.now()],
      )
      const retryRes = await this.pool.query(
        'INSERT INTO session_claims (agent_id, session_id, nonce, instance_id, expires_at) VALUES ($1, $2, $3, NULL, $4) ON CONFLICT (agent_id, session_id) DO NOTHING',
        [agentId, sessionId, nonce, expiresAt],
      )
      if ((retryRes.rowCount ?? 0) === 0) return null
      return { expiresAt, agentId, sessionId, token: { agentId, sessionId, nonce } }
    }

    // Active claim held by another instance
    return null
  }

  async release(lease: Lease): Promise<void> {
    try {
      // as: PostgresSessionStore sets Lease.token to { agentId, sessionId, nonce }; framework types it unknown
      const { agentId, sessionId, nonce } = lease.token as ClaimToken
      await this.pool.query(
        'DELETE FROM session_claims WHERE agent_id = $1 AND session_id = $2 AND nonce = $3',
        [agentId, sessionId, nonce],
      )
    } catch {
      // release is called from finally blocks — errors must never surface
    }
  }

  async extendClaim(lease: Lease, options: ClaimOptions): Promise<Lease> {
    // as: PostgresSessionStore sets Lease.token to { agentId, sessionId, nonce }; framework types it unknown
    const { agentId, sessionId, nonce } = lease.token as ClaimToken
    const newExpiresAt = Date.now() + options.ttlMs

    const res = await this.pool.query(
      'UPDATE session_claims SET expires_at = $1 WHERE agent_id = $2 AND session_id = $3 AND nonce = $4 RETURNING expires_at',
      [newExpiresAt, agentId, sessionId, nonce],
    )

    if ((res.rowCount ?? 0) === 0) {
      throw new LeaseNotFoundError(sessionId)
    }

    return {
      expiresAt: newExpiresAt,
      agentId: lease.agentId,
      sessionId: lease.sessionId,
      token: lease.token,
      ...(lease.instanceId !== undefined ? { instanceId: lease.instanceId } : {}),
    }
  }
}
