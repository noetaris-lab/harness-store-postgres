import { describe, it, expect, vi, beforeAll, beforeEach, afterAll } from 'vitest'
import { Pool } from 'pg'
import type { StoredRun, Lease } from '@noetaris/harness'
import { PostgresSessionStore } from './postgres-session-store.js'
import { ConcurrentModificationError, BranchNotFoundError, LeaseNotFoundError } from '../errors.js'

const skip = !process.env.PG_URL

let pool: Pool
let store: PostgresSessionStore

beforeAll(async () => {
  if (skip) return
  pool = new Pool({ connectionString: process.env.PG_URL })
  store = new PostgresSessionStore({ pool })
  await store.migrate()
})

beforeEach(async () => {
  if (skip) return
  await pool.query('TRUNCATE session_runs, session_claims')
})

afterAll(async () => {
  if (skip) return
  await pool.end()
})

function makeRun(overrides: Partial<StoredRun> & { version: number }): StoredRun {
  return {
    agentId: 'agentA',
    runId: `run-${overrides.version ?? 0}`,
    sessionId: 'sess1',
    startedAt: '2026-01-01T00:00:00.000Z',
    settledAt: '2026-01-01T00:01:00.000Z',
    phase: 'completed',
    initialState: {},
    finalState: { v: overrides.version ?? 0 },
    ...overrides,
  }
}

describe('PostgresSessionStore', () => {

  describe('migrate() — Schema Creation', () => {

    it.skipIf(skip)('creates both tables when called on a database with no prior schema', async () => {
      // arrange
      await pool.query('DROP TABLE IF EXISTS session_claims, session_runs')

      // act
      await store.migrate()

      // assert
      const res = await pool.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('session_runs', 'session_claims')",
      )
      expect(res.rows.map((r: { table_name: string }) => r.table_name).sort()).toEqual(['session_claims', 'session_runs'])
    })

    it.skipIf(skip)('resolves without error when called a second time (idempotent)', async () => {
      // arrange — tables already exist from beforeAll

      // act + assert
      await expect(store.migrate()).resolves.toBeUndefined()
    })

  })

  describe('load — Reads', () => {

    it.skipIf(skip)('returns null when no rows exist for the session', async () => {
      // arrange — table is empty from TRUNCATE

      // act
      const result = await store.load('agentA', 'sess1')

      // assert
      expect(result).toBeNull()
    })

    it.skipIf(skip)('returns the stored run after a single save', async () => {
      // arrange
      const run0 = makeRun({ version: 0 })
      await store.save('agentA', 'sess1', run0)

      // act
      const result = await store.load('agentA', 'sess1')

      // assert
      expect(result).toEqual(run0)
    })

    it.skipIf(skip)('returns the highest-version row when multiple versions exist', async () => {
      // arrange
      const run0 = makeRun({ version: 0 })
      const run1 = makeRun({ version: 1, runId: 'run-1', finalState: { v: 1 } })
      await store.save('agentA', 'sess1', run0)
      await store.save('agentA', 'sess1', run1)

      // act
      const result = await store.load('agentA', 'sess1')

      // assert
      expect(result).toEqual(run1)
    })

    it.skipIf(skip)('returns null for a different sessionId; the saved session is unaffected', async () => {
      // arrange
      const run0 = makeRun({ version: 0, sessionId: 'sess1' })
      await store.save('agentA', 'sess1', run0)

      // act
      const resultSess2 = await store.load('agentA', 'sess2')
      const resultSess1 = await store.load('agentA', 'sess1')

      // assert
      expect(resultSess2).toBeNull()
      expect(resultSess1).toEqual(run0)
    })

    it.skipIf(skip)('returns null for a different agentId', async () => {
      // arrange
      const run0 = makeRun({ version: 0, agentId: 'agentA' })
      await store.save('agentA', 'sess1', run0)

      // act
      const result = await store.load('agentB', 'sess1')

      // assert
      expect(result).toBeNull()
    })

  })

  describe('save — Optimistic Locking', () => {

    it.skipIf(skip)('resolves without error and persists the run when saving version 0 on a fresh session', async () => {
      // arrange — table is empty

      // act
      await store.save('agentA', 'sess1', makeRun({ version: 0 }))

      // assert
      const loaded = await store.load('agentA', 'sess1')
      expect(loaded).toEqual(makeRun({ version: 0 }))
    })

    it.skipIf(skip)('resolves without error and makes version 1 the latest when saving sequentially after version 0', async () => {
      // arrange
      await store.save('agentA', 'sess1', makeRun({ version: 0 }))

      // act
      await store.save('agentA', 'sess1', makeRun({ version: 1, runId: 'run-1', finalState: { v: 1 } }))

      // assert
      const loaded = await store.load('agentA', 'sess1')
      expect(loaded?.version).toBe(1)
    })

    it.skipIf(skip)('throws ConcurrentModificationError when saving stale version 0 over existing version 0', async () => {
      // arrange
      const run0 = makeRun({ version: 0 })
      await store.save('agentA', 'sess1', run0)
      const run0Copy = makeRun({ version: 0, finalState: { v: 'overwrite' } })

      // act
      const promise = store.save('agentA', 'sess1', run0Copy)

      // assert
      await expect(promise).rejects.toThrow(ConcurrentModificationError)
      await expect(promise).rejects.toMatchObject({
        sessionId: 'sess1',
        attemptedVersion: 0,
        storedVersion: 0,
      })
      expect(await store.load('agentA', 'sess1')).toEqual(run0)
    })

    it.skipIf(skip)('throws ConcurrentModificationError when version skips from 0 to 2', async () => {
      // arrange
      await store.save('agentA', 'sess1', makeRun({ version: 0 }))

      // act
      const promise = store.save('agentA', 'sess1', makeRun({ version: 2, runId: 'run-2' }))

      // assert
      await expect(promise).rejects.toThrow(ConcurrentModificationError)
      await expect(promise).rejects.toMatchObject({
        attemptedVersion: 2,
        storedVersion: 0,
      })
    })

    it.skipIf(skip)('throws ConcurrentModificationError with storedVersion === -1 when saving version 1 on a non-existing session', async () => {
      // arrange — table is empty

      // act
      const promise = store.save('agentA', 'sess1', makeRun({ version: 1, runId: 'run-1' }))

      // assert
      await expect(promise).rejects.toThrow(ConcurrentModificationError)
      await expect(promise).rejects.toMatchObject({
        sessionId: 'sess1',
        attemptedVersion: 1,
        storedVersion: -1,
      })
    })

    it.skipIf(skip)('round-trips all optional StoredRun fields faithfully including metadata.instanceId', async () => {
      // arrange
      const fullRun: StoredRun = {
        agentId: 'agentA',
        runId: 'run-full',
        sessionId: 'sess1',
        startedAt: '2026-01-01T00:00:00.000Z',
        settledAt: '2026-01-01T01:00:00.000Z',
        phase: 'paused',
        initialState: { a: 1 },
        finalState: { a: 1, b: { nested: true } },
        signal: 'pause',
        step: 'stepB',
        metadata: { instanceId: 'inst-xyz', extra: 'field' },
        version: 0,
      }
      await store.save('agentA', 'sess1', fullRun)

      // act
      const loaded = await store.load('agentA', 'sess1')

      // assert
      expect(loaded).toEqual(fullRun)
      // any: accessing dynamic metadata field for assertion
      expect((loaded as any).metadata?.instanceId).toBe('inst-xyz') // any: dynamic metadata access for assertion
    })

  })

  describe('loadHistory — Ordered History', () => {

    it.skipIf(skip)('returns an empty array when no rows exist', async () => {
      // arrange — table is empty

      // act
      const result = await store.loadHistory('agentA', 'sess1')

      // assert
      expect(result).toEqual([])
    })

    it.skipIf(skip)('returns all runs ordered by version ascending (oldest first)', async () => {
      // arrange
      const run0 = makeRun({ version: 0 })
      const run1 = makeRun({ version: 1, runId: 'run-1', finalState: { v: 1 } })
      await store.save('agentA', 'sess1', run0)
      await store.save('agentA', 'sess1', run1)

      // act
      const result = await store.loadHistory('agentA', 'sess1')

      // assert
      expect(result).toHaveLength(2)
      expect(result[0]).toEqual(run0)
      expect(result[1]).toEqual(run1)
    })

    it.skipIf(skip)('returns only rows for the specified session, not for other sessions', async () => {
      // arrange
      const runA = makeRun({ version: 0, sessionId: 'sess1', runId: 'run-a' })
      const runB = makeRun({ version: 0, sessionId: 'sess2', runId: 'run-b' })
      await store.save('agentA', 'sess1', runA)
      await store.save('agentA', 'sess2', runB)

      // act
      const result = await store.loadHistory('agentA', 'sess1')

      // assert
      expect(result).toHaveLength(1)
      expect(result[0]).toEqual(runA)
    })

  })

  describe('branch — Session Branching', () => {

    it.skipIf(skip)('returns a new sessionId and inserts a seed row with correct state and phase', async () => {
      // arrange
      const sourceRun = makeRun({ version: 0, runId: 'run-src', finalState: { answer: 42 } })
      await store.save('agentA', 'sess1', sourceRun)

      // act
      const newSessionId = await store.branch('agentA', 'sess1', 'run-src')

      // assert
      expect(typeof newSessionId).toBe('string')
      expect(newSessionId).toMatch(/^[0-9a-f-]{36}$/)
      const seed = await store.load('agentA', newSessionId)
      expect(seed).not.toBeNull()
      expect(seed!.version).toBe(0)
      expect(seed!.phase).toBe('completed')
      expect(seed!.initialState).toEqual({ answer: 42 })
      expect(seed!.finalState).toEqual({ answer: 42 })
    })

    it.skipIf(skip)('throws BranchNotFoundError when the runId does not exist in the session', async () => {
      // arrange
      await store.save('agentA', 'sess1', makeRun({ version: 0, runId: 'run-real' }))

      // act
      const promise = store.branch('agentA', 'sess1', 'nonexistent-run-id')

      // assert
      await expect(promise).rejects.toThrow(BranchNotFoundError)
    })

    it.skipIf(skip)('throws BranchNotFoundError when the runId exists in a different session', async () => {
      // arrange
      const run = makeRun({ version: 0, runId: 'run-abc', sessionId: 'sess1' })
      await store.save('agentA', 'sess1', run)

      // act
      const promise = store.branch('agentA', 'sess2', 'run-abc')

      // assert
      await expect(promise).rejects.toThrow(BranchNotFoundError)
    })

  })

  describe('claim — Distributed Lock Acquisition', () => {

    it.skipIf(skip)('returns a Lease with correct fields when no claim row exists', async () => {
      // arrange
      const before = Date.now()

      // act
      const lease = await store.claim('agentA', 'sess1', { ttlMs: 5000 })

      // assert
      expect(lease).not.toBeNull()
      expect(lease!.agentId).toBe('agentA')
      expect(lease!.sessionId).toBe('sess1')
      expect(lease!.expiresAt).toBeGreaterThan(before)
      expect(lease!.expiresAt).toBeLessThanOrEqual(Date.now() + 5000 + 50)
      const token = lease!.token as { agentId: string; sessionId: string; nonce: string }
      expect(token.agentId).toBe('agentA')
      expect(token.sessionId).toBe('sess1')
      expect(typeof token.nonce).toBe('string')
      expect(token.nonce).toMatch(/^[0-9a-f-]{36}$/)
    })

    it.skipIf(skip)('returns null when an active claim already exists', async () => {
      // arrange
      await store.claim('agentA', 'sess1', { ttlMs: 5000 })

      // act
      const lease = await store.claim('agentA', 'sess1', { ttlMs: 5000 })

      // assert
      expect(lease).toBeNull()
    })

    it.skipIf(skip)('acquires the claim when the existing row has expired', { timeout: 5000 }, async () => {
      // arrange
      await store.claim('agentA', 'sess1', { ttlMs: 50 })
      await new Promise(resolve => setTimeout(resolve, 100))

      // act
      const newLease = await store.claim('agentA', 'sess1', { ttlMs: 5000 })

      // assert
      expect(newLease).not.toBeNull()
    })

  })

  describe('release — Lock Release and Idempotency', () => {

    it.skipIf(skip)('removes the claim row; subsequent claim by the same store succeeds', async () => {
      // arrange
      const lease = await store.claim('agentA', 'sess1', { ttlMs: 5000 })

      // act
      await store.release(lease!)

      // assert
      const newLease = await store.claim('agentA', 'sess1', { ttlMs: 5000 })
      expect(newLease).not.toBeNull()
    })

    it.skipIf(skip)('resolves without error when no claim row exists (idempotent)', async () => {
      // arrange
      const lease = await store.claim('agentA', 'sess1', { ttlMs: 5000 })
      await store.release(lease!)

      // act + assert
      await expect(store.release(lease!)).resolves.toBeUndefined()
    })

    it('resolves without error when the database throws during the release query', async () => {
      // arrange
      const fakePool = {
        query: vi.fn().mockRejectedValue(new Error('connection refused')),
        connect: vi.fn(),
      }
      const errorStore = new PostgresSessionStore({ pool: fakePool as unknown as Pool })
      const fakeLease: Lease = {
        agentId: 'agentA',
        sessionId: 'sess1',
        expiresAt: Date.now() + 5000,
        token: { agentId: 'agentA', sessionId: 'sess1', nonce: 'any-nonce' },
      }

      // act + assert
      await expect(errorStore.release(fakeLease)).resolves.toBeUndefined()
    })

    it.skipIf(skip)('does not delete the claim when the nonce does not match', async () => {
      // arrange
      const leaseA = await store.claim('agentA', 'sess1', { ttlMs: 5000 })
      const leaseB: Lease = {
        agentId: 'agentA',
        sessionId: 'sess1',
        expiresAt: Date.now() + 5000,
        token: { agentId: 'agentA', sessionId: 'sess1', nonce: 'stale-nonce-B' },
      }

      // act
      await store.release(leaseB)

      // assert
      const reClaim = await store.claim('agentA', 'sess1', { ttlMs: 5000 })
      expect(reClaim).toBeNull()

      // cleanup
      await store.release(leaseA!)
    })

  })

  describe('extendClaim — TTL Extension', () => {

    it.skipIf(skip)('returns a new Lease with updated expiresAt and unchanged token', async () => {
      // arrange
      const lease = await store.claim('agentA', 'sess1', { ttlMs: 5000 })
      const originalExpiresAt = lease!.expiresAt

      // act
      const newLease = await store.extendClaim(lease!, { ttlMs: 10_000 })

      // assert
      expect(newLease.expiresAt).toBeGreaterThan(originalExpiresAt)
      expect(newLease.expiresAt).toBeLessThanOrEqual(Date.now() + 10_000 + 50)
      expect(newLease.agentId).toBe('agentA')
      expect(newLease.sessionId).toBe('sess1')
      expect(newLease.token).toBe(lease!.token)
      await expect(store.release(newLease)).resolves.toBeUndefined()
    })

    it.skipIf(skip)('throws LeaseNotFoundError with correct sessionId when no claim row exists', async () => {
      // arrange
      const lease = await store.claim('agentA', 'sess1', { ttlMs: 5000 })
      await store.release(lease!)

      // act
      const promise = store.extendClaim(lease!, { ttlMs: 5000 })

      // assert
      await expect(promise).rejects.toThrow(LeaseNotFoundError)
      await expect(promise).rejects.toMatchObject({ sessionId: 'sess1' })
    })

    it.skipIf(skip)('throws LeaseNotFoundError when the nonce does not match', async () => {
      // arrange
      await store.claim('agentA', 'sess1', { ttlMs: 5000 })
      const staleToken = { agentId: 'agentA', sessionId: 'sess1', nonce: 'old-nonce-B' }
      const leaseB: Lease = {
        agentId: 'agentA',
        sessionId: 'sess1',
        expiresAt: Date.now() + 5000,
        token: staleToken,
      }

      // act
      const promise = store.extendClaim(leaseB, { ttlMs: 5000 })

      // assert
      await expect(promise).rejects.toThrow(LeaseNotFoundError)
      await expect(promise).rejects.toMatchObject({ sessionId: 'sess1' })
    })

  })

  describe('Cross-Instance Scenarios', () => {

    it.skipIf(skip)('second store instance reads data saved by the first', async () => {
      // arrange
      const storeA = new PostgresSessionStore({ pool })
      const storeB = new PostgresSessionStore({ pool })
      const run0 = makeRun({ version: 0, finalState: { result: 99 } })
      await storeA.save('agentA', 'sess1', run0)

      // act
      const loaded = await storeB.load('agentA', 'sess1')

      // assert
      expect(loaded).toEqual(run0)
    })

    it.skipIf(skip)("second writer's save throws ConcurrentModificationError when first already wrote", async () => {
      // arrange
      const storeA = new PostgresSessionStore({ pool })
      const storeB = new PostgresSessionStore({ pool })
      const run0 = makeRun({ version: 0 })
      await storeA.save('agentA', 'sess1', run0)
      const runA1 = makeRun({ version: 1, runId: 'run-A1', finalState: { winner: 'A' } })
      const runB1 = makeRun({ version: 1, runId: 'run-B1', finalState: { winner: 'B' } })
      await storeA.save('agentA', 'sess1', runA1)

      // act
      const promise = storeB.save('agentA', 'sess1', runB1)

      // assert
      await expect(promise).rejects.toThrow(ConcurrentModificationError)
      const loaded = await storeA.load('agentA', 'sess1')
      expect(loaded).toEqual(runA1)
    })

  })

})
