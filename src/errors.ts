export class ConcurrentModificationError extends Error {
  readonly sessionId: string
  readonly attemptedVersion: number
  readonly storedVersion: number

  constructor(sessionId: string, attemptedVersion: number, storedVersion: number) {
    super(
      `session "${sessionId}" was modified concurrently — attempted version ${attemptedVersion}, stored version ${storedVersion}`,
    )
    this.name = 'ConcurrentModificationError'
    this.sessionId = sessionId
    this.attemptedVersion = attemptedVersion
    this.storedVersion = storedVersion
  }
}

export class BranchNotFoundError extends Error {
  constructor(sessionId: string, runId: string) {
    super(`branch target not found: sessionId=${sessionId}, runId=${runId}`)
    this.name = 'BranchNotFoundError'
  }
}

export class LeaseNotFoundError extends Error {
  readonly sessionId: string

  constructor(sessionId: string) {
    super(`claim for session "${sessionId}" not found or nonce mismatch — lease may have expired`)
    this.name = 'LeaseNotFoundError'
    this.sessionId = sessionId
  }
}
