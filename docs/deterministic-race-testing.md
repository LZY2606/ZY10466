# Deterministic race tests: timeout, stop and output backpressure

The tests in `src/test/java/org/zeroturnaround/exec/test/race/` cover the three most
intermittent paths of the executor — exiting near a timeout, both output streams
filling up, and the stopper chain — without sleeps, a platform shell, real signals or
real processes. Every run of a test executes the exact same event sequence, so a
failure is reproducible and shrinks to the shortest scripted prefix by construction.

## How determinism is achieved

- **Controlled `Process` substitute.** `ScriptedProcess` implements
  `java.lang.Process`. Nothing happens unless the test scripts it: `writeOut`,
  `writeErr`, `exit`, plus the observations the library makes (`waitFor`, `destroy`,
  `interrupt`, stream reads/closes). All state lives in the instance; there is no
  hidden global state behind the fake.
- **Event log.** Every scripted operation and every library observation is appended
  to a per-test `EventLog` with the producing thread's name. Awaiting an event
  (`log.await("waitFor")`) replaces sleeps; a missing event fails with the sequence
  recorded so far.
- **Failure report.** `RaceReportRule` wraps each test: on failure it prints the
  event sequence, the fake's resource state (bytes read/pending/closed per stream,
  destroy count) and the stacks of any live `zt-exec-*` / `WaitForProcess-*` worker
  threads; after a pass it fails the test if any such thread is still alive, so no
  background work or temporary artifacts survive a test.
- **Safeguards, not timing.** Internal waits carry an upper bound (30 s) that only
  fires in a broken run. The passing path never depends on wall-clock time.

## Design decisions and the alternatives considered

### 1. Trust boundary of the fake `Process`

- **Chosen: a `protected` seam on `ProcessExecutor`.** `invokeStart()` was widened
  from `private` to `protected` (its only change; the default implementation still
  just calls `ProcessBuilder.start()` and enriches exceptions). The test subclass
  `ScriptedProcessExecutor` returns the fake from `invokeStart()`. Everything after
  that point — stream pumping, waiting, stopping, closing — is the production code
  path operating on the returned `Process` through the standard interface only, so
  default configuration semantics are unchanged.
- Alternative: inject a `ProcessBuilder`/`ProcessFactory` collaborator. Rejected:
  it adds new public API surface and a second way to configure the executor, while
  the protected-method seam already exists as the established extension style of the
  class (`newExecutor`, `invokeSubmit`, `wrapTask` are protected for the same reason).
- Alternative: reflection to swap the `ProcessBuilder`. Rejected: it reaches into
  private state, breaks under a security manager / JPMS strong encapsulation, and
  hides the dependency the test has on the library.

### 2. Timeout linearization point

- **Chosen: a gated future.** `ScriptedProcessExecutor.invokeSubmit` wraps the real
  future in `GatedFuture`, whose timed `get` parks on a test-fired `TimeoutGate`.
  When the gate fires, the get re-checks the delegate exactly once: a finished task
  yields its result (completion linearized first), a running task yields
  `TimeoutException` (timeout linearized first). This mirrors
  `FutureTask.get(timeout, unit)` — whichever happens first wins — but makes "first"
  a decision of the test. The configured timeout value still flows into messages.
- Alternative: real short timeouts (e.g. 1 ms). Rejected: the outcome depends on the
  scheduler; under load the process can "win" or "lose" nondeterministically, which
  is exactly the intermittence being removed.
- Alternative: an injected clock into `ProcessExecutor.waitFor`. Rejected: the
  linearization point is the future's timed get, not a clock read; a clock would add
  production surface for no additional control.

### 3. Memory bound for the backpressure case

- **Chosen: the pump read-ahead bound.** While a consumer blocks, a `StreamPumper`
  holds at most one buffer (1024 bytes, `StreamPumper`'s default) between the process
  and the consumer. `StreamBackpressureTest.blockedConsumerBoundsReadAhead` asserts
  `consumed - delivered <= 1024` against the fake stream's read counter — a stable,
  measurable bound — and the failure report points at the pump thread parked in
  `write()`. The consumer's block itself has an explicit upper bound
  (`BlockingOutputStream.BLOCK_SAFEGUARD_MILLIS`) after which the write fails instead
  of hanging the pump.
- Alternative: an unbounded in-memory queue between pump and consumer. Rejected:
  that is precisely the unbounded-memory failure mode the bound is meant to exclude.
- Alternative: measuring heap usage. Rejected: GC-dependent and unstable across JVMs;
  counting bytes at the stream boundary is exact.

## Directed mutations covered

- Success after timeout (`successAfterTimeoutDoesNotOverride`) and success before
  timeout (`successBeforeTimeoutLinearizationWins`): the two linearization orders
  have stable, opposite outcomes.
- Undrained tail (`tailWrittenAtExitIsDrained`): bytes written right at exit are
  drained before the output snapshot.
- Duplicate stopper (`duplicateStopperInvocationsStayVisible`,
  `duplicateCancelDoesNotDuplicateStop`): repeated stops stay visible and
  idempotent.
- Exception override (`invalidExitValueExceptionSurvivesUnwrapping`): a failing
  exit-value check survives the executor's exception unwrapping with type, message
  and result intact.
- Process ignoring interrupts
  (`processIgnoringInterruptYieldsTimeoutAndWorkerUnwindsOnExit`).

## Verification evidence

- `./gradlew assemble` and `./gradlew test`: 131 tests, 0 failures (26 test classes,
  including the 22 race tests in `org.zeroturnaround.exec.test.race`).
- The race package was re-run 5 times with `--rerun-tasks`: identical pass, ~1 s per
  run, no safeguard ever fired.
- Negative tests (`HarnessDiagnosticsTest`) prove the diagnostics themselves: a
  missing event fails with the recorded sequence, a failing test's report contains
  the event sequence, the fake's resource state and the worker thread dump, and a
  leaked worker thread is detected after the test.
