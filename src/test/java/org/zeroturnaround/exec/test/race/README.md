# Deterministic race tests: timeout, stop and output backpressure

This package turns the three flakiest paths of zt-exec — exiting near the
timeout, filling both output streams, and the stopper chain — into deterministic
tests. Every scenario is a scripted sequence of events (`start`, `writeOut`,
`writeErr`, `exit`, `timeout`, `interrupt`, `stopComplete`) executed against the
unmodified production code of `WaitForProcess`, `PumpStreamHandler`, the closers
and the stoppers. There are no sleeps, no platform shells, no real signals and no
random sources; all synchronization is event driven and every deadline in the
harness is a hang guard, never a timing assumption.

## Pieces

- `ScriptedProcess` — a controlled `Process` double. The test thread drives its
  lifecycle through script operations; the library threads observe a normal
  `Process`. Everything it sees (interrupt, destroy, stream close) is recorded.
- `ScriptedPipe` — one stdout/stderr direction of the double, with an optional
  byte budget that models a full OS pipe and makes backpressure observable.
- `ScriptedExecutor` + `TimeoutGate` — a `ProcessExecutor` that injects the
  double and moves the timeout linearization point under script control.
- `EventTracer` — the totally ordered event log; failures are reported as the
  shortest diverging prefix plus a thread and resource snapshot.
- `RaceTestBase` — scenario plumbing, assertions, a hang-guard rule and a
  thread-leak rule that fails the test if any pump, worker or driver thread is
  still alive afterwards (no background work or temporary state survives).
- `Driver`, `GatedOutputStream`, `GatedLineConsumer` — a thread to run
  `execute()` while the test thread drives the script, and consumers that block
  deterministically.

## Decision: the trust boundary of the fake `Process`

Options considered:

1. **Protected `ProcessExecutor.startProcess()` hook (chosen).** The executor
   delegates process creation to one protected method; the default
   implementation calls `ProcessBuilder.start()`, so the default configuration
   keeps its exact previous semantics. The double is instance-local — no static
   state, no leakage between scenarios — and everything downstream of process
   creation (stream pumping, waiting, stopping, closing, exit-value checking)
   is the real production code.
2. A static, injectable `ProcessFactory`. Rejected: hidden global state that
   leaks between tests and between library users in the same JVM.
3. Mocking `ProcessBuilder`/`Process` via reflection or a Java agent.
   Rejected: depends on JDK internals and changes the code under test.

New error branches of the double (close failures, dropped writes, abrupt exit)
surface through the existing contracts — `IOException` from `execute()`, the
`Process` exit/destroy semantics, byte counts on the pipes — so tests
distinguish them through the interfaces the library already exposes, not
through new exception types.

## Decision: the timeout linearization point

Options considered:

1. **A scripted `Future` via the existing `invokeSubmit` hook (chosen).**
   `ProcessExecutor.waitFor` waits on `invokeSubmit(...).get(timeout, unit)`.
   `ScriptedExecutor` runs the wait task on the real executor but returns a
   `LinearizableFuture` whose timed `get` blocks until the script either
   completes the task or fires `TimeoutGate.fireTimeout()`. The linearization
   order of *exit vs. timeout* is chosen by the test, deterministically:
   - exit linearizes first → the result is delivered, the stopper never runs;
   - timeout linearizes first → `TimeoutException`, the worker is interrupted,
     the stopper runs exactly once, `stopComplete` reaps the process;
   - success after the timeout linearized (the worker is parked in the stream
     closer because the fake keeps the streams open) → the success does not
     override the timeout: the caller still gets `TimeoutException`, enriched
     with the exit value and the worker stack dump. Both the presence of the
     exit value and of the stack-dump cause are deterministic because the
     worker cannot leave the closer before the script ends the streams.
2. Very small wall-clock timeouts against a process that blocks forever.
   Rejected: the outcome is stable but the *interleavings* are not — exactly
   the flakiness this package exists to remove.
3. Instrumenting `WaitForProcess` with test hooks. Rejected: changes the
   production code under test for the benefit of the test.

## Decision: the memory bound of the backpressure cases

Options considered:

1. **A byte budget on the fake pipe plus assertions on the observed in-flight
   maximum (chosen).** With the consumer blocked, the bytes in flight are
   bounded by `pipe budget + one pump buffer (1024)` for raw streams, plus one
   partial line for the `LogOutputStream` path. The pipe records its maximum
   in-flight count; the tests assert it equals the budget exactly (a stable
   result) and name the pipe and the offending position on failure (a stable
   error location). The captured-output buffer (`readOutput(true)`) cannot grow
   past what the pump delivered, which the full-stream equality assertion
   verifies.
2. Measuring heap usage around the scenario. Rejected: GC-dependent and
   unstable across JVMs.
3. An unbounded fake pipe with eyeball-checked scenarios. Rejected: without a
   budget there is no backpressure at all — the pipe absorbs everything and the
   blocked-consumer interleaving never happens.

## Directed mutations and negative tests

- **Success after timeout** (`TimeoutRaceTest.successAfterTimeout...`): the
  exit is real but linearizes after the timeout; the timeout stands and reports
  the exit value.
- **Undrained tail** (`OutputBackpressureTest.abruptExit...`): the pipe is
  reset at exit; exactly the one in-flight pump buffer survives, and the
  delivery check reports the loss with pipe name and first missing byte.
- **Duplicate stopper** (`TimeoutRaceTest.duplicateStopperMutation...`): a
  stopper that stops twice is exposed by the event log and the destroy-count
  assertion.
- **Harness self-tests** (`RaceHarnessSelfTest`): a lost write, a missing stop,
  an out-of-order expectation, an awaited event that never comes and a leaked
  thread are each detected and reported — the fixtures do not mask defects.

## Verification evidence

- `./gradlew assemble` — compiles, including the Java 8 main sources (the only
  production change is the `startProcess()` extraction; default semantics are
  unchanged and covered by the pre-existing suite).
- `./gradlew test` — 128 tests green, including the 19 tests of this package.
- The race package was re-run 9 consecutive times (`--rerun-tasks`): all green,
  ~1s per run, which is only possible because no test waits on wall-clock time.
- No temporary files are used; the thread-leak rule proves no pump, worker or
  driver thread survives a test.
