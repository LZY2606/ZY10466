package org.zeroturnaround.exec.test.race;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.DESTROY;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.EOF_OUT;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.EXIT;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.INTERRUPT;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.START;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.STOP_COMPLETE;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.TIMEOUT;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.WORKER_DONE;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.WRITE_OUT;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.StartedProcess;
import org.zeroturnaround.exec.stop.ProcessStopper;

/**
 * Pins down the linearization points between process exit, execution timeout and
 * cancellation. The timeout fires exactly where the script places it (see
 * {@link TimeoutGate}), so each test covers one deterministic interleaving that
 * wall-clock stress tests can only hit by chance.
 */
public class TimeoutRaceTest extends RaceTestBase {

  /**
   * The process exits before the scripted timeout: the result is delivered and
   * the stopper is never engaged.
   */
  @Test
  public void exitBeforeTimeoutSucceeds() throws Exception {
    ScriptedProcess process = newProcess("exit-before-timeout");
    ProcessExecutor executor = executor(process)
        .timeout(60, TimeUnit.SECONDS)
        .readOutput(true)
        .exitValueNormal();
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));

    process.writeOut(utf8("hello\n"));
    process.exit(0);

    ProcessResult result = driver.joinSuccess(diagnostics(process));
    assertEquals(0, result.getExitValue());
    assertEquals("hello\n", result.outputUTF8());
    assertDestroyCount(process, 0);
    assertSequence(process, START, WRITE_OUT, EXIT, EOF_OUT, WORKER_DONE);
    assertAbsent(process, TIMEOUT, INTERRUPT, DESTROY);
  }

  /**
   * The scripted timeout fires while the process is still running: the caller gets
   * a TimeoutException without an exit value, the worker is interrupted and the
   * stopper runs exactly once before the scripted stop completes.
   */
  @Test
  public void timeoutBeforeExitStopsProcessOnce() throws Exception {
    ScriptedProcess process = newProcess("timeout-before-exit");
    ScriptedExecutor executor = executor(process);
    executor.timeout(60, TimeUnit.SECONDS).readOutput(true);
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));
    process.awaitWaiting(diagnostics(process));

    gate.fireTimeout();
    tracer.awaitCount(INTERRUPT, 1, diagnostics(process));
    tracer.awaitCount(DESTROY, 1, diagnostics(process));
    process.stopComplete(143);

    TimeoutException e = driver.joinFailure(TimeoutException.class, diagnostics(process));
    assertThat(e.getMessage(), containsString("Timed out waiting for"));
    assertThat(e.getMessage(), containsString("timeout-before-exit"));
    assertNull("no exit value known, so no worker stack dump expected", e.getCause());
    executor.awaitWorkerDone(diagnostics(process));
    assertDestroyCount(process, 1);
    assertSequence(process, START, TIMEOUT, INTERRUPT, DESTROY, STOP_COMPLETE, EOF_OUT, WORKER_DONE);
  }

  /**
   * Directed mutation "success after timeout": the process exits successfully but
   * the timeout linearizes first (the worker is still parked in the stream
   * closer). The success must not silently override the timeout: the caller still
   * gets a TimeoutException, now enriched with the exit value and the worker
   * stack dump.
   */
  @Test
  public void successAfterTimeoutKeepsTimeoutAndReportsExitValue() throws Exception {
    ScriptedProcess process = newProcess("success-after-timeout");
    ScriptedExecutor executor = executor(process);
    executor.timeout(60, TimeUnit.SECONDS);
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));
    process.awaitWaiting(diagnostics(process));

    // The exit is real (waitFor returns 0) but the streams stay open, so the
    // worker cannot leave the closer before the script ends them.
    process.exitKeepingStreamsOpen(0);
    gate.fireTimeout();

    TimeoutException e = driver.joinFailure(TimeoutException.class, diagnostics(process));
    assertThat(e.getMessage(), containsString("Timed out finishing"));
    assertThat(e.getMessage(), containsString("exit value: 0"));
    assertNotNull("worker is parked in the closer, so its stack dump is attached", e.getCause());
    assertThat(e.getCause().getMessage(), containsString("Stack dump of worker thread"));
    assertDestroyCount(process, 0);

    process.endStreams();
    executor.awaitWorkerDone(diagnostics(process));
    assertSequence(process, START, EXIT, TIMEOUT, WORKER_DONE);
    assertAbsent(process, DESTROY);
  }

  /**
   * Cancellation before the exit: the worker is interrupted, the stopper runs
   * once and the future reports cancellation.
   */
  @Test
  public void cancelBeforeExitStopsProcessOnce() throws Exception {
    ScriptedProcess process = newProcess("cancel-before-exit");
    ScriptedExecutor executor = executor(process);
    StartedProcess started = executor.start();
    tracer.awaitCount(START, 1, diagnostics(process));
    process.awaitWaiting(diagnostics(process));

    assertTrue(started.getFuture().cancel(true));
    tracer.awaitCount(INTERRUPT, 1, diagnostics(process));
    tracer.awaitCount(DESTROY, 1, diagnostics(process));
    process.stopComplete(1);

    try {
      started.getFuture().get();
      fail("CancellationException expected");
    }
    catch (CancellationException expected) {
      // the future was cancelled before the process exited
    }
    executor.awaitWorkerDone(diagnostics(process));
    assertDestroyCount(process, 1);
    assertSequence(process, START, INTERRUPT, DESTROY, STOP_COMPLETE, WORKER_DONE);
  }

  /**
   * Cancellation after the exit: the result is already materialized, the cancel
   * comes too late and the stopper stays unused.
   */
  @Test
  public void exitBeforeCancelKeepsResult() throws Exception {
    ScriptedProcess process = newProcess("exit-before-cancel");
    ScriptedExecutor executor = executor(process);
    StartedProcess started = executor.start();
    tracer.awaitCount(START, 1, diagnostics(process));

    process.exit(7);
    executor.awaitWorkerDone(diagnostics(process));

    assertFalse("the wait task is already done", started.getFuture().cancel(true));
    assertEquals(7, started.getFuture().get().getExitValue());
    assertDestroyCount(process, 0);
    assertSequence(process, START, EXIT, EOF_OUT, WORKER_DONE);
    assertAbsent(process, INTERRUPT, DESTROY);
  }

  /**
   * Directed mutation "duplicate stopper": a stopper that stops the process twice
   * is a defect, and the harness must expose it — both stops appear in the event
   * log and the destroy-count assertion pinpoints the duplicate.
   */
  @Test
  public void duplicateStopperMutationIsExposed() throws Exception {
    final ScriptedProcess process = newProcess("duplicate-stopper");
    ProcessExecutor executor = executor(process)
        .timeout(60, TimeUnit.SECONDS)
        .stopper(new ProcessStopper() {
          public void stop(Process target) {
            target.destroy();
            target.destroy(); // planted defect: non-idempotent stopper
          }
        });
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));
    process.awaitWaiting(diagnostics(process));

    gate.fireTimeout();
    tracer.awaitCount(DESTROY, 2, diagnostics(process));
    process.stopComplete(143);
    driver.joinFailure(TimeoutException.class, diagnostics(process));

    AssertionError error = expectAssertion("destroy count", new Runnable() {
      public void run() {
        assertDestroyCount(process, 1);
      }
    });
    assertThat(error.getMessage(), containsString("destroy() x1"));
    assertThat(error.getMessage(), containsString("observed x2"));
  }

}
