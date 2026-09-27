package org.zeroturnaround.exec.test.race;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.DESTROY;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.EXIT;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.INTERRUPT;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.START;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.TIMEOUT;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.WORKER_DONE;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;

/**
 * Negative tests for the harness itself: planted defects (a lost write, a missing
 * stop, a wrong expectation, a leaked thread) must be detected and reported as
 * the shortest diverging event sequence with a thread and resource snapshot —
 * never masked by the fixtures.
 */
public class RaceHarnessSelfTest extends RaceTestBase {

  /**
   * A write that vanishes inside the fake (a broken pipe) is caught by the
   * delivery check, which names the pipe and the first missing byte.
   */
  @Test
  public void droppedWriteIsDetectedWithLocation() throws Exception {
    ScriptedProcess process = newProcess("dropped-write");
    process.out().dropNextWrites(1); // planted defect: the next write is lost
    ProcessExecutor executor = executor(process).readOutput(true);
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));

    final byte[] data = utf8("lost-bytes");
    process.writeOut(data);
    process.exit(0);

    final ProcessResult result = driver.joinSuccess(diagnostics(process));
    assertEquals("the pipe reports the drop", data.length, process.out().totalDropped());
    AssertionError error = expectAssertion("full delivery", new Runnable() {
      public void run() {
        assertFullyDelivered("stdout", data, result.getOutput().getBytes());
      }
    });
    assertThat(error.getMessage(), containsString("stdout"));
    assertThat(error.getMessage(), containsString("diverges at byte 0"));
    assertThat(error.getMessage(), containsString("process wrote " + data.length + " bytes"));
  }

  /**
   * A stopper that never stops the process (here: the NOP stopper) leaves the
   * expected DESTROY event out; the sequence assertion reports the divergence at
   * exactly that step, with the process state showing zero destroys.
   */
  @Test
  public void missingStopIsReportedAsShortestDivergence() throws Exception {
    ScriptedProcess process = newProcess("missing-stop");
    ProcessExecutor executor = executor(process)
        .timeout(60, TimeUnit.SECONDS)
        .stopper(null); // NopProcessStopper: the stop never happens
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));
    process.awaitWaiting(diagnostics(process));

    gate.fireTimeout();
    tracer.awaitCount(INTERRUPT, 1, diagnostics(process));
    process.endStreams(); // let the worker leave the closer without a stop
    driver.joinFailure(TimeoutException.class, diagnostics(process));

    AssertionError error = expectAssertion("event sequence", new Runnable() {
      public void run() {
        assertSequence(process, START, TIMEOUT, INTERRUPT, DESTROY);
      }
    });
    assertThat(error.getMessage(), containsString("diverged at step 3"));
    assertThat(error.getMessage(), containsString("expected DESTROY next"));
    assertThat(error.getMessage(), containsString("destroyCount=0"));
  }

  /**
   * An expectation in the wrong order fails at the first impossible step and the
   * report shows the matched prefix, not the whole trace.
   */
  @Test
  public void outOfOrderExpectationShrinksToFirstDivergence() throws Exception {
    ScriptedProcess process = newProcess("out-of-order");
    ProcessExecutor executor = executor(process);
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));

    process.exit(0);
    driver.joinSuccess(diagnostics(process));

    AssertionError error = expectAssertion("event sequence", new Runnable() {
      public void run() {
        assertSequence(process, EXIT, START);
      }
    });
    assertThat(error.getMessage(), containsString("diverged at step 1"));
    assertThat(error.getMessage(), containsString("expected START next"));
    assertThat(error.getMessage(), containsString("matched prefix (1 events)"));
  }

  /**
   * A thread that outlives the scenario is reported by name and stack; once it
   * ends, the check passes again.
   */
  @Test
  public void leakedThreadIsDetected() throws Exception {
    final CountDownLatch release = new CountDownLatch(1);
    Thread leak = new Thread(new Runnable() {
      public void run() {
        try {
          release.await();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }, "zt-exec-selftest-leak");
    leak.setDaemon(true);
    Map<Long, Thread> before = raceThreads();
    leak.start();

    AssertionError error = expectAssertion("leak check", new Runnable() {
      public void run() {
        checkForLeakedThreads(before, 100);
      }
    });
    assertThat(error.getMessage(), containsString("zt-exec-selftest-leak"));

    release.countDown();
    leak.join(AWAIT_TIMEOUT_MS);
    checkForLeakedThreads(before, 100); // passes now
  }

  /**
   * Awaiting an event that never comes fails with the full state dump instead of
   * hanging: execute() without a timeout runs the wait task on the driver thread,
   * so no WORKER_DONE event exists.
   */
  @Test
  public void missingWorkerCompletionIsReported() throws Exception {
    ScriptedProcess process = newProcess("missing-worker-done");
    ProcessExecutor executor = executor(process);
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));

    process.exit(0);
    driver.joinSuccess(diagnostics(process));

    AssertionError error = expectAssertion("worker done", new Runnable() {
      public void run() {
        tracer.awaitCount(WORKER_DONE, 1, 200, diagnostics(process));
      }
    });
    assertThat(error.getMessage(), containsString("timed out waiting for WORKER_DONE"));
    assertThat(error.getMessage(), containsString("exitValue=0"));
  }

}
