/*
 * Copyright (C) 2026 Neeme Praks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zeroturnaround.exec.test.race;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.zeroturnaround.exec.InvalidExitValueException;
import org.zeroturnaround.exec.stop.DestroyProcessStopper;
import org.zeroturnaround.exec.stop.ProcessStopper;

/**
 * Deterministic races around the timeout linearization point: the test decides the
 * order of {@code timeout} versus {@code exit} and asserts the stable outcome of each
 * order. No sleeps, no wall-clock deadlines, no real processes.
 */
public class TimeoutLinearizationTest {

  private final EventLog log = new EventLog();
  private final ScriptedProcess process = new ScriptedProcess(log);
  private ScriptedProcessExecutor executor;
  private RecordingStopper stopper;

  @Rule
  public RaceReportRule report = new RaceReportRule(log, process);

  /** Records every stopper invocation; the stopper chain stays observable. */
  private static final class RecordingStopper implements ProcessStopper {

    private final EventLog log;

    RecordingStopper(EventLog log) {
      this.log = log;
    }

    @Override
    public void stop(Process p) {
      log.record("stopper");
      DestroyProcessStopper.INSTANCE.stop(p);
    }

  }

  @Before
  public void setUp() {
    stopper = new RecordingStopper(log);
    executor = new ScriptedProcessExecutor(log, process);
    executor.stopper(stopper).timeout(2, TimeUnit.MINUTES).readOutput(true);
  }

  private Driver executeAsync() {
    return new Driver(log, "zt-exec-test-driver", () -> executor.execute()).start();
  }

  @Test
  public void timeoutLinearizedWhileProcessRuns() throws Exception {
    process.writeOut("partial-output");
    Driver driver = executeAsync();
    log.await("waitFor");

    executor.fireTimeout();

    Driver.Outcome outcome = driver.awaitDone();
    Assert.assertFalse("expected a timeout, got " + outcome, outcome.isSuccess());
    Assert.assertTrue("expected TimeoutException, got " + outcome,
        outcome.getFailure() instanceof TimeoutException);
    String message = outcome.getFailure().getMessage();
    Assert.assertTrue("message lacks the timeout: " + message, message.contains("2 minutes"));
    Assert.assertTrue("message lacks the command: " + message,
        message.contains("zt-exec-scripted-command"));
    Assert.assertTrue("message lacks the output read so far: " + message,
        message.contains("partial-output"));
    // The stopper chain ran exactly once and the worker left completely.
    log.await("stopComplete");
    Assert.assertEquals(1, log.count("stopper"));
    Assert.assertEquals(1, process.getDestroyCount());
    Assert.assertTrue("interrupt not observed", log.contains("interrupt"));
  }

  /**
   * Directed mutation: the process exits successfully right after the timeout
   * linearized. The late success must not override the TimeoutException.
   */
  @Test
  public void successAfterTimeoutDoesNotOverride() throws Exception {
    Driver driver = executeAsync();
    log.await("waitFor");

    executor.fireTimeout();
    Driver.Outcome outcome = driver.awaitDone();
    Assert.assertTrue(outcome.getFailure() instanceof TimeoutException);
    log.await("stopComplete");

    // The process only now manages to exit, successfully.
    process.exit(0);
    Assert.assertEquals(0, process.exitValue());
    Assert.assertFalse("late success changed the outcome", outcome.isSuccess());
    Assert.assertEquals(1, process.getDestroyCount());
  }

  /**
   * Directed mutation, reverse order: the completion linearizes before the timeout, so
   * firing the timeout afterwards cannot fail the execute call any more.
   */
  @Test
  public void successBeforeTimeoutLinearizationWins() throws Exception {
    process.writeOut("done-output");
    Driver driver = executeAsync();
    log.await("waitFor");

    process.exit(0);
    executor.awaitWorkerCompletion();
    executor.fireTimeout();

    Driver.Outcome outcome = driver.awaitDone();
    Assert.assertTrue("expected success, got " + outcome, outcome.isSuccess());
    Assert.assertEquals(0, outcome.getResult().getExitValue());
    Assert.assertEquals("done-output", outcome.getResult().outputUTF8());
    Assert.assertEquals("stopper must not run for a finished process", 0, log.count("stopper"));
  }

  /**
   * Interrupting the waiting thread stops the process through the same stopper chain
   * as a timeout.
   */
  @Test
  public void interruptStopsProcessThroughStopper() throws Exception {
    Driver driver = executeAsync();
    log.await("waitFor");

    driver.getThread().interrupt();

    Driver.Outcome outcome = driver.awaitDone();
    Assert.assertTrue("expected InterruptedException, got " + outcome,
        outcome.getFailure() instanceof InterruptedException);
    log.await("stopComplete");
    Assert.assertEquals(1, log.count("stopper"));
    Assert.assertEquals(1, process.getDestroyCount());
  }

  /**
   * Directed mutation: a process that ignores the interrupt. The timeout outcome is
   * delivered anyway; the parked worker only unwinds when the process later exits,
   * without any stopper call.
   */
  @Test
  public void processIgnoringInterruptYieldsTimeoutAndWorkerUnwindsOnExit() throws Exception {
    process.setIgnoreInterrupts(true);
    Driver driver = executeAsync();
    log.await("waitFor");

    executor.fireTimeout();

    Driver.Outcome outcome = driver.awaitDone();
    Assert.assertTrue(outcome.getFailure() instanceof TimeoutException);
    Assert.assertEquals(0, log.count("stopper"));
    // The worker is still parked in waitFor; let the process exit so nothing leaks.
    process.exit(0);
    log.await("stopComplete");
  }

  /**
   * Directed mutation: a failing exit-value check survives the executor's exception
   * unwrapping with its type, message and result intact.
   */
  @Test
  public void invalidExitValueExceptionSurvivesUnwrapping() throws Exception {
    executor.exitValue(0);
    process.writeOut("failing-output");
    Driver driver = executeAsync();
    log.await("waitFor");

    process.exit(3);
    executor.awaitWorkerCompletion();
    executor.fireTimeout();

    Driver.Outcome outcome = driver.awaitDone();
    Assert.assertTrue("expected InvalidExitValueException, got " + outcome,
        outcome.getFailure() instanceof InvalidExitValueException);
    InvalidExitValueException failure = (InvalidExitValueException) outcome.getFailure();
    Assert.assertTrue(failure.getMessage().contains("Unexpected exit value: 3"));
    Assert.assertEquals(3, failure.getResult().getExitValue());
    Assert.assertEquals("failing-output", failure.getResult().outputUTF8());
    Assert.assertEquals(0, log.count("stopper"));
  }

  /**
   * Directed mutation: the stopper chain is invoked twice (e.g. a cancel racing a
   * shutdown hook). Both invocations stay visible and the process state stays
   * consistent.
   */
  @Test
  public void duplicateStopperInvocationsStayVisible() throws Exception {
    Driver driver = executeAsync();
    log.await("waitFor");

    executor.fireTimeout();
    driver.awaitDone();
    log.await("stopComplete");
    Assert.assertEquals(1, log.count("stopper"));

    // A second stop fires, as happens when two shutdown mechanisms overlap.
    stopper.stop(process);

    Assert.assertEquals(2, log.count("stopper"));
    Assert.assertEquals(2, process.getDestroyCount());
    process.exit(0);
    Assert.assertEquals(0, process.exitValue());
  }

}
