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

import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.StartedProcess;

/**
 * The {@code start()} path: the returned promise, the exit value and the captured
 * output are compared against the scripted event sequence, and cancelling the promise
 * drives the (default destroy) stopper chain deterministically.
 */
public class PromiseAndStopperTest {

  private final EventLog log = new EventLog();
  private final ScriptedProcess process = new ScriptedProcess(log);

  @Rule
  public RaceReportRule report = new RaceReportRule(log, process);

  @Test
  public void promiseCompletesWithExitValueAndCapturedOutput() throws Exception {
    ScriptedProcessExecutor executor = new ScriptedProcessExecutor(log, process);
    executor.readOutput(true).exitValueAny();

    StartedProcess started = executor.start();
    log.await("waitFor");
    Future<ProcessResult> promise = started.getFuture();
    Assert.assertFalse("promise must be pending while the process runs", promise.isDone());

    process.writeOut("async-output");
    process.exit(7);
    log.await("stopComplete");

    Assert.assertTrue("promise must be done after stopComplete", promise.isDone());
    ProcessResult result = promise.get();
    Assert.assertEquals(7, result.getExitValue());
    Assert.assertEquals("async-output", result.outputUTF8());
    Assert.assertSame("the promise exposes the scripted process", process, started.getProcess());
  }

  /**
   * Cancelling the promise interrupts the worker, which routes through the stopper
   * exactly once.
   */
  @Test
  public void cancellingPromiseInvokesStopperOnce() throws Exception {
    ScriptedProcessExecutor executor = new ScriptedProcessExecutor(log, process);

    StartedProcess started = executor.start();
    log.await("waitFor");

    Assert.assertTrue(started.getFuture().cancel(true));
    log.await("stopComplete");

    Assert.assertTrue(started.getFuture().isCancelled());
    Assert.assertEquals(1, process.getDestroyCount());
    Assert.assertTrue("interrupt not observed", log.contains("interrupt"));
  }

  /**
   * A second cancel does not re-enter the stopper chain: the promise was already
   * cancelled, so nothing new can race the first stop.
   */
  @Test
  public void duplicateCancelDoesNotDuplicateStop() throws Exception {
    ScriptedProcessExecutor executor = new ScriptedProcessExecutor(log, process);

    StartedProcess started = executor.start();
    log.await("waitFor");

    Assert.assertTrue(started.getFuture().cancel(true));
    log.await("stopComplete");
    Assert.assertFalse("second cancel must be a no-op", started.getFuture().cancel(true));
    Assert.assertEquals(1, process.getDestroyCount());
  }

}
