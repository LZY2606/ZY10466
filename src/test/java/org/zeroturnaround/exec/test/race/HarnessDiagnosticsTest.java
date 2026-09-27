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

import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Negative tests for the harness itself: the failure diagnostics and the leak check
 * must fire when a run goes wrong, with the event sequence, the resource state and
 * the thread state in the report. These run without any executor involvement.
 */
public class HarnessDiagnosticsTest {

  @Test
  public void awaitingAnUnscriptedEventFailsWithTheEventSequence() {
    EventLog log = new EventLog(50);
    log.record("start");
    log.record("writeOut", "3 bytes");
    try {
      log.await("exit");
      Assert.fail("await must fail for an event that was never scripted");
    }
    catch (AssertionError e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("'exit'"));
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("start"));
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("writeOut"));
    }
  }

  @Test
  public void failureReportContainsEventsProcessStateAndThreads() throws Throwable {
    EventLog log = new EventLog();
    ScriptedProcess process = new ScriptedProcess(log);
    process.writeOut("report-me");
    RaceReportRule rule = new RaceReportRule(log, process);
    Statement failing = new Statement() {
      @Override
      public void evaluate() {
        throw new AssertionError("boom-location");
      }
    };
    try {
      rule.apply(failing, Description.EMPTY).evaluate();
      Assert.fail("the rule must rethrow the failure as a report");
    }
    catch (AssertionError report) {
      String text = report.getMessage();
      Assert.assertTrue(text, text.contains("boom-location"));
      Assert.assertTrue(text, text.contains("scripted event sequence"));
      Assert.assertTrue(text, text.contains("writeOut"));
      Assert.assertTrue(text, text.contains("resource state"));
      Assert.assertTrue(text, text.contains("worker threads"));
      Assert.assertTrue(text, text.contains("stdout("));
    }
  }

  @Test
  public void leftoverWorkerThreadIsDetected() throws Throwable {
    EventLog log = new EventLog();
    ScriptedProcess process = new ScriptedProcess(log);
    RaceReportRule rule = new RaceReportRule(log, process, 100);
    final CountDownLatch stop = new CountDownLatch(1);
    Thread leak = new Thread(() -> {
      try {
        stop.await();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }, "zt-exec-test-leak");
    leak.setDaemon(true);
    leak.start();
    try {
      rule.apply(new Statement() {
        @Override
        public void evaluate() {
          // The test body itself passes; the leaked thread must still be caught.
        }
      }, Description.EMPTY).evaluate();
      Assert.fail("the rule must detect the leaked worker thread");
    }
    catch (AssertionError report) {
      Assert.assertTrue(report.getMessage(), report.getMessage().contains("zt-exec-test-leak"));
    }
    finally {
      stop.countDown();
      leak.join(5000);
    }
  }

  /**
   * The executor's timeout message probes {@link Process#exitValue()} and relies on
   * {@link IllegalThreadStateException} for a running process; the fake must honor
   * that contract instead of leaking hidden state.
   */
  @Test
  public void exitValueOfRunningProcessThrowsLikeARealProcess() {
    ScriptedProcess process = new ScriptedProcess(new EventLog());
    try {
      process.exitValue();
      Assert.fail("exitValue must reject a running process");
    }
    catch (IllegalThreadStateException expected) {
      // the contract java.lang.Process defines and ProcessExecutor relies on
    }
    process.exit(5);
    Assert.assertEquals(5, process.exitValue());
  }

}
