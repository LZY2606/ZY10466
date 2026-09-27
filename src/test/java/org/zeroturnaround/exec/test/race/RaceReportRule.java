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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * JUnit rule that turns a failing race test into a deterministic report: the recorded
 * event sequence (already the shortest one, since every event is test-driven), the
 * state of the scripted process' resources, and the stacks of any zt-exec worker
 * threads still alive. After a passing test it also verifies that no background work
 * outlives the test.
 */
public class RaceReportRule implements TestRule {

  /**
   * How long the quiescence check waits for daemon worker threads to die after a test.
   * Threads joined by the production code are already gone; this only absorbs the
   * asynchronous death of interrupted executor threads.
   */
  private static final long QUIESCENCE_SAFEGUARD_MILLIS = 10000;

  private final EventLog log;
  private final ScriptedProcess process;
  private final long quiescenceSafeguardMillis;

  public RaceReportRule(EventLog log, ScriptedProcess process) {
    this(log, process, QUIESCENCE_SAFEGUARD_MILLIS);
  }

  /**
   * @param quiescenceSafeguardMillis how long to wait for leftover worker threads;
   *        tests that verify the leak check itself pass a small value.
   */
  public RaceReportRule(EventLog log, ScriptedProcess process, long quiescenceSafeguardMillis) {
    this.log = log;
    this.process = process;
    this.quiescenceSafeguardMillis = quiescenceSafeguardMillis;
  }

  @Override
  public Statement apply(final Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          base.evaluate();
        }
        catch (Throwable t) {
          throw report(t);
        }
        assertQuiescent();
      }
    };
  }

  private Throwable report(Throwable t) {
    StringBuilder sb = new StringBuilder();
    sb.append(t.getMessage() == null ? "" : t.getMessage()).append('\n');
    sb.append("---- scripted event sequence (shortest failing prefix) ----\n");
    sb.append(log.dump());
    sb.append("---- scripted process resource state ----\n");
    sb.append(process.stateDump());
    sb.append("---- live zt-exec worker threads ----\n");
    sb.append(workerThreadDump());
    AssertionError error = new AssertionError(sb.toString());
    error.setStackTrace(t.getStackTrace());
    error.initCause(t);
    return error;
  }

  /**
   * Fails when a pump or wait thread started by the test is still alive afterwards,
   * so no background work leaks between tests.
   */
  private void assertQuiescent() {
    long deadline = System.currentTimeMillis() + quiescenceSafeguardMillis;
    List<String> alive;
    while (!(alive = liveWorkerThreads()).isEmpty()) {
      if (System.currentTimeMillis() >= deadline) {
        throw new AssertionError("Background work survived the test: " + alive
            + "\n" + workerThreadDump() + log.dump());
      }
      Thread.yield();
    }
  }

  private static List<String> liveWorkerThreads() {
    List<String> result = new ArrayList<String>();
    for (Thread thread : Thread.getAllStackTraces().keySet()) {
      String name = thread.getName();
      if ((name.startsWith("zt-exec-") || name.startsWith("WaitForProcess-")
          || name.startsWith("zt-exec-test-")) && thread.isAlive()) {
        result.add(name + "(" + thread.getState() + ")");
      }
    }
    return result;
  }

  private static String workerThreadDump() {
    StringBuilder sb = new StringBuilder();
    ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    boolean any = false;
    for (ThreadInfo info : threads.dumpAllThreads(false, false)) {
      String name = info.getThreadName();
      if (name.startsWith("zt-exec-") || name.startsWith("WaitForProcess-")
          || name.startsWith("zt-exec-test-")) {
        any = true;
        sb.append("  \"").append(name).append("\" ").append(info.getThreadState()).append('\n');
        StackTraceElement[] stack = info.getStackTrace();
        for (int i = 0; i < Math.min(stack.length, 8); i++) {
          sb.append("    at ").append(stack[i]).append('\n');
        }
      }
    }
    if (!any) {
      sb.append("  (none)\n");
    }
    return sb.toString();
  }

}
