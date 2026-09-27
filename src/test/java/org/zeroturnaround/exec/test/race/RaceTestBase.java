package org.zeroturnaround.exec.test.race;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.test.race.EventTracer.Op;

/**
 * Base of the deterministic race tests. Provides the scripted-process plumbing,
 * the failure diagnostics (shortest diverging event prefix plus a thread and
 * resource snapshot) and the rules that keep the scenarios honest:
 *
 * <ul>
 *   <li>a hang guard so a broken scenario fails instead of blocking the build;</li>
 *   <li>a leak check that fails the test if any pump, worker or driver thread is
 *   still alive afterwards, so no background work or temporary state survives.</li>
 * </ul>
 *
 * <p>There are no random sources and no sleeps: every wait is event driven and the
 * deadlines are hang guards only.</p>
 */
public abstract class RaceTestBase {

  /** Deadline for event-driven waits; a hang guard, never a timing assumption. */
  static final long AWAIT_TIMEOUT_MS = 30000;

  /** Grace period for threads that are already finished to actually disappear. */
  private static final long LEAK_GRACE_MS = 10000;

  private static final String[] RACE_THREAD_PREFIXES = { "zt-exec-", "WaitForProcess-", "scripted-" };

  protected final EventTracer tracer = new EventTracer();
  protected final TimeoutGate gate = new TimeoutGate(tracer);

  private Map<Long, Thread> threadsBefore;

  private final TestRule hangGuard = new Timeout(60, TimeUnit.SECONDS);

  private final TestRule threadLeakCheck = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      threadsBefore = raceThreads();
    }

    @Override
    protected void finished(Description description) {
      checkForLeakedThreads(threadsBefore, LEAK_GRACE_MS);
    }
  };

  @Rule
  public final TestRule rules = RuleChain.outerRule(hangGuard).around(threadLeakCheck);

  // --- scenario construction --------------------------------------------------

  protected ScriptedProcess newProcess(String name) {
    return new ScriptedProcess(name, tracer);
  }

  /** A process emulating {@code redirectErrorStream(true)}: error writes merge into stdout. */
  protected ScriptedProcess newMergedProcess(String name) {
    ScriptedProcess process = newProcess(name);
    process.setMergeErrorIntoOut(true);
    return process;
  }

  protected ScriptedExecutor executor(ScriptedProcess process) {
    return new ScriptedExecutor(process, tracer, gate);
  }

  protected Driver drive(ScriptedProcess process, final ProcessExecutor executor) {
    return Driver.start("scripted-driver-" + process.toString(), new Driver.Executing() {
      public ProcessResult run() throws Exception {
        return executor.execute();
      }
    });
  }

  // --- deterministic data ------------------------------------------------------

  /** Byte pattern derived from the position and a seed; no randomness involved. */
  protected static byte[] pattern(int length, int seed) {
    byte[] result = new byte[length];
    for (int i = 0; i < length; i++) {
      result[i] = (byte) ((i * 31 + seed * 17) % 251);
    }
    return result;
  }

  protected static byte[] slice(byte[] data, int offset, int length) {
    return Arrays.copyOfRange(data, offset, offset + length);
  }

  protected static byte[] utf8(String s) {
    try {
      return s.getBytes("UTF-8");
    }
    catch (java.io.UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  // --- pipe driving -------------------------------------------------------------

  /** Offers chunks until the pipe budget rejects one; returns the new offset. */
  protected int fillPipe(ScriptedPipe pipe, byte[] data, int offset, int chunk) {
    while (offset < data.length) {
      int length = Math.min(chunk, data.length - offset);
      if (!pipe.tryWrite(slice(data, offset, length))) {
        break;
      }
      offset += length;
    }
    return offset;
  }

  /** Writes the rest of the data, waiting for pipe space as the pump drains it. */
  protected int writeRemaining(ScriptedPipe pipe, byte[] data, int offset, int chunk, Supplier<String> diagnostics) {
    while (offset < data.length) {
      int length = Math.min(chunk, data.length - offset);
      pipe.awaitSpaceFor(length, diagnostics);
      pipe.write(slice(data, offset, length));
      offset += length;
    }
    return offset;
  }

  // --- assertions ---------------------------------------------------------------

  /**
   * Asserts the expected operations appear in order (other events may interleave).
   * On divergence the failure shows the shortest prefix that proves it: the matched
   * events, the expected next step and what actually followed, plus the thread and
   * resource snapshot.
   */
  protected void assertSequence(ScriptedProcess process, Op... expected) {
    List<EventTracer.Event> actual = tracer.snapshot();
    List<EventTracer.Event> matched = new ArrayList<EventTracer.Event>();
    int next = 0;
    for (int step = 0; step < expected.length; step++) {
      int found = -1;
      for (int i = next; i < actual.size(); i++) {
        if (actual.get(i).op == expected[step]) {
          found = i;
          break;
        }
      }
      if (found < 0) {
        StringBuilder message = new StringBuilder();
        message.append("event sequence diverged at step ").append(step)
            .append(": expected ").append(expected[step]).append(" next");
        message.append("\nmatched prefix (").append(matched.size()).append(" events):");
        for (EventTracer.Event e : matched) {
          message.append("\n  ").append(e);
        }
        message.append("\nactual events after the prefix:");
        int shown = 0;
        for (int i = next; i < actual.size() && shown < 5; i++, shown++) {
          message.append("\n  ").append(actual.get(i));
        }
        if (next >= actual.size()) {
          message.append("\n  (none — the trace ends here)");
        }
        message.append("\n").append(stateDump(process));
        throw new AssertionError(message.toString());
      }
      matched.add(actual.get(found));
      next = found + 1;
    }
  }

  protected void assertAbsent(ScriptedProcess process, Op... ops) {
    for (Op op : ops) {
      if (tracer.count(op) > 0) {
        throw new AssertionError("expected no " + op + " event but observed " + tracer.count(op)
            + "\n" + stateDump(process));
      }
    }
  }

  protected void assertDestroyCount(ScriptedProcess process, int expected) {
    int actual = process.destroyCount();
    if (actual != expected) {
      throw new AssertionError("process " + process + ": expected destroy() x" + expected
          + " but observed x" + actual + "\n" + stateDump(process));
    }
  }

  protected void assertPipeFull(ScriptedProcess process, ScriptedPipe pipe, int budget) {
    if (pipe.inFlight() != budget) {
      throw new AssertionError(pipe.pipeName() + ": expected exactly " + budget
          + " bytes in flight (the pipe budget) but observed " + pipe.inFlight()
          + "\n" + stateDump(process));
    }
  }

  /**
   * Compares what the process wrote with what the consumer captured; on divergence
   * the failure names the pipe and the first differing byte.
   */
  protected static void assertFullyDelivered(String pipeName, byte[] written, byte[] received) {
    if (!Arrays.equals(written, received)) {
      int divergence = 0;
      while (divergence < written.length && divergence < received.length
          && written[divergence] == received[divergence]) {
        divergence++;
      }
      throw new AssertionError(pipeName + ": captured output diverges at byte " + divergence
          + ": process wrote " + written.length + " bytes, consumer received "
          + received.length + " bytes");
    }
  }

  /** Runs a check expected to fail and returns the failure, for negative tests. */
  protected static AssertionError expectAssertion(String what, Runnable check) {
    try {
      check.run();
    }
    catch (AssertionError e) {
      return e;
    }
    throw new AssertionError("expected an AssertionError from " + what + " but the check passed");
  }

  // --- diagnostics ----------------------------------------------------------------

  protected Supplier<String> diagnostics(final ScriptedProcess process) {
    return new Supplier<String>() {
      public String get() {
        return stateDump(process);
      }
    };
  }

  protected String stateDump(ScriptedProcess process) {
    StringBuilder sb = new StringBuilder("--- state dump ---");
    if (process != null) {
      sb.append("\n").append(process.stateDump());
    }
    sb.append("\nrace threads:").append(dumpRaceThreads());
    sb.append("\n").append(tracer.dump());
    return sb.toString();
  }

  static String dumpRaceThreads() {
    StringBuilder sb = new StringBuilder();
    Map<Thread, StackTraceElement[]> all = Thread.getAllStackTraces();
    boolean any = false;
    for (Map.Entry<Thread, StackTraceElement[]> entry : all.entrySet()) {
      Thread thread = entry.getKey();
      if (!isRaceThread(thread.getName())) {
        continue;
      }
      any = true;
      sb.append("\n  ").append(thread.getName()).append(" [").append(thread.getState()).append("]");
      StackTraceElement[] stack = entry.getValue();
      for (int i = 0; i < Math.min(4, stack.length); i++) {
        sb.append("\n    at ").append(stack[i]);
      }
    }
    if (!any) {
      sb.append("\n  (none)");
    }
    return sb.toString();
  }

  // --- leak check -------------------------------------------------------------------

  static Map<Long, Thread> raceThreads() {
    Map<Long, Thread> result = new HashMap<Long, Thread>();
    for (Thread thread : Thread.getAllStackTraces().keySet()) {
      if (isRaceThread(thread.getName())) {
        result.put(thread.getId(), thread);
      }
    }
    return result;
  }

  private static boolean isRaceThread(String name) {
    for (String prefix : RACE_THREAD_PREFIXES) {
      if (name.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Fails if any race thread created since {@code before} is still alive. Threads
   * that are already done may take a moment to disappear, so there is a bounded
   * grace period; a thread that survives it is a real leak and is reported with
   * its stack.
   */
  static void checkForLeakedThreads(Map<Long, Thread> before, long graceMs) {
    long deadline = System.currentTimeMillis() + graceMs;
    List<Thread> leaked = aliveNewThreads(before);
    while (!leaked.isEmpty()) {
      long remaining = deadline - System.currentTimeMillis();
      if (remaining <= 0) {
        break;
      }
      try {
        leaked.get(0).join(Math.min(100, remaining));
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      leaked = aliveNewThreads(before);
    }
    if (!leaked.isEmpty()) {
      StringBuilder message = new StringBuilder("background threads survived the test:");
      for (Thread thread : leaked) {
        message.append("\n  ").append(thread.getName()).append(" [").append(thread.getState()).append("]");
        StackTraceElement[] stack = thread.getStackTrace();
        for (int i = 0; i < Math.min(4, stack.length); i++) {
          message.append("\n    at ").append(stack[i]);
        }
      }
      throw new AssertionError(message.toString());
    }
  }

  private static List<Thread> aliveNewThreads(Map<Long, Thread> before) {
    List<Thread> result = new ArrayList<Thread>();
    for (Map.Entry<Long, Thread> entry : raceThreads().entrySet()) {
      if (!before.containsKey(entry.getKey()) && entry.getValue().isAlive()) {
        result.add(entry.getValue());
      }
    }
    return result;
  }

}
