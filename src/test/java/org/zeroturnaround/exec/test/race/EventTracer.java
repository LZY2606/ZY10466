package org.zeroturnaround.exec.test.race;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Records the events of a scripted race scenario in a single total order. Every
 * event carries a sequence number, the operation, free-form detail and the name of
 * the thread that produced it, so a failure can be reported as the shortest event
 * prefix that diverges from the expectation instead of a wall of log lines.
 *
 * <p>There is no randomness and no wall-clock input: the order is fully determined
 * by the script and the synchronization points of the harness.</p>
 */
final class EventTracer {

  /** The operations a race script is built from. */
  enum Op {
    START,
    WRITE_OUT,
    WRITE_ERR,
    EXIT,
    TIMEOUT,
    INTERRUPT,
    STOP_COMPLETE,
    DESTROY,
    EOF_OUT,
    EOF_ERR,
    CLOSE_STDIN,
    WORKER_DONE,
    CONSUMER_BLOCK,
    CONSUMER_RELEASE
  }

  static final class Event {
    final long seq;
    final Op op;
    final String detail;
    final String thread;

    Event(long seq, Op op, String detail, String thread) {
      this.seq = seq;
      this.op = op;
      this.detail = detail;
      this.thread = thread;
    }

    @Override
    public String toString() {
      return "#" + seq + " " + op + (detail.isEmpty() ? "" : " (" + detail + ")") + " [" + thread + "]";
    }
  }

  private final List<Event> events = new ArrayList<Event>();
  private long seq;

  synchronized void record(Op op) {
    record(op, "");
  }

  synchronized void record(Op op, String detail) {
    events.add(new Event(++seq, op, detail, Thread.currentThread().getName()));
    notifyAll();
  }

  synchronized List<Event> snapshot() {
    return new ArrayList<Event>(events);
  }

  synchronized int count(Op op) {
    int result = 0;
    for (Event e : events) {
      if (e.op == op) {
        result++;
      }
    }
    return result;
  }

  /**
   * Waits until at least {@code n} events of {@code op} have been recorded. The
   * deadline is a hang guard only; all synchronization is event driven.
   */
  void awaitCount(Op op, int n, Supplier<String> diagnostics) {
    awaitCount(op, n, RaceTestBase.AWAIT_TIMEOUT_MS, diagnostics);
  }

  /**
   * Same as {@link #awaitCount(Op, int, Supplier)} with an explicit deadline, for
   * negative tests that expect the wait itself to fail.
   */
  void awaitCount(Op op, int n, long timeoutMs, Supplier<String> diagnostics) {
    long deadline = System.currentTimeMillis() + timeoutMs;
    synchronized (this) {
      while (count(op) < n) {
        long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0) {
          throw new AssertionError("timed out waiting for " + op + " x" + n
              + " (observed x" + count(op) + ")\n" + diagnostics.get());
        }
        try {
          wait(remaining);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError("interrupted while waiting for " + op + " x" + n + "\n" + diagnostics.get());
        }
      }
    }
  }

  String dump() {
    StringBuilder sb = new StringBuilder("events (").append(snapshot().size()).append("):");
    for (Event e : snapshot()) {
      sb.append("\n  ").append(e);
    }
    return sb.toString();
  }

}
