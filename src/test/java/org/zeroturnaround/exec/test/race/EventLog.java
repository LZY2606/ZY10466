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

import java.util.ArrayList;
import java.util.List;

/**
 * Ordered, thread-safe log of the events a deterministic race test drives through the
 * executor ({@code start}, {@code writeOut}, {@code writeErr}, {@code exit},
 * {@code timeout}, {@code interrupt}, {@code stopComplete}, ...). Every entry carries
 * the name of the thread that produced it, so a failure can be shrunk to the shortest
 * event sequence without any wall-clock timestamps.
 * <p>
 * All state lives in the instance; nothing is shared between tests through statics.
 */
public class EventLog {

  /**
   * Safeguard used by {@link #await(String)}: a waiting test fails instead of hanging
   * when the expected event never arrives. This is an upper bound for a broken run,
   * never part of the passing path.
   */
  public static final long AWAIT_SAFEGUARD_MILLIS = 30000;

  private final long awaitSafeguardMillis;

  /**
   * One recorded event.
   */
  public static final class Event {

    private final int sequence;
    private final String thread;
    private final String op;
    private final String detail;

    Event(int sequence, String thread, String op, String detail) {
      this.sequence = sequence;
      this.thread = thread;
      this.op = op;
      this.detail = detail;
    }

    public String getOp() {
      return op;
    }

    public String getDetail() {
      return detail;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append('#').append(sequence).append(' ');
      sb.append('[').append(thread).append("] ");
      sb.append(op);
      if (detail != null && !detail.isEmpty()) {
        sb.append(' ').append(detail);
      }
      return sb.toString();
    }

  }

  private final List<Event> events = new ArrayList<Event>();

  public EventLog() {
    this(AWAIT_SAFEGUARD_MILLIS);
  }

  /**
   * @param awaitSafeguardMillis how long {@link #await(String)} waits before failing;
   *        tests that verify the failure path itself pass a small value.
   */
  public EventLog(long awaitSafeguardMillis) {
    this.awaitSafeguardMillis = awaitSafeguardMillis;
  }

  /**
   * Records an event produced by the calling thread.
   *
   * @param op the operation name, e.g. {@code writeOut}.
   * @param detail optional detail, e.g. the bytes written (maybe <code>null</code>).
   */
  public void record(String op, String detail) {
    synchronized (events) {
      events.add(new Event(events.size(), Thread.currentThread().getName(), op, detail));
      events.notifyAll();
    }
  }

  public void record(String op) {
    record(op, null);
  }

  /**
   * @return how many events with the given operation have been recorded so far.
   */
  public int count(String op) {
    synchronized (events) {
      int result = 0;
      for (Event event : events) {
        if (event.getOp().equals(op)) {
          result++;
        }
      }
      return result;
    }
  }

  /**
   * @return <code>true</code> once at least one event with the given operation exists.
   */
  public boolean contains(String op) {
    return count(op) > 0;
  }

  /**
   * Blocks until an event with the given operation has been recorded. Fails the test
   * with the full event sequence when the event does not arrive within
   * {@link #AWAIT_SAFEGUARD_MILLIS}.
   *
   * @param op the operation to wait for.
   */
  public void await(String op) {
    long deadline = System.currentTimeMillis() + awaitSafeguardMillis;
    synchronized (events) {
      while (count(op) == 0) {
        long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0) {
          throw new AssertionError("Timed out waiting for event '" + op + "'."
              + " Events so far (shortest failing sequence):\n" + dump());
        }
        try {
          events.wait(remaining);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError("Interrupted waiting for event '" + op + "'.\n" + dump());
        }
      }
    }
  }

  /**
   * @return the recorded events, one per line, in the order they happened.
   */
  public String dump() {
    synchronized (events) {
      StringBuilder sb = new StringBuilder();
      for (Event event : events) {
        sb.append("  ").append(event).append('\n');
      }
      return sb.toString();
    }
  }

  @Override
  public String toString() {
    return dump();
  }

}
