package org.zeroturnaround.exec.test.race;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Supplier;

/**
 * One direction of a scripted process pipe (stdout or stderr of the fake process).
 * The test thread offers chunks and an end-of-stream marker; the library's pump
 * thread drains them. An optional byte budget models a full OS pipe: once the
 * budget is in flight, further writes are rejected until the pump consumes, which
 * is what makes the backpressure scenarios deterministic.
 *
 * <p>All state is instance-local; nothing is shared between scenarios.</p>
 */
final class ScriptedPipe extends InputStream {

  private static final Object EOF = new Object();

  private final String name;
  private final EventTracer tracer;
  private final EventTracer.Op eofOp;
  private final LinkedBlockingDeque<Object> queue = new LinkedBlockingDeque<Object>();

  private int byteBudget;
  private byte[] current;
  private int pos;
  private boolean closed;
  private IOException closeFailure;
  private int dropNextWrites;

  private int inFlight;
  private int maxInFlight;
  private long totalWritten;
  private long totalRead;
  private long totalDropped;

  ScriptedPipe(String name, EventTracer tracer, EventTracer.Op eofOp) {
    this.name = name;
    this.tracer = tracer;
    this.eofOp = eofOp;
  }

  String pipeName() {
    return name;
  }

  /** Limits the bytes that may be in flight; {@code 0} means unbounded. */
  void setByteBudget(int byteBudget) {
    this.byteBudget = byteBudget;
  }

  /** Planted defect for negative tests: the next {@code n} writes vanish silently. */
  synchronized void dropNextWrites(int n) {
    dropNextWrites = n;
  }

  void failOnClose(IOException failure) {
    closeFailure = failure;
  }

  /**
   * Offers a chunk if it fits the budget. Returns {@code false} (and records
   * nothing) when the pipe is full, so the script can observe backpressure.
   */
  synchronized boolean tryWrite(byte[] data) {
    if (closed) {
      throw new IllegalStateException(name + ": scripted write after close");
    }
    if (dropNextWrites > 0) {
      dropNextWrites--;
      totalDropped += data.length;
      return true;
    }
    if (byteBudget > 0 && inFlight + data.length > byteBudget) {
      return false;
    }
    queue.offer(data);
    inFlight += data.length;
    totalWritten += data.length;
    if (inFlight > maxInFlight) {
      maxInFlight = inFlight;
    }
    return true;
  }

  void write(byte[] data) {
    if (!tryWrite(data)) {
      throw new IllegalStateException(name + ": scripted write of " + data.length
          + " bytes exceeds the byte budget " + byteBudget + " (in flight: " + inFlight + ")");
    }
  }

  /** Waits until a chunk of {@code length} bytes would fit the budget again. */
  synchronized void awaitSpaceFor(int length, Supplier<String> diagnostics) {
    long deadline = System.currentTimeMillis() + RaceTestBase.AWAIT_TIMEOUT_MS;
    while (byteBudget > 0 && inFlight + length > byteBudget) {
      long remaining = deadline - System.currentTimeMillis();
      if (remaining <= 0) {
        throw new AssertionError(name + ": no space for " + length + " bytes within the byte budget "
            + byteBudget + " (in flight: " + inFlight + ")\n" + diagnostics.get());
      }
      try {
        wait(remaining);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(name + ": interrupted while waiting for pipe space\n" + diagnostics.get());
      }
    }
  }

  /** Queues end-of-stream behind any undrained bytes, like a process exit does. */
  void eof() {
    queue.offer(EOF);
    tracer.record(eofOp);
  }

  @Override
  public int read() throws IOException {
    byte[] single = new byte[1];
    int n = read(single, 0, 1);
    return n < 0 ? -1 : single[0] & 0xff;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    while (current == null) {
      Object item;
      try {
        item = queue.take();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return -1;
      }
      if (item == EOF) {
        queue.offerFirst(EOF); // stay at end-of-stream
        return -1;
      }
      current = (byte[]) item;
      pos = 0;
    }
    int n = Math.min(len, current.length - pos);
    System.arraycopy(current, pos, b, off, n);
    pos += n;
    if (pos == current.length) {
      current = null;
    }
    synchronized (this) {
      inFlight -= n;
      totalRead += n;
      notifyAll();
    }
    return n;
  }

  /**
   * Closes the pipe, discarding undrained bytes like a closed OS pipe. Wakes a
   * blocked reader with end-of-stream. A configured close failure is thrown after
   * the state change, mirroring real streams that fail while closing.
   */
  @Override
  public void close() throws IOException {
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      if (current != null) {
        // the reader holds a partially consumed chunk; its remainder is lost too
        totalDropped += current.length - pos;
        current = null;
      }
      Object item;
      while ((item = queue.poll()) != null) {
        if (item != EOF) {
          byte[] chunk = (byte[]) item;
          totalDropped += chunk.length;
        }
      }
      inFlight = 0;
      notifyAll();
    }
    queue.offerFirst(EOF);
    if (closeFailure != null) {
      throw closeFailure;
    }
  }

  synchronized int inFlight() {
    return inFlight;
  }

  synchronized int maxInFlight() {
    return maxInFlight;
  }

  synchronized long totalWritten() {
    return totalWritten;
  }

  synchronized long totalRead() {
    return totalRead;
  }

  synchronized long totalDropped() {
    return totalDropped;
  }

  synchronized String stateDump() {
    return name + "{inFlight=" + inFlight + ", maxInFlight=" + maxInFlight
        + ", written=" + totalWritten + ", read=" + totalRead + ", dropped=" + totalDropped
        + ", closed=" + closed + "}";
  }

}
