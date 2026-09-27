package org.zeroturnaround.exec.test.race;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * A controlled {@link Process} test double. The library threads (the WaitForProcess
 * worker and the stream pumps) interact with it exactly as with a real process,
 * while the test thread drives the lifecycle through script operations:
 * {@link #writeOut}, {@link #writeErr}, {@link #exit}, {@link #stopComplete} and
 * their variants. Every operation and every observation the library makes
 * (interrupt, destroy, stream close) is recorded in the {@link EventTracer}, so a
 * scenario is a fully determined sequence of events with no sleeps, no platform
 * shell and no real signals.
 *
 * <p>The trust boundary is a single instance: the double is injected through
 * {@code ProcessExecutor.startProcess()} and shares no state with other scenarios.
 * Failure branches (close failures, dropped writes, abrupt exit) are configured
 * per instance and surface through the existing {@link Process} and
 * {@link java.io.IOException} contracts, not through new exception types.</p>
 */
final class ScriptedProcess extends Process {

  /** The process' standard input as seen by the library. */
  static final class ScriptedStdin extends OutputStream {
    private final EventTracer tracer;
    private IOException closeFailure;
    private boolean closed;
    private long totalWritten;

    ScriptedStdin(EventTracer tracer) {
      this.tracer = tracer;
    }

    void failOnClose(IOException failure) {
      closeFailure = failure;
    }

    @Override
    public synchronized void write(int b) throws IOException {
      if (closed) {
        throw new IOException("scripted stdin: write after close");
      }
      totalWritten++;
    }

    /**
     * Closes the stream. A configured close failure is thrown on every close call
     * while configured, emulating a stream whose close-time flush always fails;
     * the event is recorded on the first close only.
     */
    @Override
    public synchronized void close() throws IOException {
      if (!closed) {
        closed = true;
        tracer.record(EventTracer.Op.CLOSE_STDIN);
      }
      if (closeFailure != null) {
        throw closeFailure;
      }
    }

    synchronized long totalWritten() {
      return totalWritten;
    }
  }

  private final String name;
  private final EventTracer tracer;
  private final ScriptedPipe out;
  private final ScriptedPipe err;
  private final ScriptedStdin stdin;
  private final CountDownLatch exitLatch = new CountDownLatch(1);
  private final CountDownLatch waitingLatch = new CountDownLatch(1);
  private final AtomicInteger destroyCount = new AtomicInteger();
  private volatile Integer exitValue;
  private volatile boolean mergeErrorIntoOut;

  ScriptedProcess(String name, EventTracer tracer) {
    this.name = name;
    this.tracer = tracer;
    this.out = new ScriptedPipe("stdout", tracer, EventTracer.Op.EOF_OUT);
    this.err = new ScriptedPipe("stderr", tracer, EventTracer.Op.EOF_ERR);
    this.stdin = new ScriptedStdin(tracer);
  }

  // --- script configuration -------------------------------------------------

  /** Emulates {@code redirectErrorStream(true)}: error writes land in the out pipe. */
  void setMergeErrorIntoOut(boolean mergeErrorIntoOut) {
    this.mergeErrorIntoOut = mergeErrorIntoOut;
  }

  ScriptedPipe out() {
    return out;
  }

  ScriptedPipe err() {
    return err;
  }

  ScriptedStdin stdin() {
    return stdin;
  }

  // --- script operations (test thread) --------------------------------------

  void writeOut(byte[] data) {
    tracer.record(EventTracer.Op.WRITE_OUT, data.length + " bytes");
    out.write(data);
  }

  void writeErr(byte[] data) {
    tracer.record(EventTracer.Op.WRITE_ERR, data.length + " bytes");
    if (mergeErrorIntoOut) {
      out.write(data);
    }
    else {
      err.write(data);
    }
  }

  /** The process exits and its streams end, like a normal OS process exit. */
  void exit(int code) {
    doExit(code, true, EventTracer.Op.EXIT, "");
  }

  /**
   * The process exits but its streams stay open (a real possibility with
   * inherited handles, e.g. detached grandchildren). The WaitForProcess worker
   * parks inside the stream closer until {@link #endStreams()} is scripted.
   */
  void exitKeepingStreamsOpen(int code) {
    doExit(code, false, EventTracer.Op.EXIT, "streams open");
  }

  /**
   * The stop requested through {@link #destroy()} takes effect: the process is
   * reaped with the given exit code and its streams end.
   */
  void stopComplete(int code) {
    if (destroyCount.get() == 0) {
      throw new IllegalStateException(name + ": stopComplete scripted without a preceding destroy");
    }
    doExit(code, true, EventTracer.Op.STOP_COMPLETE, "");
  }

  /**
   * Directed mutation: the process exits and its pipes are reset immediately,
   * discarding bytes the pumps have not drained yet.
   */
  void exitAbruptly(int code) {
    doExit(code, false, EventTracer.Op.EXIT, "abrupt");
    closePipeQuietly(out);
    closePipeQuietly(err);
  }

  /** Ends both streams without touching the exit state. */
  void endStreams() {
    out.eof();
    err.eof();
  }

  private void doExit(int code, boolean eof, EventTracer.Op op, String detail) {
    if (exitValue != null) {
      throw new IllegalStateException(name + ": already exited with " + exitValue);
    }
    tracer.record(op, ("exit=" + code + (detail.isEmpty() ? "" : ", " + detail)));
    // End the streams before releasing waiters, so the event order mirrors a real
    // exit (pipes are gone by the time waitFor returns) and stays deterministic.
    if (eof) {
      endStreams();
    }
    exitValue = code;
    exitLatch.countDown();
  }

  private static void closePipeQuietly(ScriptedPipe pipe) {
    try {
      pipe.close();
    }
    catch (IOException e) {
      throw new IllegalStateException("unexpected close failure in scripted abrupt exit", e);
    }
  }

  // --- observations ----------------------------------------------------------

  int destroyCount() {
    return destroyCount.get();
  }

  /**
   * Waits until the WaitForProcess worker is actually blocked in {@link #waitFor()},
   * so scripted timeouts and cancellations cannot fire before the task starts.
   */
  void awaitWaiting(Supplier<String> diagnostics) {
    try {
      if (!waitingLatch.await(RaceTestBase.AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        throw new AssertionError(name + ": the wait task never reached waitFor()\n" + diagnostics.get());
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(name + ": interrupted while waiting for the wait task\n" + diagnostics.get());
    }
  }

  String stateDump() {
    return "process " + name + "{exitValue=" + exitValue + ", destroyCount=" + destroyCount.get()
        + ", mergeErrorIntoOut=" + mergeErrorIntoOut + "}\n  " + out.stateDump()
        + "\n  " + err.stateDump();
  }

  // --- Process API (library threads) -----------------------------------------

  @Override
  public int waitFor() throws InterruptedException {
    waitingLatch.countDown();
    try {
      exitLatch.await();
    }
    catch (InterruptedException e) {
      tracer.record(EventTracer.Op.INTERRUPT, "waitFor interrupted");
      throw e;
    }
    return exitValue;
  }

  @Override
  public int exitValue() {
    Integer value = exitValue;
    if (value == null) {
      throw new IllegalThreadStateException("scripted process " + name + " has not exited");
    }
    return value;
  }

  @Override
  public void destroy() {
    int n = destroyCount.incrementAndGet();
    tracer.record(EventTracer.Op.DESTROY, "count=" + n);
  }

  @Override
  public boolean isAlive() {
    return exitValue == null;
  }

  @Override
  public OutputStream getOutputStream() {
    return stdin;
  }

  @Override
  public InputStream getInputStream() {
    return out;
  }

  @Override
  public InputStream getErrorStream() {
    return err;
  }

  @Override
  public String toString() {
    return name;
  }

}
