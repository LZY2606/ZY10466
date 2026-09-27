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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A controlled {@link Process} substitute for deterministic race tests. Nothing happens
 * unless the test scripts it: output is pushed with {@link #writeOut(byte[])} /
 * {@link #writeErr(byte[])}, the process exits with {@link #exit(int)}, and
 * {@link #waitFor()} only reacts to an exit or to the worker thread being interrupted.
 * <p>
 * The trust boundary is the standard <code>java.lang.Process</code> interface: every
 * observation the executor can make (stream reads, exit value, destroy, interrupt
 * response) is recorded in the {@link EventLog} and all state is kept in this instance,
 * so tests never depend on hidden global state.
 */
public class ScriptedProcess extends Process {

  /**
   * Safeguard for internal blocking waits; an upper bound for a broken run, never part
   * of the passing path.
   */
  private static final long SAFEGUARD_MILLIS = EventLog.AWAIT_SAFEGUARD_MILLIS;

  /**
   * Input stream the test feeds byte by byte or chunk by chunk. Reads block until the
   * test pushes more data, closes the stream, or exits the process. After
   * {@link ScriptedProcess#exit(int)} the pushed-but-unread bytes are still drained
   * before EOF, matching how an OS pipe keeps the tail of a dead process readable.
   */
  public final class ScriptedInputStream extends InputStream {

    private final String name;
    private final EventLog log;

    private final Object lock = new Object();

    /** Bytes pushed by the test but not yet consumed by a pump. */
    private final byte[] pending = new byte[1024 * 1024];
    private int pendingStart;
    private int pendingEnd;

    /** Total bytes consumed by readers; the basis of the backpressure memory bound. */
    private int totalRead;

    /** At most this many bytes are returned per read call; 1 simulates short writes. */
    private int maxChunk = Integer.MAX_VALUE;

    /** Exception thrown by {@link #close()}, or <code>null</code> for a clean close. */
    private IOException closeException;

    private boolean closed;

    ScriptedInputStream(String name, EventLog log) {
      this.name = name;
      this.log = log;
    }

    /**
     * Limits how many bytes a single read returns, to simulate a process whose output
     * arrives in short writes.
     */
    public void setMaxChunk(int maxChunk) {
      this.maxChunk = maxChunk;
    }

    /**
     * Makes {@link #close()} fail with the given exception, to exercise the error
     * branch of the process closer.
     */
    public void failCloseWith(IOException exception) {
      this.closeException = exception;
    }

    /** Pushes bytes into the stream, as if the process had written them. */
    void push(byte[] data) {
      synchronized (lock) {
        if (data.length > pending.length - (pendingEnd - pendingStart)) {
          throw new IllegalArgumentException(name + " pending buffer overflow: " + data.length);
        }
        for (byte b : data) {
          pending[pendingEnd++ % pending.length] = b;
        }
        lock.notifyAll();
      }
    }

    /** Wakes readers so they observe the exit and drain the remaining tail. */
    void signalExit() {
      synchronized (lock) {
        lock.notifyAll();
      }
    }

    public int getTotalRead() {
      synchronized (lock) {
        return totalRead;
      }
    }

    public int getPending() {
      synchronized (lock) {
        return pendingEnd - pendingStart;
      }
    }

    public boolean isClosed() {
      synchronized (lock) {
        return closed;
      }
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      int n = read(one, 0, 1);
      return n < 0 ? -1 : one[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      synchronized (lock) {
        boolean terminal = ScriptedProcess.this.isReadTerminal();
        while (pendingEnd == pendingStart && !closed && !terminal) {
          try {
            lock.wait(SAFEGUARD_MILLIS);
            // A wait that returns without any state change can only mean a stuck test:
            // fail with a location instead of hanging the build.
            if (pendingEnd == pendingStart && !closed && !ScriptedProcess.this.isReadTerminal()) {
              throw new IOException("Safeguard: no data scripted for " + name + " within "
                  + SAFEGUARD_MILLIS + " ms");
            }
            terminal = ScriptedProcess.this.isReadTerminal();
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            IOException ioe = new IOException("Interrupted reading " + name);
            ioe.initCause(e);
            throw ioe;
          }
        }
        if (pendingEnd == pendingStart) {
          return -1;
        }
        int n = Math.min(Math.min(len, maxChunk), pendingEnd - pendingStart);
        for (int i = 0; i < n; i++) {
          b[off + i] = pending[pendingStart++ % pending.length];
        }
        totalRead += n;
        return n;
      }
    }

    @Override
    public void close() throws IOException {
      synchronized (lock) {
        closed = true;
        lock.notifyAll();
      }
      log.record("streamClose", name);
      if (closeException != null) {
        throw closeException;
      }
    }

    @Override
    public String toString() {
      return name + "(read=" + getTotalRead() + ", pending=" + getPending() + ", closed=" + isClosed() + ")";
    }

  }

  /**
   * Records everything written to the process' standard input.
   */
  public static final class RecordingOutputStream extends OutputStream {

    private final EventLog log;
    private final ByteArrayOutputStream data = new ByteArrayOutputStream();
    private boolean closed;
    private IOException closeException;

    RecordingOutputStream(EventLog log) {
      this.log = log;
    }

    @Override
    public synchronized void write(int b) {
      data.write(b);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) {
      data.write(b, off, len);
    }

    /** Makes {@link #close()} fail with the given exception. */
    public void failCloseWith(IOException exception) {
      this.closeException = exception;
    }

    @Override
    public synchronized void close() throws IOException {
      closed = true;
      log.record("stdinClose");
      if (closeException != null) {
        throw closeException;
      }
    }

    public synchronized byte[] getWritten() {
      return data.toByteArray();
    }

    public synchronized boolean isClosed() {
      return closed;
    }

  }

  private final EventLog log;

  private final ScriptedInputStream out;
  private final ScriptedInputStream err;
  private final RecordingOutputStream stdin;

  private final CountDownLatch exitLatch = new CountDownLatch(1);

  private volatile Integer exitValue;
  private volatile boolean ignoreInterrupts;
  private volatile int destroyCount;
  private volatile boolean destroyed;

  public ScriptedProcess(EventLog log) {
    this.log = log;
    this.out = new ScriptedInputStream("stdout", log);
    this.err = new ScriptedInputStream("stderr", log);
    this.stdin = new RecordingOutputStream(log);
  }

  /** Operation {@code writeOut}: the process emits bytes on its standard output. */
  public void writeOut(String data) {
    writeOut(data.getBytes());
  }

  public void writeOut(byte[] data) {
    log.record("writeOut", data.length + " bytes");
    out.push(data);
  }

  /** Operation {@code writeErr}: the process emits bytes on its standard error. */
  public void writeErr(String data) {
    writeErr(data.getBytes());
  }

  public void writeErr(byte[] data) {
    log.record("writeErr", data.length + " bytes");
    err.push(data);
  }

  /** Operation {@code exit}: the process terminates with the given exit value. */
  public void exit(int value) {
    log.record("exit", String.valueOf(value));
    this.exitValue = value;
    out.signalExit();
    err.signalExit();
    exitLatch.countDown();
  }

  /** Makes {@link #waitFor()} keep waiting when the worker thread is interrupted. */
  public void setIgnoreInterrupts(boolean ignoreInterrupts) {
    this.ignoreInterrupts = ignoreInterrupts;
  }

  public boolean isExited() {
    return exitValue != null;
  }

  /**
   * Reads are terminal once the process exited or was destroyed: any pushed but
   * unread tail is still drained, then EOF. Destroying a real OS process closes its
   * output pipes the same way.
   */
  boolean isReadTerminal() {
    return isExited() || destroyed;
  }

  public int getDestroyCount() {
    return destroyCount;
  }

  public ScriptedInputStream getOutStream() {
    return out;
  }

  public ScriptedInputStream getErrStream() {
    return err;
  }

  public RecordingOutputStream getStdinStream() {
    return stdin;
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
  public int waitFor() throws InterruptedException {
    log.record("waitFor");
    while (true) {
      try {
        if (!exitLatch.await(SAFEGUARD_MILLIS, TimeUnit.MILLISECONDS) && !isExited()) {
          throw new AssertionError("Safeguard: process was not scripted to exit within "
              + SAFEGUARD_MILLIS + " ms.\n" + log.dump());
        }
        return exitValue;
      }
      catch (InterruptedException e) {
        log.record("interrupt", "waitFor");
        if (!ignoreInterrupts) {
          throw e;
        }
        // Mutation: a process that ignores the interrupt keeps waiting for its exit.
      }
    }
  }

  @Override
  public boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException {
    return waitFor() >= 0;
  }

  @Override
  public int exitValue() {
    Integer value = exitValue;
    if (value == null) {
      throw new IllegalThreadStateException("process has not exited");
    }
    return value;
  }

  @Override
  public void destroy() {
    destroyCount++;
    destroyed = true;
    log.record("destroy", "count=" + destroyCount);
    out.signalExit();
    err.signalExit();
  }

  @Override
  public boolean isAlive() {
    return !isExited();
  }

  /**
   * @return a snapshot of the fake's resource state, for failure reports.
   */
  public String stateDump() {
    StringBuilder sb = new StringBuilder();
    sb.append("  ").append(out).append('\n');
    sb.append("  ").append(err).append('\n');
    sb.append("  stdin(closed=").append(stdin.isClosed()).append(")\n");
    sb.append("  exited=").append(isExited());
    sb.append(", destroyCount=").append(destroyCount);
    sb.append(", ignoreInterrupts=").append(ignoreInterrupts).append('\n');
    return sb.toString();
  }

  @Override
  public String toString() {
    return "ScriptedProcess";
  }

}
