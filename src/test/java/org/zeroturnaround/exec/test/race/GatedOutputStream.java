package org.zeroturnaround.exec.test.race;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A consumer that blocks deterministically: the first write parks the calling pump
 * thread until {@link #release()} is scripted. Everything written after the
 * release is captured for comparison with what the process produced. The block is
 * bounded by the harness deadline, so a broken scenario fails instead of hanging.
 */
final class GatedOutputStream extends OutputStream {

  private final String name;
  private final EventTracer tracer;
  private final CountDownLatch entered = new CountDownLatch(1);
  private final Semaphore gate = new Semaphore(0);
  private final ByteArrayOutputStream received = new ByteArrayOutputStream();
  private volatile boolean released;

  GatedOutputStream(String name, EventTracer tracer) {
    this.name = name;
    this.tracer = tracer;
  }

  @Override
  public void write(int b) throws IOException {
    write(new byte[] { (byte) b }, 0, 1);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (!released) {
      if (entered.getCount() > 0) {
        entered.countDown();
        tracer.record(EventTracer.Op.CONSUMER_BLOCK, name);
      }
      try {
        gate.acquire();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        InterruptedIOException interrupted = new InterruptedIOException("gated consumer " + name + " interrupted");
        interrupted.initCause(e);
        throw interrupted;
      }
    }
    synchronized (received) {
      received.write(b, off, len);
    }
  }

  /** Waits until a pump thread is parked inside {@code write}. */
  void awaitEntered(Supplier<String> diagnostics) {
    try {
      if (!entered.await(RaceTestBase.AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        throw new AssertionError("no consumer write reached the gate " + name + "\n" + diagnostics.get());
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("interrupted while waiting for the gated consumer " + name + "\n" + diagnostics.get());
    }
  }

  void release() {
    released = true;
    tracer.record(EventTracer.Op.CONSUMER_RELEASE, name);
    gate.release();
  }

  byte[] received() {
    synchronized (received) {
      return received.toByteArray();
    }
  }

}
