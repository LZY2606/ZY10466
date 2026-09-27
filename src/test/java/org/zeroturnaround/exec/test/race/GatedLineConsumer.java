package org.zeroturnaround.exec.test.race;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.zeroturnaround.exec.stream.LineConsumer;

/**
 * A line consumer that parks the pump thread on the first line until released,
 * then records every line in order. Used to pin down the buffering bounds of the
 * line-parsing consumer path without sleeps.
 */
final class GatedLineConsumer implements LineConsumer {

  private final String name;
  private final EventTracer tracer;
  private final CountDownLatch entered = new CountDownLatch(1);
  private final Semaphore gate = new Semaphore(0);
  private final List<String> lines = new ArrayList<String>();
  private volatile boolean released;

  GatedLineConsumer(String name, EventTracer tracer) {
    this.name = name;
    this.tracer = tracer;
  }

  public void accept(String line) {
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
        return;
      }
    }
    synchronized (lines) {
      lines.add(line);
    }
  }

  void awaitEntered(Supplier<String> diagnostics) {
    try {
      if (!entered.await(RaceTestBase.AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        throw new AssertionError("no line reached the gated consumer " + name + "\n" + diagnostics.get());
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

  List<String> lines() {
    synchronized (lines) {
      return new ArrayList<String>(lines);
    }
  }

}
