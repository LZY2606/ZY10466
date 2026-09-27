package org.zeroturnaround.exec.test.race;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The deterministic clock for timeout scenarios. {@code ProcessExecutor} waits on
 * the future returned by {@code invokeSubmit}; the harness hands it a future whose
 * timed {@code get} blocks until the script either completes the task or fires the
 * timeout signal. The linearization point of the timeout is therefore chosen by
 * the test, never by the wall clock.
 */
final class TimeoutGate {

  enum Signal {
    TIMEOUT,
    TASK_DONE
  }

  private final BlockingQueue<Signal> signals = new LinkedBlockingQueue<Signal>();
  private final EventTracer tracer;

  TimeoutGate(EventTracer tracer) {
    this.tracer = tracer;
  }

  /** Script operation: the execution timeout fires now. */
  void fireTimeout() {
    tracer.record(EventTracer.Op.TIMEOUT);
    signals.offer(Signal.TIMEOUT);
  }

  /** Called by the worker wrapper when the wait task finishes for any reason. */
  void taskDone() {
    signals.offer(Signal.TASK_DONE);
  }

  Signal take() throws InterruptedException {
    return signals.take();
  }

}
