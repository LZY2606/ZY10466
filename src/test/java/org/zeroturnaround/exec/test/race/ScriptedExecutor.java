package org.zeroturnaround.exec.test.race;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;

/**
 * A {@link ProcessExecutor} that runs against a {@link ScriptedProcess} and a
 * scripted timeout. Both hooks already exist as protected extension points of the
 * library, so the production code under test (WaitForProcess, stream pumps,
 * closers, stoppers) is exercised unmodified:
 *
 * <ul>
 *   <li>{@link #startProcess()} returns the scripted process instead of forking.</li>
 *   <li>{@link #invokeSubmit} wraps the wait task so the timed {@code Future.get}
 *   blocks on the {@link TimeoutGate} instead of the wall clock.</li>
 * </ul>
 */
final class ScriptedExecutor extends ProcessExecutor {

  /**
   * Future whose timed {@code get} linearizes exactly where the script says: it
   * returns the task outcome once the task is done, or throws
   * {@link TimeoutException} once the gate fires, whichever the script ordered
   * first.
   */
  private static final class LinearizableFuture<T> implements Future<T> {
    private final Future<T> real;
    private final TimeoutGate gate;

    LinearizableFuture(Future<T> real, TimeoutGate gate) {
      this.real = real;
      this.gate = gate;
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
      return real.cancel(mayInterruptIfRunning);
    }

    public boolean isCancelled() {
      return real.isCancelled();
    }

    public boolean isDone() {
      return real.isDone();
    }

    public T get() throws InterruptedException, ExecutionException {
      return real.get();
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      if (real.isDone()) {
        return real.get();
      }
      while (true) {
        TimeoutGate.Signal signal = gate.take();
        if (signal == TimeoutGate.Signal.TIMEOUT) {
          throw new TimeoutException();
        }
        // The task signalled completion from its finally block; the executor
        // future is set immediately afterwards, so this returns promptly.
        return real.get();
      }
    }
  }

  private final ScriptedProcess process;
  private final EventTracer tracer;
  private final TimeoutGate gate;
  private final CountDownLatch workerDone = new CountDownLatch(1);
  private volatile Future<ProcessResult> workerFuture;

  ScriptedExecutor(ScriptedProcess process, EventTracer tracer, TimeoutGate gate) {
    super("scripted", process.toString());
    this.process = process;
    this.tracer = tracer;
    this.gate = gate;
  }

  @Override
  protected Process startProcess() {
    tracer.record(EventTracer.Op.START);
    return process;
  }

  @Override
  protected <T> Future<T> invokeSubmit(ExecutorService executor, Callable<T> task) {
    Future<T> real = executor.submit(wrapTask(new SignallingCallable<T>(task)));
    @SuppressWarnings("unchecked")
    Future<ProcessResult> typed = (Future<ProcessResult>) real;
    workerFuture = typed;
    return new LinearizableFuture<T>(real, gate);
  }

  /** Runs the wait task and signals its completion to the gate and the harness. */
  private final class SignallingCallable<T> implements Callable<T> {
    private final Callable<T> delegate;

    SignallingCallable(Callable<T> delegate) {
      this.delegate = delegate;
    }

    public T call() throws Exception {
      try {
        return delegate.call();
      }
      finally {
        tracer.record(EventTracer.Op.WORKER_DONE);
        gate.taskDone();
        workerDone.countDown();
      }
    }
  }

  /** The future of the wait task, for assertions on the worker outcome. */
  Future<ProcessResult> workerFuture() {
    return workerFuture;
  }

  /** Waits until the wait task has finished; the deadline is a hang guard only. */
  void awaitWorkerDone(Supplier<String> diagnostics) {
    try {
      if (!workerDone.await(RaceTestBase.AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        throw new AssertionError("wait task did not finish\n" + diagnostics.get());
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("interrupted while waiting for the wait task\n" + diagnostics.get());
    }
  }

}
