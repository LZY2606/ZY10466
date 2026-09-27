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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.listener.ProcessListener;

/**
 * A {@link ProcessExecutor} whose process creation and timeout linearization point are
 * controlled by the test instead of the OS and the wall clock:
 * <ul>
 *   <li>{@link #invokeStart()} returns a {@link ScriptedProcess} (operation
 *   {@code start}).</li>
 *   <li>When a timeout is configured, the future returned by {@link #invokeSubmit} is
 *   gated: its timed {@code get} only completes when the task has already finished, or
 *   throws {@link TimeoutException} once the test fires {@link #fireTimeout()}
 *   (operation {@code timeout}). The configured timeout value still appears in messages
 *   but no wall-clock deadline decides the outcome.</li>
 * </ul>
 * Everything between these two seams (stream pumping, waiting, stopping, closing) is
 * the production code path, unchanged.
 */
public class ScriptedProcessExecutor extends ProcessExecutor {

  /**
   * The timeout linearization point. A gated {@code get(timeout, unit)} blocks on this
   * gate; when the test fires it, the get re-checks the task exactly once: a finished
   * task yields its result (completion linearized first), a running task yields
   * {@link TimeoutException} (timeout linearized first). This mirrors
   * {@code FutureTask.get(timeout, unit)}, where whichever of the two happens first
   * wins, but makes "first" a decision of the test rather than of the scheduler.
   */
  static final class TimeoutGate {

    private final EventLog log;
    private final CountDownLatch fired = new CountDownLatch(1);

    TimeoutGate(EventLog log) {
      this.log = log;
    }

    void fire() {
      log.record("timeout");
      fired.countDown();
    }

    void await() throws InterruptedException {
      if (!fired.await(EventLog.AWAIT_SAFEGUARD_MILLIS, TimeUnit.MILLISECONDS)) {
        throw new AssertionError("Safeguard: test did not fire the timeout gate.\n" + log.dump());
      }
    }

  }

  /**
   * Future returned to {@code ProcessExecutor.waitFor} while the gate is armed. Only
   * the timed {@code get} is gated; every other method delegates, so cancellation and
   * completion keep their production semantics.
   */
  static final class GatedFuture<T> implements Future<T> {

    private final Future<T> delegate;
    private final TimeoutGate gate;

    GatedFuture(Future<T> delegate, TimeoutGate gate) {
      this.delegate = delegate;
      this.gate = gate;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      return delegate.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      if (!delegate.isDone()) {
        gate.await();
      }
      if (delegate.isDone()) {
        // Completion linearized before the timeout.
        return delegate.get();
      }
      // Timeout linearized before the completion.
      throw new TimeoutException();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
      return delegate.isDone();
    }

  }

  private final EventLog log;
  private final ScriptedProcess process;
  private final TimeoutGate gate;

  /** The most recently submitted waiting task, for {@link #awaitWorkerCompletion()}. */
  private volatile Future<?> workerTask;

  public ScriptedProcessExecutor(EventLog log, ScriptedProcess process) {
    this.log = log;
    this.process = process;
    this.gate = new TimeoutGate(log);
    command("zt-exec-scripted-command");
    addListener(new ProcessListener() {
      @Override
      public void afterFinish(Process process, ProcessResult result) {
        log.record("finish", "exit=" + result.getExitValue());
      }

      @Override
      public void afterStop(Process process) {
        // Operation stopComplete: the worker left WaitForProcess entirely.
        ScriptedProcessExecutor.this.log.record("stopComplete");
      }
    });
  }

  public ScriptedProcess getScriptedProcess() {
    return process;
  }

  /** Operation {@code timeout}: the test fires the timeout linearization point. */
  public void fireTimeout() {
    gate.fire();
  }

  @Override
  protected Process invokeStart() throws IOException {
    log.record("start");
    return process;
  }

  @Override
  protected <T> Future<T> invokeSubmit(ExecutorService executor, Callable<T> task) {
    Future<T> delegate = super.invokeSubmit(executor, task);
    this.workerTask = delegate;
    return new GatedFuture<T>(delegate, gate);
  }

  /**
   * Waits until the background waiting task has completed. Gives the test a
   * happens-after edge on the worker, so that a timeout fired afterwards linearizes
   * deterministically behind the completion.
   */
  public void awaitWorkerCompletion() {
    Future<?> task = workerTask;
    if (task == null) {
      throw new IllegalStateException("No task was submitted; is a timeout configured?");
    }
    try {
      task.get(EventLog.AWAIT_SAFEGUARD_MILLIS, TimeUnit.MILLISECONDS);
    }
    catch (TimeoutException e) {
      throw new AssertionError("Safeguard: worker task did not complete.\n" + log.dump());
    }
    catch (ExecutionException e) {
      // A failed task is still a completed task; the driver observes the failure.
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted waiting for the worker task.\n" + log.dump());
    }
  }

}
