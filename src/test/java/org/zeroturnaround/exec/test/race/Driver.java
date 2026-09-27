package org.zeroturnaround.exec.test.race;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.zeroturnaround.exec.ProcessResult;

/**
 * Runs {@code ProcessExecutor.execute()} on a dedicated thread so the test thread
 * stays free to drive the script. The outcome (result or failure) is captured for
 * assertions, and the thread is always joined before the test ends, so no
 * background work survives a scenario.
 */
final class Driver {

  /** A task that may fail the way {@code ProcessExecutor.execute()} does. */
  interface Executing {
    ProcessResult run() throws Exception;
  }

  private final Thread thread;
  private final CountDownLatch finished = new CountDownLatch(1);
  private volatile ProcessResult result;
  private volatile Throwable failure;

  private Driver(String name, final Executing executing) {
    thread = new Thread(new Runnable() {
      public void run() {
        try {
          result = executing.run();
        }
        catch (Throwable t) {
          failure = t;
        }
        finally {
          finished.countDown();
        }
      }
    }, name);
    thread.setDaemon(true);
  }

  static Driver start(String name, Executing executing) {
    Driver driver = new Driver(name, executing);
    driver.thread.start();
    return driver;
  }

  private void join(Supplier<String> diagnostics) {
    try {
      if (!finished.await(RaceTestBase.AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        throw new AssertionError("driver thread " + thread.getName() + " did not finish\n" + diagnostics.get());
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("interrupted while joining the driver thread\n" + diagnostics.get());
    }
  }

  /** Joins and returns the result; fails if the execution threw. */
  ProcessResult joinSuccess(Supplier<String> diagnostics) {
    join(diagnostics);
    if (failure != null) {
      AssertionError error = new AssertionError("expected the execution to succeed but it threw "
          + failure + "\n" + diagnostics.get());
      error.initCause(failure);
      throw error;
    }
    return result;
  }

  /** Joins and returns the failure of the expected type. */
  <T extends Throwable> T joinFailure(Class<T> type, Supplier<String> diagnostics) {
    join(diagnostics);
    if (failure == null) {
      throw new AssertionError("expected " + type.getSimpleName() + " but the execution succeeded with exit value "
          + result.getExitValue() + "\n" + diagnostics.get());
    }
    if (!type.isInstance(failure)) {
      AssertionError error = new AssertionError("expected " + type.getSimpleName() + " but got "
          + failure.getClass().getName() + ": " + failure.getMessage() + "\n" + diagnostics.get());
      error.initCause(failure);
      throw error;
    }
    return type.cast(failure);
  }

}
