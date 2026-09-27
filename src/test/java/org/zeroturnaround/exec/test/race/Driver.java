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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.zeroturnaround.exec.ProcessResult;

/**
 * Runs {@code ProcessExecutor.execute()} on a dedicated thread so the test thread stays
 * free to script process events. The outcome (result or failure) is captured exactly
 * once and can be awaited without any sleep.
 */
public class Driver {

  /**
   * Safeguard for joining the driver thread; an upper bound for a broken run, never
   * part of the passing path.
   */
  private static final long JOIN_SAFEGUARD_MILLIS = EventLog.AWAIT_SAFEGUARD_MILLIS;

  /**
   * What the execute call produced.
   */
  public static final class Outcome {

    private final ProcessResult result;
    private final Throwable failure;

    Outcome(ProcessResult result, Throwable failure) {
      this.result = result;
      this.failure = failure;
    }

    public boolean isSuccess() {
      return failure == null;
    }

    public ProcessResult getResult() {
      return result;
    }

    public Throwable getFailure() {
      return failure;
    }

    @Override
    public String toString() {
      return isSuccess()
          ? "success(exit=" + result.getExitValue() + ")"
          : "failure(" + failure.getClass().getName() + ": " + failure.getMessage() + ")";
    }

  }

  /**
   * The call to drive.
   */
  public interface Call {
    ProcessResult run() throws Exception;
  }

  private final EventLog log;
  private final CountDownLatch done = new CountDownLatch(1);
  private final Thread thread;

  private volatile Outcome outcome;

  public Driver(EventLog log, String name, Call call) {
    this.log = log;
    this.thread = new Thread(() -> {
      try {
        outcome = new Outcome(call.run(), null);
      }
      catch (Throwable t) {
        outcome = new Outcome(null, t);
      }
      finally {
        log.record("driverDone");
        done.countDown();
      }
    }, name);
    this.thread.setDaemon(true);
  }

  public Driver start() {
    thread.start();
    return this;
  }

  public Thread getThread() {
    return thread;
  }

  public boolean isDone() {
    return done.getCount() == 0;
  }

  /**
   * Waits for the execute call to finish and returns its outcome. Fails with the event
   * sequence and the driver stack when the call does not finish within the safeguard.
   */
  public Outcome awaitDone() {
    try {
      if (!done.await(JOIN_SAFEGUARD_MILLIS, TimeUnit.MILLISECONDS)) {
        throw new AssertionError("Safeguard: execute() did not finish within "
            + JOIN_SAFEGUARD_MILLIS + " ms. Driver thread state: "
            + thread.getState() + "\n" + log.dump());
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted waiting for the driver.\n" + log.dump());
    }
    return outcome;
  }

}
