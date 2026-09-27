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
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A consumer that applies backpressure: it accepts {@code capacity} bytes and then
 * blocks inside {@code write} until the test releases it. The block has an explicit
 * upper bound ({@link #BLOCK_SAFEGUARD_MILLIS}) after which the write fails instead of
 * hanging the pump thread forever.
 */
public class BlockingOutputStream extends OutputStream {

  /**
   * Explicit upper bound for how long a consumer may block a pump. Only reached in a
   * broken run; the passing path is released by the test well before it.
   */
  public static final long BLOCK_SAFEGUARD_MILLIS = EventLog.AWAIT_SAFEGUARD_MILLIS;

  private final EventLog log;
  private final int capacity;
  private final long blockSafeguardMillis;
  private final CountDownLatch release = new CountDownLatch(1);
  private final ByteArrayOutputStream received = new ByteArrayOutputStream();

  private int accepted;
  private boolean released;

  public BlockingOutputStream(EventLog log, int capacity) {
    this(log, capacity, BLOCK_SAFEGUARD_MILLIS);
  }

  /**
   * @param blockSafeguardMillis custom explicit upper bound for the block, for tests
   *        that exercise the bound itself.
   */
  public BlockingOutputStream(EventLog log, int capacity, long blockSafeguardMillis) {
    this.log = log;
    this.capacity = capacity;
    this.blockSafeguardMillis = blockSafeguardMillis;
  }

  /** Lets the blocked write continue. */
  public void release() {
    synchronized (this) {
      if (released) {
        return;
      }
      released = true;
      log.record("consumerReleased");
      release.countDown();
    }
  }

  public synchronized int getAccepted() {
    return accepted;
  }

  public synchronized byte[] getReceived() {
    return received.toByteArray();
  }

  @Override
  public void write(int b) throws IOException {
    write(new byte[] { (byte) b }, 0, 1);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    int offset = off;
    int remaining = len;
    while (remaining > 0) {
      int chunk;
      synchronized (this) {
        chunk = released ? remaining : Math.min(remaining, Math.max(0, capacity - accepted));
        if (chunk > 0) {
          received.write(b, offset, chunk);
          accepted += chunk;
          offset += chunk;
          remaining -= chunk;
        }
      }
      if (remaining > 0) {
        log.record("consumerBlocked", "accepted=" + getAccepted());
        try {
          if (!release.await(blockSafeguardMillis, TimeUnit.MILLISECONDS)) {
            throw new IOException("Consumer was not released within "
                + blockSafeguardMillis + " ms (explicit blocking bound)");
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          IOException ioe = new IOException("Consumer interrupted while blocked");
          ioe.initCause(e);
          throw ioe;
        }
      }
    }
  }

}
