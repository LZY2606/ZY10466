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
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessResult;

/**
 * Deterministic races of the output pumps against process exit and a slow consumer:
 * separated vs merged streams, short writes, the tail that arrives right at exit,
 * close error branches, and the memory bound under backpressure. No platform shell,
 * signals or sleeps are involved.
 */
public class StreamBackpressureTest {

  private final EventLog log = new EventLog();
  private final ScriptedProcess process = new ScriptedProcess(log);

  @Rule
  public RaceReportRule report = new RaceReportRule(log, process);

  /** One pump buffer may sit between the process and a blocked consumer. */
  private static final int PUMP_BUFFER_SIZE = 1024;

  private ScriptedProcessExecutor executor() {
    return new ScriptedProcessExecutor(log, process);
  }

  private static byte[] pattern(int length, byte fill) {
    byte[] data = new byte[length];
    Arrays.fill(data, fill);
    return data;
  }

  @Test
  public void separatedStreamsBothFillWithoutDeadlock() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    byte[] outData = pattern(64 * 1024, (byte) 'o');
    byte[] errData = pattern(64 * 1024, (byte) 'e');

    process.writeOut(outData);
    process.writeErr(errData);
    process.exit(0);

    ScriptedProcessExecutor executor = executor();
    executor.redirectOutput(out).redirectError(err);
    Assert.assertEquals(0, executor.execute().getExitValue());

    Assert.assertArrayEquals(outData, out.toByteArray());
    Assert.assertArrayEquals(errData, err.toByteArray());
    Assert.assertEquals(0, process.getOutStream().getPending());
    Assert.assertEquals(0, process.getErrStream().getPending());
  }

  @Test
  public void mergedStreamsCarryEverythingThroughStdout() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    process.writeOut("out-line\n");
    process.writeErr("err-line\n");
    process.exit(0);

    // Default configuration: error stream merged into output.
    ScriptedProcessExecutor executor = executor();
    executor.redirectOutput(out);
    Assert.assertEquals(0, executor.execute().getExitValue());

    Assert.assertEquals("out-line\n", out.toString());
    // The library never even pumps the error stream when they are merged.
    Assert.assertEquals(0, process.getErrStream().getTotalRead());
  }

  @Test
  public void shortWritesAreReassembled() throws Exception {
    process.getOutStream().setMaxChunk(1);
    process.writeOut("0123456789-0123456789-0123456789");
    process.exit(0);

    ProcessResult result = executor().readOutput(true).execute();

    Assert.assertEquals("0123456789-0123456789-0123456789", result.outputUTF8());
  }

  /**
   * Directed mutation: data written right at exit must still be drained by the pumps
   * before the output snapshot is taken.
   */
  @Test
  public void tailWrittenAtExitIsDrained() throws Exception {
    process.writeOut("tail-content");
    process.exit(0);

    ProcessResult result = executor().readOutput(true).execute();

    Assert.assertEquals("tail-content", result.outputUTF8());
    Assert.assertEquals(0, process.getOutStream().getPending());
  }

  /**
   * A close failure must surface as a distinguishable IOException carrying the exact
   * branch that failed, rather than being swallowed or reclassified.
   */
  @Test
  public void streamCloseExceptionIsDistinguishable() throws Exception {
    process.getOutStream().failCloseWith(new IOException("boom-close-out"));
    process.exit(0);

    try {
      executor().execute();
      Assert.fail("IOException expected from closing the output stream");
    }
    catch (IOException e) {
      Assert.assertEquals("boom-close-out", e.getMessage());
    }
  }

  /**
   * The historical "Stream closed" quirk of the process' own output stream (its
   * standard input) is tolerated; everything else is propagated.
   */
  @Test
  public void streamClosedQuirkIsToleratedButOtherStdinErrorsAreNot() throws Exception {
    process.getStdinStream().failCloseWith(new IOException("Stream closed"));
    process.exit(0);
    Assert.assertEquals(0, executor().execute().getExitValue());

    process.getStdinStream().failCloseWith(new IOException("boom-close-stdin"));
    try {
      executor().execute();
      Assert.fail("IOException expected from closing standard input");
    }
    catch (IOException e) {
      Assert.assertEquals("boom-close-stdin", e.getMessage());
    }
  }

  /**
   * Backpressure: once the consumer blocks, the pump holds at most one buffer of
   * output. The failure report points at the pump thread parked in write().
   */
  @Test
  public void blockedConsumerBoundsReadAhead() throws Exception {
    final int capacity = 100;
    BlockingOutputStream consumer = new BlockingOutputStream(log, capacity);
    byte[] data = pattern(64 * 1024, (byte) 'x');
    process.writeOut(data);
    process.exit(0);

    Driver driver = new Driver(log, "zt-exec-test-driver", () -> executor().redirectOutput(consumer).execute()).start();
    log.await("consumerBlocked");

    int consumed = process.getOutStream().getTotalRead();
    int delivered = consumer.getAccepted();
    int inFlight = consumed - delivered;
    Assert.assertTrue("pump read ahead " + inFlight + " bytes (consumed=" + consumed
        + ", delivered=" + delivered + "), bound is one buffer (" + PUMP_BUFFER_SIZE + ")",
        inFlight <= PUMP_BUFFER_SIZE);
    Assert.assertTrue("nothing was delivered to the blocked consumer", delivered >= capacity);

    consumer.release();
    Driver.Outcome outcome = driver.awaitDone();
    Assert.assertTrue("execute() should finish once the consumer drains, got " + outcome,
        outcome.isSuccess());
    Assert.assertArrayEquals(data, consumer.getReceived());
  }

  /**
   * A consumer that is never released hits the explicit blocking bound instead of
   * leaving a pump thread stuck forever. The pump surfaces the failure by dying (the
   * production pumper swallows stream errors by design), so execute() completes and
   * the undelivered bytes are observable.
   */
  @Test
  public void consumerBlockingHasAnExplicitUpperBound() throws Exception {
    BlockingOutputStream consumer = new BlockingOutputStream(log, 0, 200);
    process.writeOut(new byte[] { 1 });
    process.exit(0);

    Driver driver = new Driver(log, "zt-exec-test-driver",
        () -> executor().redirectOutput(consumer).execute()).start();
    Driver.Outcome outcome = driver.awaitDone();
    Assert.assertTrue("execute() should complete once the blocking bound is hit, got "
        + outcome, outcome.isSuccess());
    Assert.assertTrue("consumer must not receive bytes it never accepted",
        consumer.getReceived().length == 0);
    Assert.assertTrue(log.contains("consumerBlocked"));
  }

}
