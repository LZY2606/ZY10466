package org.zeroturnaround.exec.test.race;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.zeroturnaround.exec.test.race.EventTracer.Op.START;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;

/**
 * Deterministic output-backpressure scenarios: both streams full, merged versus
 * separate pumping, short writes, close failures and blocked consumers. The fake
 * pipes carry a byte budget that models a full OS pipe, so the amount of memory
 * in flight has a hard, assertable bound and every failure names the pipe and the
 * offending position.
 */
public class OutputBackpressureTest extends RaceTestBase {

  /** The copy buffer size of the library's StreamPumper. */
  private static final int PUMP_BUFFER = 1024;

  /**
   * Both streams are filled beyond what the consumers accept: each pump parks in
   * its consumer write, each pipe fills to exactly its budget and nothing is lost
   * or crossed between the streams once the consumers drain.
   */
  @Test
  public void separateStreamsBothFullDrainInOrder() throws Exception {
    int budget = 4096;
    int chunk = 512;
    ScriptedProcess process = newProcess("both-streams-full");
    process.out().setByteBudget(budget);
    process.err().setByteBudget(budget);
    GatedOutputStream outGate = new GatedOutputStream("out", tracer);
    GatedOutputStream errGate = new GatedOutputStream("err", tracer);
    ProcessExecutor executor = executor(process).redirectOutput(outGate).redirectError(errGate);
    byte[] outData = pattern(16 * 1024, 1);
    byte[] errData = pattern(8 * 1024, 2);
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));

    // The first chunk of each stream is consumed by its pump, which then parks in
    // the gated consumer write; the pipes then fill to exactly their budget.
    process.writeOut(slice(outData, 0, chunk));
    outGate.awaitEntered(diagnostics(process));
    process.writeErr(slice(errData, 0, chunk));
    errGate.awaitEntered(diagnostics(process));

    int outOffset = fillPipe(process.out(), outData, chunk, chunk);
    int errOffset = fillPipe(process.err(), errData, chunk, chunk);
    assertPipeFull(process, process.out(), budget);
    assertPipeFull(process, process.err(), budget);
    assertEquals("out consumer is blocked before its first byte", 0, outGate.received().length);
    assertEquals("err consumer is blocked before its first byte", 0, errGate.received().length);

    outGate.release();
    errGate.release();
    writeRemaining(process.out(), outData, outOffset, chunk, diagnostics(process));
    writeRemaining(process.err(), errData, errOffset, chunk, diagnostics(process));
    process.exit(0);

    ProcessResult result = driver.joinSuccess(diagnostics(process));
    assertEquals(0, result.getExitValue());
    assertFullyDelivered("stdout", outData, outGate.received());
    assertFullyDelivered("stderr", errData, errGate.received());
    assertEquals("stdout pipe never held more than its budget", budget, process.out().maxInFlight());
    assertEquals("stderr pipe never held more than its budget", budget, process.err().maxInFlight());
  }

  /**
   * With the error stream merged into the output stream (the library default),
   * the single pump delivers the bytes in the exact order the process produced
   * them and no error pump is created at all.
   */
  @Test
  public void mergedStreamPreservesWriteOrder() throws Exception {
    ScriptedProcess process = newMergedProcess("merged-streams");
    ByteArrayOutputStream capture = new ByteArrayOutputStream();
    ProcessExecutor executor = executor(process).redirectOutput(capture);
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));

    process.writeOut(utf8("out-1\n"));
    process.writeErr(utf8("err-1\n"));
    process.writeOut(utf8("out-2\n"));
    process.exit(0);

    ProcessResult result = driver.joinSuccess(diagnostics(process));
    assertEquals(0, result.getExitValue());
    assertEquals("out-1\nerr-1\nout-2\n", capture.toString("UTF-8"));
    assertEquals("no error pump reads the separate error pipe", 0, process.err().totalRead());
  }

  /**
   * Short writes — single bytes and odd-sized chunks — are delivered intact and
   * in order on both streams.
   */
  @Test
  public void shortWritesAreDeliveredIntact() throws Exception {
    ScriptedProcess process = newProcess("short-writes");
    ByteArrayOutputStream outCapture = new ByteArrayOutputStream();
    ByteArrayOutputStream errCapture = new ByteArrayOutputStream();
    ProcessExecutor executor = executor(process)
        .redirectOutput(outCapture)
        .redirectError(errCapture);
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));

    ByteArrayOutputStream expectedOut = new ByteArrayOutputStream();
    ByteArrayOutputStream expectedErr = new ByteArrayOutputStream();
    for (int i = 0; i < 100; i++) {
      process.writeOut(new byte[] { (byte) i });
      expectedOut.write(i);
      process.writeErr(new byte[] { (byte) (255 - i) });
      expectedErr.write(255 - i);
    }
    byte[] odd = pattern(1023, 3);
    process.writeOut(odd);
    expectedOut.write(odd, 0, odd.length);
    process.exit(0);

    driver.joinSuccess(diagnostics(process));
    assertArrayEquals(expectedOut.toByteArray(), outCapture.toByteArray());
    assertArrayEquals(expectedErr.toByteArray(), errCapture.toByteArray());
  }

  /**
   * A blocked output consumer bounds the memory in flight: the pipe fills to
   * exactly its budget, the pump holds at most one copy buffer and the captured
   * output buffer cannot grow past what the pump delivered. Once the consumer
   * drains, everything arrives and the bound is confirmed never to have been
   * exceeded.
   */
  @Test
  public void blockedConsumerBoundsInFlightMemory() throws Exception {
    int budget = 8192;
    ScriptedProcess process = newProcess("bounded-memory");
    process.out().setByteBudget(budget);
    GatedOutputStream gate = new GatedOutputStream("out", tracer);
    ProcessExecutor executor = executor(process).redirectOutput(gate).readOutput(true);
    byte[] data = pattern(64 * 1024, 7);
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));

    process.writeOut(slice(data, 0, PUMP_BUFFER));
    gate.awaitEntered(diagnostics(process));
    int offset = fillPipe(process.out(), data, PUMP_BUFFER, PUMP_BUFFER);

    assertPipeFull(process, process.out(), budget);
    assertEquals("consumer is blocked before its first byte", 0, gate.received().length);

    gate.release();
    writeRemaining(process.out(), data, offset, PUMP_BUFFER, diagnostics(process));
    process.exit(0);

    ProcessResult result = driver.joinSuccess(diagnostics(process));
    assertFullyDelivered("stdout", data, gate.received());
    assertArrayEquals("captured output matches the full stream", data, result.getOutput().getBytes());
    assertEquals("the pipe budget is the stable in-flight bound", budget, process.out().maxInFlight());
  }

  /**
   * A blocked line consumer bounds buffering to the pipe budget plus one pump
   * buffer plus one partial line, and all lines arrive in order once it drains.
   */
  @Test
  public void blockedLineConsumerBoundsBuffering() throws Exception {
    int budget = 4096;
    int lineCount = 200;
    ScriptedProcess process = newProcess("bounded-lines");
    process.out().setByteBudget(budget);
    GatedLineConsumer consumer = new GatedLineConsumer("lines", tracer);
    ProcessExecutor executor = executor(process).redirectOutput(
        org.zeroturnaround.exec.stream.LogOutputStream.create(consumer));
    StringBuilder all = new StringBuilder();
    for (int i = 0; i < lineCount; i++) {
      all.append(String.format("line-%04d-%89s\n", i, ""));
    }
    byte[] data = utf8(all.toString());
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));

    process.writeOut(slice(data, 0, 2 * PUMP_BUFFER));
    consumer.awaitEntered(diagnostics(process));
    int offset = fillPipe(process.out(), data, 2 * PUMP_BUFFER, PUMP_BUFFER);
    assertPipeFull(process, process.out(), budget);

    consumer.release();
    writeRemaining(process.out(), data, offset, PUMP_BUFFER, diagnostics(process));
    process.exit(0);

    driver.joinSuccess(diagnostics(process));
    List<String> lines = consumer.lines();
    assertEquals(lineCount, lines.size());
    for (int i = 0; i < lineCount; i++) {
      assertEquals(String.format("line-%04d-%89s", i, ""), lines.get(i));
    }
    assertEquals(budget, process.out().maxInFlight());
  }

  /**
   * Close failures of the process streams surface through the existing
   * IOException contract of {@code execute()}: the stdin close failure is the
   * primary failure and the stdout close failure is attached as suppressed.
   */
  @Test
  public void closeFailuresPropagateThroughIOException() throws Exception {
    ScriptedProcess process = newProcess("close-failures");
    process.stdin().failOnClose(new IOException("scripted stdin close failure"));
    process.out().failOnClose(new IOException("scripted stdout close failure"));
    ProcessExecutor executor = executor(process);
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));

    process.exit(0);

    IOException e = driver.joinFailure(IOException.class, diagnostics(process));
    assertEquals("scripted stdin close failure", e.getMessage());
    assertEquals(1, e.getSuppressed().length);
    assertEquals("scripted stdout close failure", e.getSuppressed()[0].getMessage());
  }

  /**
   * Bytes still queued in the pipe when the process exits are drained before the
   * end-of-stream: the tail is not lost.
   */
  @Test
  public void undrainedTailIsDeliveredAfterExit() throws Exception {
    ScriptedProcess process = newProcess("undrained-tail");
    ProcessExecutor executor = executor(process).readOutput(true);
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));

    process.writeOut(utf8("tail-after-exit"));
    process.exit(0);

    ProcessResult result = driver.joinSuccess(diagnostics(process));
    assertEquals("tail-after-exit", result.outputUTF8());
    assertEquals("nothing was dropped", 0, process.out().totalDropped());
  }

  /**
   * Directed mutation "undrained tail": the process exits abruptly and its pipe
   * is reset before the pump drains it. The result is stable — exactly one pump
   * buffer that was already in flight is delivered — and the delivery check
   * reports the loss with the pipe name and the first missing byte.
   */
  @Test
  public void abruptExitDropsUndrainedTailAndHarnessReportsIt() throws Exception {
    ScriptedProcess process = newProcess("abrupt-exit");
    GatedOutputStream gate = new GatedOutputStream("out", tracer);
    ProcessExecutor executor = executor(process).redirectOutput(gate);
    final byte[] data = pattern(4 * PUMP_BUFFER, 9);
    Driver driver = drive(process, executor);
    tracer.awaitCount(START, 1, diagnostics(process));

    process.writeOut(data);
    gate.awaitEntered(diagnostics(process));
    assertEquals("one pump buffer is in flight, the rest waits in the pipe",
        3 * PUMP_BUFFER, process.out().inFlight());

    process.exitAbruptly(0);
    assertEquals("the undrained tail is discarded by the reset pipe", 0, process.out().inFlight());
    assertEquals(3 * PUMP_BUFFER, process.out().totalDropped());
    gate.release();

    ProcessResult result = driver.joinSuccess(diagnostics(process));
    assertEquals(0, result.getExitValue());
    final byte[] received = gate.received();
    assertArrayEquals("exactly the in-flight pump buffer survives", slice(data, 0, PUMP_BUFFER), received);

    AssertionError error = expectAssertion("full delivery", new Runnable() {
      public void run() {
        assertFullyDelivered("stdout", data, received);
      }
    });
    assertThat(error.getMessage(), containsString("stdout"));
    assertThat(error.getMessage(), containsString("diverges at byte " + PUMP_BUFFER));
  }

}
