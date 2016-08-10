package com.yahoo.ycsb.db;

import com.datastax.driver.core.QueryTrace;
import com.google.common.base.Optional;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * AsyncTraceWriter is used for writing query traces to a file
 * only after some time has passed. This is required because
 * the trace is not immediately available. Additionally, this
 * prevents worker threads from stalling.
 */
public final class AsyncTraceWriter {
  public static final long DEFAULT_DELAY_MILLIS = 5000;

  private final OutputStream rawOut;
  private final GZIPOutputStream gzipOut;

  private final List<Optional<MarkedQueryTrace>> work =
    Collections.synchronizedList(new LinkedList<Optional<MarkedQueryTrace>>());
  private final Thread asyncWriter;

  public AsyncTraceWriter(Path gzDataPath) throws IOException { this(gzDataPath, DEFAULT_DELAY_MILLIS); }

  public AsyncTraceWriter(Path gzDataPath, final long delayMillis) throws IOException {
    rawOut = Files.newOutputStream(gzDataPath);
    gzipOut = new GZIPOutputStream(rawOut);

    asyncWriter = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          if (work.size() == 0) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {}
            continue;
          }
          Optional<MarkedQueryTrace> trace = work.remove(0);
          if (!trace.isPresent()) {
            // signal to end
            return;
          }
          if (System.currentTimeMillis() >= trace.get().startTime+delayMillis) {
            try {
              writeToOutput(trace.get().trace, trace.get().startTime, gzipOut);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          } else {
            // delay hasn't been met yet, put back into work queue
            work.add(trace);
          }
        }
      }
    });
    asyncWriter.start();
  }

  private static void writeToOutput(QueryTrace t, long reqEndMillis, OutputStream out) throws IOException {
    Traceinfo.TraceInfo.Builder b =
      Traceinfo.TraceInfo.newBuilder()
        .setDurationMicros(t.getDurationMicros())
        .setReqType(t.getRequestType())
        .setReqEndTimeMillis(reqEndMillis)
        .setCoordinatorAddr(ByteString.copyFrom(t.getCoordinator().getAddress()));
    for (QueryTrace.Event e : t.getEvents()) {
      b.addEvents(
        Traceinfo.Event.newBuilder()
          .setDesc(e.getDescription())
          .setDurationMicros(e.getSourceElapsedMicros())
          .setSource(ByteString.copyFrom(e.getSource().getAddress())));
    }
    b.build().writeDelimitedTo(out);
  }

  public void write(QueryTrace t) {
    work.add(Optional.of(new MarkedQueryTrace(t)));
  }

  public void close() throws IOException {
    while (work.size() > 0) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {}
    }
    work.add(Optional.<MarkedQueryTrace>absent());
    try {
      asyncWriter.join();
    } catch (InterruptedException e) {}
    gzipOut.finish();
    rawOut.close();
  }

  private static final class MarkedQueryTrace {
    public final long startTime;
    public final QueryTrace trace;

    public MarkedQueryTrace(QueryTrace trace) {
      startTime = System.currentTimeMillis();
      this.trace = trace;
    }
  }
}
