package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.v1.EnrichDocumentRequest;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.vlm.VlmClient;
import ai.pipestream.enrich.vlm.VlmClient.VlmException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A cancelled call stops costing VLM capacity: a client cancel, an expired
 * deadline, an RPC that failed mid-flight, and an NDJSON client that hung up
 * each interrupt the VLM calls in flight and keep the queued ones from
 * starting.
 */
class CancellationTest {

  /** A VLM that never answers and counts the calls it was given and the
   * ones it saw interrupted. */
  private static final class StuckVlm {
    final AtomicInteger started = new AtomicInteger();
    final AtomicInteger interrupted = new AtomicInteger();

    VlmClient.Factory factory() {
      return endpoint -> request -> {
        started.incrementAndGet();
        try {
          Thread.sleep(Duration.ofMinutes(5));
          return "never";
        } catch (InterruptedException interrupt) {
          interrupted.incrementAndGet();
          Thread.currentThread().interrupt();
          throw new VlmException("interrupted");
        }
      };
    }
  }

  private static InProcessEnrich start(StuckVlm vlm) throws Exception {
    return InProcessEnrich.start(vlm.factory(), EndpointPolicy.defaultOnly("http://vlm.invalid"),
        4, 16, Duration.ofSeconds(30), 64L * 1024 * 1024);
  }

  private static EnrichOptions describe(int pictures, int concurrency) {
    return EnrichOptions.newBuilder()
        .setDoPictureDescription(true)
        .setConcurrency(concurrency)
        .setDocument(InProcessEnrich.pictures(pictures))
        .build();
  }

  @Test
  void clientCancel_interruptsInFlightCallsAndStartsNoMore() throws Exception {
    StuckVlm vlm = new StuckVlm();
    try (InProcessEnrich enrich = start(vlm)) {
      BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
      ClientCallStreamObserver<EnrichDocumentRequest> requester =
          (ClientCallStreamObserver<EnrichDocumentRequest>)
              enrich.stub.enrichDocument(InProcessEnrich.inbox(inbox));
      requester.onNext(InProcessEnrich.options(describe(20, 2)));
      requester.onCompleted();
      await().atMost(Duration.ofSeconds(10)).until(() -> vlm.started.get() == 2);

      requester.cancel("client gave up", null);

      await().atMost(Duration.ofSeconds(10)).until(() -> vlm.interrupted.get() == 2);
      await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
          .until(() -> vlm.started.get() == 2);
    }
  }

  @Test
  void expiredDeadline_interruptsInFlightCallsAndStartsNoMore() throws Exception {
    StuckVlm vlm = new StuckVlm();
    try (InProcessEnrich enrich = start(vlm)) {
      BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
      StreamObserver<EnrichDocumentRequest> requester =
          enrich.stub.withDeadlineAfter(500, TimeUnit.MILLISECONDS)
              .enrichDocument(InProcessEnrich.inbox(inbox));
      requester.onNext(InProcessEnrich.options(describe(20, 3)));
      requester.onCompleted();

      InProcessEnrich.Collected result = InProcessEnrich.collect(inbox);
      assertThat(result.error()).isNotNull();
      assertThat(result.error().getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);
      await().atMost(Duration.ofSeconds(10)).until(() -> vlm.interrupted.get() == 3);
      await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
          .until(() -> vlm.started.get() == 3);
    }
  }

  @Test
  void clientCancel_realVlmCallCountStopsGrowing() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      CountDownLatch release = new CountDownLatch(1);
      Map<Integer, CountDownLatch> gates = new HashMap<>();
      for (int call = 1; call <= 20; call++) {
        gates.put(call, release);
      }
      vlm.gates = gates;
      try {
        BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
        ClientCallStreamObserver<EnrichDocumentRequest> requester =
            (ClientCallStreamObserver<EnrichDocumentRequest>)
                enrich.stub.enrichDocument(InProcessEnrich.inbox(inbox));
        requester.onNext(InProcessEnrich.options(describe(20, 2)));
        requester.onCompleted();
        await().atMost(Duration.ofSeconds(10)).until(() -> vlm.calls() == 2);

        requester.cancel("client gave up", null);
        // Letting the parked calls answer must not free slots for the rest.
        release.countDown();

        await().during(Duration.ofMillis(700)).atMost(Duration.ofSeconds(3))
            .until(() -> vlm.calls() == 2);
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  void failedRpc_stopsItsVlmWork() throws Exception {
    StuckVlm vlm = new StuckVlm();
    try (InProcessEnrich enrich = start(vlm)) {
      BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
      StreamObserver<EnrichDocumentRequest> requester =
          enrich.stub.enrichDocument(InProcessEnrich.inbox(inbox));
      requester.onNext(InProcessEnrich.options(describe(20, 2)));
      await().atMost(Duration.ofSeconds(10)).until(() -> vlm.started.get() == 2);
      // A second options message is a protocol error that fails the RPC.
      requester.onNext(InProcessEnrich.options(describe(1, 1)));
      requester.onCompleted();

      InProcessEnrich.Collected result = InProcessEnrich.collect(inbox);
      assertThat(result.error()).isNotNull();
      assertThat(result.error().getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      await().atMost(Duration.ofSeconds(10)).until(() -> vlm.interrupted.get() == 2);
      await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
          .until(() -> vlm.started.get() == 2);
    }
  }

  @Test
  void ndjsonClientHangingUp_stopsVlmCalls() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      // Every call after the first waits for its own release, so the test
      // decides when each later event is written.
      Map<Integer, CountDownLatch> gates = new HashMap<>();
      for (int call = 2; call <= 20; call++) {
        gates.put(call, new CountDownLatch(1));
      }
      vlm.gates = gates;
      try {
        String body = "{\"options\":" + JsonFormat.printer().print(describe(20, 1)) + "}";
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        URI base = URI.create(enrich.httpBase());
        try (Socket socket = new Socket(base.getHost(), base.getPort())) {
          socket.setSoTimeout(10_000);
          OutputStream out = socket.getOutputStream();
          out.write(("POST /v1/enrich/stream HTTP/1.1\r\nHost: localhost\r\n"
              + "Content-Type: application/json\r\nContent-Length: " + bytes.length + "\r\n\r\n")
              .getBytes(StandardCharsets.US_ASCII));
          out.write(bytes);
          out.flush();
          // Read until the first annotation line, then hang up.
          InputStream in = socket.getInputStream();
          StringBuilder seen = new StringBuilder();
          while (!seen.toString().contains("\"annotation\"")) {
            int c = in.read();
            assertThat(c).as("stream ended before the first annotation").isNotEqualTo(-1);
            seen.append((char) c);
          }
        }
        // Release later calls one at a time: each answer is an event write to
        // a closed connection, and once one fails the call is cancelled, so
        // the calls stop arriving long before all 20 have run.
        for (int call = 2; call <= 20; call++) {
          int next = call;
          boolean arrived;
          try {
            await().atMost(Duration.ofSeconds(2)).until(() -> vlm.calls() >= next);
            arrived = true;
          } catch (org.awaitility.core.ConditionTimeoutException stopped) {
            arrived = false;
          }
          if (!arrived) {
            break;
          }
          gates.get(call).countDown();
        }
        assertThat(vlm.calls()).as("VLM calls kept running after the client left")
            .isLessThan(20);
      } finally {
        gates.values().forEach(CountDownLatch::countDown);
      }
    }
  }
}
