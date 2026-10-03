package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import ai.pipestream.document.v1.Document;
import ai.pipestream.enrich.InProcessEnrich.Collected;
import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.server.EnrichServiceImpl;
import ai.pipestream.enrich.v1.DocumentChunk;
import ai.pipestream.enrich.v1.EnrichDocumentRequest;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.ItemImage;
import ai.pipestream.enrich.v1.SkipReason;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.Test;

/**
 * Memory and work bounds: the byte cap covers inline documents and crops as
 * well as chunks, crops are capped in count and refused after enrichment
 * started, the HTTP shim bounds request bodies before parsing, the VLM
 * concurrency cap is process-wide, and a caller cannot raise the per-call
 * timeout (or with it the Retry-After clamp) above the server's.
 */
class ResourceLimitsTest {

  private static ItemImage crop(String selfRef, int bytes) {
    return ItemImage.newBuilder()
        .setSelfRef(selfRef)
        .setMimetype("image/png")
        .setData(ByteString.copyFrom(new byte[bytes]))
        .build();
  }

  private static EnrichDocumentRequest image(ItemImage image) {
    return EnrichDocumentRequest.newBuilder().setImage(image).build();
  }

  private static EnrichDocumentRequest completeChunk(Document document) {
    return EnrichDocumentRequest.newBuilder()
        .setChunk(DocumentChunk.newBuilder().setData(document.toByteString()).setComplete(true))
        .build();
  }

  private static EnrichDocumentRequest describeOptions() {
    return InProcessEnrich.options(EnrichOptions.newBuilder().setDoPictureDescription(true).build());
  }

  private static InProcessEnrich withCap(String vlmUrl, long maxDocumentBytes) throws Exception {
    return InProcessEnrich.start(EndpointPolicy.defaultOnly(vlmUrl), 4, 16,
        Duration.ofSeconds(10), maxDocumentBytes);
  }

  // -------------------------------------------------------------------------
  // The byte cap covers inline documents and crops
  // -------------------------------------------------------------------------

  @Test
  void inlineDocumentOverTheByteCap_isResourceExhausted() throws Exception {
    Document document = InProcessEnrich.pictures(3);
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = withCap(vlm.url(), document.getSerializedSize() - 1)) {
      Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDocument(document)
          .build());

      assertThat(result.error()).isNotNull();
      assertThat(result.error().getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
      assertThat(result.error().getStatus().getDescription()).contains("byte cap");
      assertThat(vlm.calls()).isZero();
    }
  }

  @Test
  void inlineDocumentExactlyAtTheByteCap_isAccepted() throws Exception {
    Document document = InProcessEnrich.pictures(1);
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = withCap(vlm.url(), document.getSerializedSize())) {
      Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDocument(document)
          .build());

      assertThat(result.error()).isNull();
      assertThat(result.annotations()).hasSize(1);
    }
  }

  @Test
  void cropsOverTheByteCap_areResourceExhausted() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = withCap(vlm.url(), 4096)) {
      Collected result = enrich.run(List.of(describeOptions(),
          image(crop("#/pictures/0", 3000)),
          image(crop("#/pictures/1", 3000)),
          completeChunk(InProcessEnrich.pictures(2))));

      assertThat(result.error()).isNotNull();
      assertThat(result.error().getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
      assertThat(vlm.calls()).isZero();
    }
  }

  @Test
  void cropsPlusChunksOverTheByteCap_areResourceExhausted() throws Exception {
    Document document = InProcessEnrich.pictures(1);
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = withCap(vlm.url(), 3000 + document.getSerializedSize())) {
      Collected result = enrich.run(List.of(describeOptions(),
          image(crop("#/pictures/0", 3000)),
          completeChunk(document)));

      assertThat(result.error()).isNotNull();
      assertThat(result.error().getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
      assertThat(vlm.calls()).isZero();
    }
  }

  @Test
  void aCropSentAgainForTheSameRef_countsOnce() throws Exception {
    Document document = InProcessEnrich.pictures(1);
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = withCap(vlm.url(), 3100 + document.getSerializedSize())) {
      Collected result = enrich.run(List.of(describeOptions(),
          image(crop("#/pictures/0", 3000)),
          image(crop("#/pictures/0", 3000)),
          completeChunk(document)));

      assertThat(result.error()).as("%s", result.error()).isNull();
      assertThat(result.annotations()).hasSize(1);
    }
  }

  @Test
  void moreCropsThanTheCountCap_areResourceExhausted() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = withCap(vlm.url(), 64L * 1024 * 1024)) {
      List<EnrichDocumentRequest> requests = new ArrayList<>(EnrichServiceImpl.MAX_CROPS + 2);
      requests.add(describeOptions());
      for (int i = 0; i <= EnrichServiceImpl.MAX_CROPS; i++) {
        requests.add(image(crop("#/pictures/" + i, 0)));
      }
      Collected result = enrich.run(requests);

      assertThat(result.error()).isNotNull();
      assertThat(result.error().getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
      assertThat(result.error().getStatus().getDescription())
          .contains(String.valueOf(EnrichServiceImpl.MAX_CROPS));
    }
  }

  @Test
  void cropAfterEnrichmentStarted_isInvalidArgument() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = withCap(vlm.url(), 64L * 1024 * 1024)) {
      // Park the VLM call so the stream is still open when the crop lands.
      CountDownLatch gate = new CountDownLatch(1);
      vlm.gates = Map.of(1, gate);
      try {
        BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
        StreamObserver<EnrichDocumentRequest> requester =
            enrich.stub.enrichDocument(InProcessEnrich.inbox(inbox));
        requester.onNext(InProcessEnrich.options(EnrichOptions.newBuilder()
            .setDoPictureDescription(true)
            .setDocument(InProcessEnrich.pictures(1))
            .build()));
        await().atMost(Duration.ofSeconds(10)).until(() -> vlm.calls() == 1);
        requester.onNext(image(crop("#/pictures/0", 10)));
        requester.onCompleted();
        Collected result = InProcessEnrich.collect(inbox);

        assertThat(result.error()).isNotNull();
        assertThat(result.error().getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      } finally {
        gate.countDown();
      }
    }
  }

  // -------------------------------------------------------------------------
  // HTTP shim body limit
  // -------------------------------------------------------------------------

  /** Sends raw HTTP and returns the status line, so an early 413 is read
   * even though the server never reads the rest of the body. */
  private static String rawPost(String base, String headers, byte[] bodyPart) throws Exception {
    URI uri = URI.create(base);
    try (Socket socket = new Socket(uri.getHost(), uri.getPort())) {
      socket.setSoTimeout(10_000);
      OutputStream out = socket.getOutputStream();
      out.write(("POST /v1/enrich HTTP/1.1\r\nHost: localhost\r\n" + headers + "\r\n")
          .getBytes(StandardCharsets.US_ASCII));
      out.write(bodyPart);
      out.flush();
      InputStream in = socket.getInputStream();
      StringBuilder line = new StringBuilder();
      for (int c = in.read(); c != -1 && c != '\n'; c = in.read()) {
        line.append((char) c);
      }
      return line.toString().strip();
    }
  }

  @Test
  void httpBodyDeclaredOverTheLimit_is413WithoutReadingIt() throws Exception {
    try (InProcessEnrich enrich = withCap("", 1024)) {
      // Claims 1 GiB, sends 16 bytes: the shim must answer from the header.
      String status = rawPost(enrich.httpBase(),
          "Content-Type: application/json\r\nContent-Length: 1073741824\r\n",
          "{\"options\":{}}  ".getBytes(StandardCharsets.US_ASCII));

      assertThat(status).startsWith("HTTP/1.1 413");
    }
  }

  @Test
  void httpChunkedBodyOverTheLimit_is413() throws Exception {
    try (InProcessEnrich enrich = withCap("", 1024)) {
      // No Content-Length: the shim reads up to the limit and stops there.
      int size = (1 << 20) + 2048;
      byte[] padding = new byte[size];
      java.util.Arrays.fill(padding, (byte) ' ');
      ByteString chunked = ByteString.copyFromUtf8(Integer.toHexString(size) + "\r\n")
          .concat(ByteString.copyFrom(padding))
          .concat(ByteString.copyFromUtf8("\r\n"));
      String status = rawPost(enrich.httpBase(),
          "Content-Type: application/json\r\nTransfer-Encoding: chunked\r\n",
          chunked.toByteArray());

      assertThat(status).startsWith("HTTP/1.1 413");
    }
  }

  @Test
  void httpInlineDocumentOverTheByteCap_is413() throws Exception {
    Document document = InProcessEnrich.pictures(3);
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = withCap(vlm.url(), document.getSerializedSize() - 1)) {
      String body = "{\"options\":{\"doPictureDescription\":true,\"document\":"
          + com.google.protobuf.util.JsonFormat.printer().print(document) + "}}";
      java.net.http.HttpResponse<String> response = java.net.http.HttpClient.newHttpClient()
          .send(java.net.http.HttpRequest.newBuilder(URI.create(enrich.httpBase() + "/v1/enrich"))
                  .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body)).build(),
              java.net.http.HttpResponse.BodyHandlers.ofString());

      assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(413);
      assertThat(response.body()).contains("byte cap");
      assertThat(vlm.calls()).isZero();
    }
  }

  // -------------------------------------------------------------------------
  // Process-wide VLM concurrency
  // -------------------------------------------------------------------------

  @Test
  void vlmConcurrencyCap_isSharedByEveryRequest() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()),
            2, 2, Duration.ofSeconds(10), 64L * 1024 * 1024)) {
      CountDownLatch release = new CountDownLatch(1);
      Map<Integer, CountDownLatch> gates = new HashMap<>();
      for (int call = 1; call <= 6; call++) {
        gates.put(call, release);
      }
      vlm.gates = gates;
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setConcurrency(2)
          .setDocument(InProcessEnrich.pictures(3))
          .build();
      // Two documents at once, each allowed two calls of its own.
      List<BlockingQueue<Object>> inboxes = List.of(new LinkedBlockingQueue<>(),
          new LinkedBlockingQueue<>());
      for (BlockingQueue<Object> inbox : inboxes) {
        StreamObserver<EnrichDocumentRequest> requester =
            enrich.stub.enrichDocument(InProcessEnrich.inbox(inbox));
        requester.onNext(InProcessEnrich.options(options));
        requester.onCompleted();
      }

      await().atMost(Duration.ofSeconds(10)).until(() -> vlm.calls() == 2);
      await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
          .until(() -> vlm.calls() == 2);
      release.countDown();
      for (BlockingQueue<Object> inbox : inboxes) {
        Collected result = InProcessEnrich.collect(inbox);
        assertThat(result.error()).isNull();
        assertThat(result.annotations()).hasSize(3);
      }
      assertThat(vlm.calls()).isEqualTo(6);
    }
  }

  // -------------------------------------------------------------------------
  // The server's timeout is a ceiling
  // -------------------------------------------------------------------------

  @Test
  void callerTimeoutAboveTheServers_isClampedToIt() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()),
            4, 16, Duration.ofMillis(300), 64L * 1024 * 1024)) {
      CountDownLatch never = new CountDownLatch(1);
      vlm.gates = Map.of(1, never);
      try {
        // -1 is 4294967295 seconds on the wire; it must not stretch the
        // server's 300ms per-call timeout.
        Collected result = assertTimeoutPreemptively(Duration.ofSeconds(10),
            () -> enrich.run(EnrichOptions.newBuilder()
                .setDoPictureDescription(true)
                .setTimeoutSeconds(-1)
                .setDocument(InProcessEnrich.pictures(1))
                .build()));

        assertThat(result.skips()).singleElement()
            .satisfies(skip -> assertThat(skip.getReason())
                .isEqualTo(SkipReason.SKIP_REASON_VLM_ERROR));
      } finally {
        never.countDown();
      }
    }
  }

  @Test
  void callerTimeoutCannotLoosenTheRetryAfterClamp() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()),
            4, 16, Duration.ofMillis(200), 64L * 1024 * 1024)) {
      vlm.status = 429;
      vlm.retryAfter = "99999999";
      // With the caller's 4e9-second timeout as the clamp, the first
      // Retry-After would park the worker for three years.
      Collected result = assertTimeoutPreemptively(Duration.ofSeconds(15),
          () -> enrich.run(EnrichOptions.newBuilder()
              .setDoPictureDescription(true)
              .setTimeoutSeconds(-1)
              .setDocument(InProcessEnrich.pictures(1))
              .build()));

      assertThat(result.skips()).singleElement().satisfies(skip -> {
        assertThat(skip.getReason()).isEqualTo(SkipReason.SKIP_REASON_VLM_ERROR);
        assertThat(skip.getDetail()).contains("429");
      });
      assertThat(vlm.calls()).as("1 try + 5 retries").isEqualTo(6);
    }
  }
}
