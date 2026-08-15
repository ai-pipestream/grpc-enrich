package ai.pipestream.enrich;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.pipestream.document.v1.DocItemLabel;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.ImageRef;
import ai.pipestream.document.v1.PictureItem;
import ai.pipestream.document.v1.Size;
import ai.pipestream.document.v1.TextItemBase;
import ai.pipestream.enrich.engine.EnrichmentEngine;
import ai.pipestream.enrich.server.EnrichHttpServer;
import ai.pipestream.enrich.server.EnrichServiceImpl;
import ai.pipestream.enrich.v1.EnrichDocumentRequest;
import ai.pipestream.enrich.v1.EnrichDocumentResponse;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.EnrichServiceGrpc;
import ai.pipestream.enrich.v1.PictureDescriptionPreset;
import ai.pipestream.enrich.vlm.OpenAiCompatVlmClient;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests of the HTTP front end: the sync endpoint collects the stream into one
 * events array, the NDJSON endpoint flushes each event as the stream produces
 * it (proven against a gated fake VLM call), malformed input maps to 400, the
 * byte cap maps to 413, and the HTTP shim coexists with live gRPC traffic on
 * the same service instance.
 */
class EnrichHttpServerTest {

  private static final byte[] PNG_BYTES = new byte[] {(byte) 0x89, 'P', 'N', 'G', 1, 2, 3, 4};
  private static final String PNG_DATA_URI =
      "data:image/png;base64," + Base64.getEncoder().encodeToString(PNG_BYTES);

  private final List<AutoCloseable> cleanups = new ArrayList<>();
  private final HttpClient http = HttpClient.newHttpClient();

  @AfterEach
  void tearDown() throws Exception {
    for (AutoCloseable cleanup : cleanups) {
      cleanup.close();
    }
  }

  // -------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------

  private static PictureItem pictureWithImage(String selfRef) {
    return PictureItem.newBuilder()
        .setSelfRef(selfRef)
        .setLabel(DocItemLabel.DOC_ITEM_LABEL_PICTURE)
        .setImage(ImageRef.newBuilder()
            .setMimetype("image/png")
            .setDpi(72)
            .setSize(Size.newBuilder().setWidth(8).setHeight(8))
            .setUri(PNG_DATA_URI))
        .build();
  }

  private static Document documentOnePictureOneParagraph() {
    return Document.newBuilder()
        .setName("test")
        .addPictures(pictureWithImage("#/pictures/0"))
        .addTexts(ai.pipestream.document.v1.BaseTextItem.newBuilder()
            .setText(ai.pipestream.document.v1.TextItem.newBuilder()
                .setBase(TextItemBase.newBuilder()
                    .setSelfRef("#/texts/0")
                    .setLabel(DocItemLabel.DOC_ITEM_LABEL_PARAGRAPH)
                    .setText("a paragraph"))))
        .build();
  }

  private static EnrichOptions describeOptions(Document document) {
    return EnrichOptions.newBuilder()
        .setDoPictureDescription(true)
        .setPictureDescriptionPreset(PictureDescriptionPreset.PICTURE_DESCRIPTION_PRESET_SMOLVLM)
        .setDocument(document)
        .build();
  }

  /** A service plus its HTTP front end on an ephemeral port. */
  private record Fixture(EnrichServiceImpl service, EnrichHttpServer httpServer)
      implements AutoCloseable {
    String base() {
      return "http://127.0.0.1:" + httpServer.getPort();
    }

    @Override
    public void close() {
      httpServer.close();
    }
  }

  private Fixture startHttp(String vlmUrl) throws Exception {
    return startHttp(vlmUrl, 64L * 1024 * 1024);
  }

  private Fixture startHttp(String vlmUrl, long maxDocumentBytes) throws Exception {
    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    EnrichmentEngine engine = new EnrichmentEngine(
        endpoint -> new OpenAiCompatVlmClient(endpoint, Duration.ofMillis(5)), vlmUrl,
        4, 16, Duration.ofSeconds(10), executor);
    EnrichServiceImpl service =
        new EnrichServiceImpl(maxDocumentBytes, engine, executor, vlmUrl, 16);
    EnrichHttpServer httpServer = new EnrichHttpServer(0, service, executor);
    httpServer.start();
    cleanups.add(() -> {
      httpServer.close();
      executor.shutdownNow();
    });
    return new Fixture(service, httpServer);
  }

  private static String envelope(EnrichOptions options) {
    try {
      return "{\"options\":" + JsonFormat.printer().print(options) + "}";
    } catch (InvalidProtocolBufferException failure) {
      throw new IllegalStateException(failure);
    }
  }

  private static String envelope(EnrichOptions options, Document document) {
    try {
      return "{\"options\":" + JsonFormat.printer().print(options)
          + ",\"document\":" + JsonFormat.printer().print(document) + "}";
    } catch (InvalidProtocolBufferException failure) {
      throw new IllegalStateException(failure);
    }
  }

  private HttpResponse<String> post(String url, String body) throws Exception {
    return http.send(HttpRequest.newBuilder(URI.create(url))
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
        HttpResponse.BodyHandlers.ofString());
  }

  // -------------------------------------------------------------------------
  // Sync endpoint
  // -------------------------------------------------------------------------

  @Test
  void sync_happyPathEventsInOrder() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "a QR code on a white background";
      Fixture fixture = startHttp(vlm.url());

      HttpResponse<String> response =
          post(fixture.base() + "/v1/enrich", envelope(describeOptions(documentOnePictureOneParagraph())));

      assertEquals(200, response.statusCode(), response.body());
      JsonArray events =
          JsonParser.parseString(response.body()).getAsJsonObject().getAsJsonArray("events");
      assertEquals(3, events.size(), response.body());
      assertTrue(events.get(0).getAsJsonObject().has("started"), "first event is EnrichStarted");
      assertEquals(1, events.get(0).getAsJsonObject().getAsJsonObject("started")
          .get("pictureDescriptions").getAsInt());
      JsonObject annotation = events.get(1).getAsJsonObject().getAsJsonObject("annotation");
      assertNotNull(annotation, "second event is the ItemAnnotation");
      assertEquals("#/pictures/0", annotation.get("selfRef").getAsString());
      assertEquals("a QR code on a white background",
          annotation.getAsJsonObject("description").get("text").getAsString());
      JsonObject complete = events.get(2).getAsJsonObject().getAsJsonObject("complete");
      assertNotNull(complete, "last event is the EnrichComplete trailer");
      assertEquals(1, complete.get("succeeded").getAsInt());
      assertEquals(1, vlm.requests.size());
    }
  }

  @Test
  void sync_malformedJson_400() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      Fixture fixture = startHttp(vlm.url());
      HttpResponse<String> response = post(fixture.base() + "/v1/enrich", "{not json");
      assertEquals(400, response.statusCode());
      assertTrue(response.body().contains("\"error\""), response.body());
    }
  }

  @Test
  void sync_missingDocument_400() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      Fixture fixture = startHttp(vlm.url());
      HttpResponse<String> response =
          post(fixture.base() + "/v1/enrich", "{\"options\":{\"doPictureDescription\":true}}");
      assertEquals(400, response.statusCode(), response.body());
      assertTrue(response.body().contains("without a document"), response.body());
    }
  }

  @Test
  void sync_overByteCap_413() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      Fixture fixture = startHttp(vlm.url(), 64);
      // The top-level document goes the chunk route, where the cap applies.
      HttpResponse<String> response = post(fixture.base() + "/v1/enrich",
          envelope(EnrichOptions.newBuilder().setDoPictureDescription(true).build(),
              documentOnePictureOneParagraph()));
      assertEquals(413, response.statusCode(), response.body());
      assertTrue(response.body().contains("byte cap"), response.body());
    }
  }

  // -------------------------------------------------------------------------
  // Async NDJSON endpoint
  // -------------------------------------------------------------------------

  /** Reads one line, failing when none arrives before the deadline. */
  private static String readLine(BufferedReader reader, long deadlineNanos, String what)
      throws Exception {
    while (System.nanoTime() < deadlineNanos) {
      if (reader.ready()) {
        String line = reader.readLine();
        assertNotNull(line, "stream ended before " + what);
        return line;
      }
      Thread.sleep(5);
    }
    throw new AssertionError("no line arrived within the deadline waiting for " + what);
  }

  @Test
  void stream_ndjsonFlushesPerEvent() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      CountDownLatch secondCallGate = new CountDownLatch(1);
      vlm.gates = Map.of(2, secondCallGate);
      vlm.responder = body -> "caption";
      Fixture fixture = startHttp(vlm.url());

      Document document = Document.newBuilder()
          .setName("test")
          .addPictures(pictureWithImage("#/pictures/0"))
          .addPictures(pictureWithImage("#/pictures/1"))
          .build();
      EnrichOptions options = describeOptions(document).toBuilder().setConcurrency(2).build();

      HttpResponse<InputStream> response = http.send(
          HttpRequest.newBuilder(URI.create(fixture.base() + "/v1/enrich/stream"))
              .POST(HttpRequest.BodyPublishers.ofString(envelope(options))).build(),
          HttpResponse.BodyHandlers.ofInputStream());
      assertEquals(200, response.statusCode());

      BufferedReader reader = new BufferedReader(
          new InputStreamReader(response.body(), StandardCharsets.UTF_8));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      String started = readLine(reader, deadline, "EnrichStarted");
      assertTrue(started.contains("\"started\""), started);

      // The second VLM call is still gated, yet the first item's annotation
      // line must already be flushed to the HTTP client.
      String firstAnnotation = readLine(reader, deadline, "the first ItemAnnotation");
      assertTrue(firstAnnotation.contains("\"annotation\""), firstAnnotation);
      assertEquals(1, secondCallGate.getCount(),
          "the later VLM call must still be blocked when the first line arrived");
      secondCallGate.countDown();

      String secondAnnotation = readLine(reader, deadline, "the second ItemAnnotation");
      assertTrue(secondAnnotation.contains("\"annotation\""), secondAnnotation);
      String complete = readLine(reader, deadline, "EnrichComplete");
      assertTrue(complete.contains("\"complete\""), complete);
      assertTrue(complete.contains("\"succeeded\":2")
          || complete.contains("\"succeeded\":\"2\""), complete);
    }
  }

  // -------------------------------------------------------------------------
  // healthz and coexistence with gRPC
  // -------------------------------------------------------------------------

  @Test
  void healthz_ok() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      Fixture fixture = startHttp(vlm.url());
      HttpResponse<String> response = http.send(
          HttpRequest.newBuilder(URI.create(fixture.base() + "/healthz")).GET().build(),
          HttpResponse.BodyHandlers.ofString());
      assertEquals(200, response.statusCode());
      assertEquals("ok", response.body());
    }
  }

  @Test
  void grpcAndHttp_concurrently() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "shared fake caption";
      Fixture fixture = startHttp(vlm.url());

      // gRPC on an in-process transport over the very same service instance.
      String name = InProcessServerBuilder.generateName();
      Server grpcServer = InProcessServerBuilder.forName(name)
          .directExecutor().addService(fixture.service()).build().start();
      ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
      cleanups.add(() -> {
        channel.shutdownNow();
        grpcServer.shutdownNow();
      });
      EnrichServiceGrpc.EnrichServiceStub stub = EnrichServiceGrpc.newStub(channel);

      // The HTTP request runs on a second thread while this thread drives gRPC.
      var httpResult = new java.util.concurrent.FutureTask<>(() ->
          post(fixture.base() + "/v1/enrich",
              envelope(describeOptions(documentOnePictureOneParagraph()))));
      Thread httpThread = new Thread(httpResult, "http-request");
      httpThread.start();

      List<EnrichDocumentResponse> grpcEvents = new ArrayList<>();
      BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
      StreamObserver<EnrichDocumentResponse> observer = new StreamObserver<>() {
        @Override
        public void onNext(EnrichDocumentResponse event) {
          grpcEvents.add(event);
        }

        @Override
        public void onError(Throwable error) {
          inbox.add(error);
        }

        @Override
        public void onCompleted() {
          inbox.add("DONE");
        }
      };
      StreamObserver<EnrichDocumentRequest> requester = stub.enrichDocument(observer);
      requester.onNext(EnrichDocumentRequest.newBuilder()
          .setOptions(describeOptions(documentOnePictureOneParagraph())).build());
      requester.onCompleted();
      Object terminal = inbox.poll(30, TimeUnit.SECONDS);
      assertEquals("DONE", terminal, "gRPC stream failed: " + terminal);

      HttpResponse<String> httpResponse = httpResult.get(30, TimeUnit.SECONDS);
      httpThread.join();
      assertEquals(200, httpResponse.statusCode(), httpResponse.body());

      assertTrue(grpcEvents.stream().anyMatch(EnrichDocumentResponse::hasAnnotation),
          "gRPC stream annotated its picture");
      assertTrue(httpResponse.body().contains("\"annotation\""),
          "HTTP request annotated its picture");
      assertTrue(grpcEvents.stream().anyMatch(EnrichDocumentResponse::hasComplete));
      assertEquals(2, vlm.requests.size(), "both paths called the VLM exactly once each");
    }
  }
}
