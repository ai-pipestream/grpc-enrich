package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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

      HttpResponse<String> response = post(fixture.base() + "/v1/enrich",
          envelope(describeOptions(documentOnePictureOneParagraph())));

      assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(200);
      JsonArray events =
          JsonParser.parseString(response.body()).getAsJsonObject().getAsJsonArray("events");
      assertThat(events.size()).as("%s", response.body()).isEqualTo(3);
      assertThat(events.get(0).getAsJsonObject().has("started")).as("first event is EnrichStarted")
          .isTrue();
      assertThat(events.get(0).getAsJsonObject().getAsJsonObject("started")
          .get("pictureDescriptions").getAsInt()).isEqualTo(1);
      JsonObject annotation = events.get(1).getAsJsonObject().getAsJsonObject("annotation");
      assertThat(annotation).as("second event is the ItemAnnotation").isNotNull();
      assertThat(annotation.get("selfRef").getAsString()).isEqualTo("#/pictures/0");
      assertThat(annotation.getAsJsonObject("description").get("text").getAsString())
          .isEqualTo("a QR code on a white background");
      JsonObject complete = events.get(2).getAsJsonObject().getAsJsonObject("complete");
      assertThat(complete).as("last event is the EnrichComplete trailer").isNotNull();
      assertThat(complete.get("succeeded").getAsInt()).isEqualTo(1);
      assertThat(vlm.requests.size()).isEqualTo(1);
    }
  }

  @Test
  void sync_malformedJson_400() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      Fixture fixture = startHttp(vlm.url());
      HttpResponse<String> response = post(fixture.base() + "/v1/enrich", "{not json");
      assertThat(response.statusCode()).isEqualTo(400);
      assertThat(response.body()).as("%s", response.body()).contains("\"error\"");
    }
  }

  @Test
  void sync_missingDocument_400() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      Fixture fixture = startHttp(vlm.url());
      HttpResponse<String> response =
          post(fixture.base() + "/v1/enrich", "{\"options\":{\"doPictureDescription\":true}}");
      assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(400);
      assertThat(response.body()).as("%s", response.body()).contains("without a document");
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
      assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(413);
      assertThat(response.body()).as("%s", response.body()).contains("byte cap");
    }
  }

  // -------------------------------------------------------------------------
  // Async NDJSON endpoint
  // -------------------------------------------------------------------------

  /** Reads one line, failing when none arrives before the deadline. */
  private static String readLine(BufferedReader reader, long deadlineNanos, String what)
      throws Exception {
    await("a line for " + what)
        .atMost(Duration.ofNanos(Math.max(1, deadlineNanos - System.nanoTime())))
        .pollDelay(Duration.ZERO)
        .pollInterval(Duration.ofMillis(5))
        .until(reader::ready);
    String line = reader.readLine();
    assertThat(line).as("stream ended before " + what).isNotNull();
    return line;
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
      assertThat(response.statusCode()).isEqualTo(200);

      BufferedReader reader = new BufferedReader(
          new InputStreamReader(response.body(), StandardCharsets.UTF_8));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      String started = readLine(reader, deadline, "EnrichStarted");
      assertThat(started).as("%s", started).contains("\"started\"");

      // The second VLM call is still gated, yet the first item's annotation
      // line must already be flushed to the HTTP client.
      String firstAnnotation = readLine(reader, deadline, "the first ItemAnnotation");
      assertThat(firstAnnotation).as("%s", firstAnnotation).contains("\"annotation\"");
      assertThat(secondCallGate.getCount())
          .as("the later VLM call must still be blocked when the first line arrived").isEqualTo(1);
      secondCallGate.countDown();

      String secondAnnotation = readLine(reader, deadline, "the second ItemAnnotation");
      assertThat(secondAnnotation).as("%s", secondAnnotation).contains("\"annotation\"");
      String complete = readLine(reader, deadline, "EnrichComplete");
      assertThat(complete).as("%s", complete).contains("\"complete\"");
      assertThat(complete).as("%s", complete)
          .containsAnyOf("\"succeeded\":2", "\"succeeded\":\"2\"");
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
      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(response.body()).isEqualTo("ok");
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
      assertThat(terminal).as("gRPC stream failed: " + terminal).isEqualTo("DONE");

      HttpResponse<String> httpResponse = httpResult.get(30, TimeUnit.SECONDS);
      httpThread.join();
      assertThat(httpResponse.statusCode()).as("%s", httpResponse.body()).isEqualTo(200);

      assertThat(grpcEvents.stream().anyMatch(EnrichDocumentResponse::hasAnnotation))
          .as("gRPC stream annotated its picture").isTrue();
      assertThat(httpResponse.body()).as("HTTP request annotated its picture")
          .contains("\"annotation\"");
      assertThat(grpcEvents.stream().anyMatch(EnrichDocumentResponse::hasComplete)).isTrue();
      assertThat(vlm.requests.size()).as("both paths called the VLM exactly once each")
          .isEqualTo(2);
    }
  }
}
