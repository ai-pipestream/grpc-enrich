package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.enrich.engine.EnrichmentEngine;
import ai.pipestream.enrich.server.EnrichHttpServer;
import ai.pipestream.enrich.server.EnrichServiceImpl;
import ai.pipestream.enrich.vlm.OpenAiCompatVlmClient;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * HTTP front-end request-envelope and method-guard behavior: wrong methods
 * are 405 with a JSON error body, the two document positions are mutually
 * exclusive, item_images is accepted in both key spellings and actually
 * applies crops, and snake_case option keys are honored end to end.
 */
class EnrichHttpEnvelopeTest {

  private static final byte[] PNG_BYTES = new byte[] {(byte) 0x89, 'P', 'N', 'G', 9, 8, 7, 6};

  private final List<AutoCloseable> cleanups = new ArrayList<>();
  private final HttpClient http = HttpClient.newHttpClient();

  @AfterEach
  void tearDown() throws Exception {
    for (AutoCloseable cleanup : cleanups) {
      cleanup.close();
    }
  }

  private String startHttp(String vlmUrl) throws Exception {
    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    EnrichmentEngine engine = new EnrichmentEngine(
        endpoint -> new OpenAiCompatVlmClient(endpoint, Duration.ofMillis(5)), vlmUrl,
        4, 16, Duration.ofSeconds(10), executor);
    EnrichServiceImpl service =
        new EnrichServiceImpl(64L * 1024 * 1024, engine, executor, vlmUrl, 16);
    EnrichHttpServer httpServer = new EnrichHttpServer(0, service, executor);
    httpServer.start();
    cleanups.add(() -> {
      httpServer.close();
      executor.shutdownNow();
    });
    return "http://127.0.0.1:" + httpServer.getPort();
  }

  private HttpResponse<String> post(String url, String body) throws Exception {
    return http.send(HttpRequest.newBuilder(URI.create(url))
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> get(String url) throws Exception {
    return http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
        HttpResponse.BodyHandlers.ofString());
  }

  // -------------------------------------------------------------------------
  // Method guards
  // -------------------------------------------------------------------------

  @Test
  void wrongMethods_are405WithJsonError() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      String base = startHttp(vlm.url());

      for (String path : List.of("/v1/enrich", "/v1/enrich/stream")) {
        HttpResponse<String> response = get(base + path);
        assertThat(response.statusCode()).as("GET %s", path).isEqualTo(405);
        assertThat(response.headers().firstValue("Content-Type"))
            .contains("application/json");
        assertThat(JsonParser.parseString(response.body()).getAsJsonObject()
            .get("error").getAsString()).contains("POST");
      }

      HttpResponse<String> health = post(base + "/healthz", "{}");
      assertThat(health.statusCode()).as("POST /healthz").isEqualTo(405);
      assertThat(JsonParser.parseString(health.body()).getAsJsonObject()
          .get("error").getAsString()).contains("GET");
    }
  }

  // -------------------------------------------------------------------------
  // Envelope validation
  // -------------------------------------------------------------------------

  @Test
  void documentInBothPositions_is400() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      String base = startHttp(vlm.url());
      String body = "{\"options\":{\"doPictureDescription\":true,"
          + "\"document\":{\"name\":\"inline\"}},"
          + "\"document\":{\"name\":\"top-level\"}}";
      HttpResponse<String> response = post(base + "/v1/enrich", body);
      assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(400);
      assertThat(response.body()).contains("both");
      assertThat(vlm.requests).as("no VLM call may happen for a rejected envelope").isEmpty();
    }
  }

  @Test
  void itemImagesNotAnArray_is400() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      String base = startHttp(vlm.url());
      String body = "{\"options\":{\"doPictureDescription\":true},"
          + "\"document\":{\"name\":\"doc\"},"
          + "\"item_images\":{\"selfRef\":\"#/pictures/0\"}}";
      HttpResponse<String> response = post(base + "/v1/enrich", body);
      assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(400);
      assertThat(response.body()).contains("item_images");
    }
  }

  @Test
  void bodyThatIsNotAnObject_is400() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      String base = startHttp(vlm.url());
      HttpResponse<String> response = post(base + "/v1/enrich", "[1,2,3]");
      assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(400);
      assertThat(response.body()).contains("error");
    }
  }

  // -------------------------------------------------------------------------
  // Both key spellings, applied for real
  // -------------------------------------------------------------------------

  @Test
  void snakeCaseItemImages_cropsApply() throws Exception {
    assertCropApplies("item_images");
  }

  @Test
  void camelCaseItemImages_cropsApply() throws Exception {
    assertCropApplies("itemImages");
  }

  private void assertCropApplies(String key) throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "a crop-fed description";
      String base = startHttp(vlm.url());
      String crop = Base64.getEncoder().encodeToString(PNG_BYTES);
      // snake_case option key too: the whole envelope must accept proto3
      // JSON's alternate spelling.
      String body = "{\"options\":{\"do_picture_description\":true},"
          + "\"document\":{\"name\":\"doc\",\"pictures\":[{\"selfRef\":\"#/pictures/0\","
          + "\"label\":\"DOC_ITEM_LABEL_PICTURE\"}]},"
          + "\"" + key + "\":[{\"selfRef\":\"#/pictures/0\",\"data\":\"" + crop + "\","
          + "\"mimetype\":\"image/png\"}]}";
      HttpResponse<String> response = post(base + "/v1/enrich", body);

      assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(200);
      JsonArray events =
          JsonParser.parseString(response.body()).getAsJsonObject().getAsJsonArray("events");
      assertThat(events.size()).as("%s", response.body()).isEqualTo(3);
      JsonObject annotation = events.get(1).getAsJsonObject().getAsJsonObject("annotation");
      assertThat(annotation).as("the crop must make the picture describable").isNotNull();
      assertThat(annotation.get("selfRef").getAsString()).isEqualTo("#/pictures/0");
      assertThat(vlm.requests).hasSize(1);
      assertThat(vlm.requests.get(0).hasImage())
          .as("the VLM call must carry the crop's image bytes").isTrue();
    }
  }

  @Test
  void withoutCrop_pictureWithoutImageIsSkipped() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      String base = startHttp(vlm.url());
      String body = "{\"options\":{\"doPictureDescription\":true,"
          + "\"document\":{\"name\":\"doc\",\"pictures\":[{\"selfRef\":\"#/pictures/0\","
          + "\"label\":\"DOC_ITEM_LABEL_PICTURE\"}]}}}";
      HttpResponse<String> response = post(base + "/v1/enrich", body);

      assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(200);
      JsonArray events =
          JsonParser.parseString(response.body()).getAsJsonObject().getAsJsonArray("events");
      JsonObject skipped = events.get(1).getAsJsonObject().getAsJsonObject("skipped");
      assertThat(skipped).as("%s", response.body()).isNotNull();
      assertThat(skipped.get("reason").getAsString()).isEqualTo("SKIP_REASON_NO_IMAGE");
      assertThat(vlm.requests).isEmpty();
    }
  }
}
