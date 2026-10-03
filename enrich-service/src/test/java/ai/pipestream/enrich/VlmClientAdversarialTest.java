package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import ai.pipestream.enrich.vlm.Json;
import ai.pipestream.enrich.vlm.OpenAiCompatVlmClient;
import ai.pipestream.enrich.vlm.VlmClient.Header;
import ai.pipestream.enrich.vlm.VlmClient.VlmException;
import ai.pipestream.enrich.vlm.VlmClient.VlmImage;
import ai.pipestream.enrich.vlm.VlmClient.VlmRequest;
import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Adversarial tests for the OpenAI-compatible VLM client against a raw HTTP
 * server that can answer with any bytes: truncated bodies, error-shaped 200s,
 * empty choices, hostile Retry-After headers, and odd endpoint URLs.
 */
class VlmClientAdversarialTest {

  /** A raw OpenAI-compat endpoint: the test decides status, headers, body. */
  private static final class RawVlmServer implements AutoCloseable {
    final HttpServer server;
    final List<String> paths = new ArrayList<>();
    final List<String> queries = new ArrayList<>();
    final List<String> requestBodies = new ArrayList<>();
    final AtomicInteger calls = new AtomicInteger();
    volatile int status = 200;
    volatile String body = "";
    volatile String retryAfter;
    /** When positive, the body is sent this many bytes at a time, one chunk
     * every {@code dripMillis}. */
    volatile int dripBytes;
    volatile long dripMillis;

    RawVlmServer() throws IOException {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext("/", exchange -> {
        calls.incrementAndGet();
        String request = new String(exchange.getRequestBody().readAllBytes(),
            StandardCharsets.UTF_8);
        synchronized (paths) {
          paths.add(exchange.getRequestURI().getPath());
          queries.add(exchange.getRequestURI().getRawQuery());
          requestBodies.add(request);
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        if (retryAfter != null) {
          exchange.getResponseHeaders().set("Retry-After", retryAfter);
        }
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
          if (dripBytes <= 0) {
            out.write(bytes);
            return;
          }
          for (int at = 0; at < bytes.length; at += dripBytes) {
            out.write(bytes, at, Math.min(dripBytes, bytes.length - at));
            out.flush();
            Thread.sleep(dripMillis);
          }
        } catch (InterruptedException | IOException gone) {
          // The client hung up mid-drip; nothing left to send.
        }
      });
      server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
      server.start();
    }

    String url() {
      return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }

  private static String chatBody(String contentJson) {
    return "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":" + contentJson + "}}]}";
  }

  // -------------------------------------------------------------------------
  // Malformed 200 bodies: every one must surface as VlmException
  // -------------------------------------------------------------------------

  @Test
  void truncated200Body_throwsVlmException() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = "{\"choices\":[]";
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      assertThatThrownBy(() -> client.complete("m", "p", null, 10, Duration.ofSeconds(5)))
          .isInstanceOf(VlmException.class);
    }
  }

  @Test
  void emptyBody200_throwsVlmException() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = "";
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      assertThatThrownBy(() -> client.complete("m", "p", null, 10, Duration.ofSeconds(5)))
          .isInstanceOf(VlmException.class);
    }
  }

  @Test
  void errorShaped200_throwsVlmException() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = "{\"error\":{\"message\":\"model not loaded\"}}";
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      VlmException error = catchThrowableOfType(VlmException.class,
          () -> client.complete("m", "p", null, 10, Duration.ofSeconds(5)));
      assertThat(error.getMessage()).contains("model not loaded");
    }
  }

  @Test
  void emptyChoicesArray_throwsVlmException() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = "{\"choices\":[]}";
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      assertThatThrownBy(() -> client.complete("m", "p", null, 10, Duration.ofSeconds(5)))
          .isInstanceOf(VlmException.class);
    }
  }

  @Test
  void missingMessage_throwsVlmException() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = "{\"choices\":[{\"index\":0}]}";
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      assertThatThrownBy(() -> client.complete("m", "p", null, 10, Duration.ofSeconds(5)))
          .isInstanceOf(VlmException.class);
    }
  }

  @Test
  void nullContent_throwsVlmException() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":null}}]}";
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      assertThatThrownBy(() -> client.complete("m", "p", null, 10, Duration.ofSeconds(5)))
          .isInstanceOf(VlmException.class);
    }
  }

  @Test
  void arrayBody200_throwsVlmException() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = "[{\"choices\":[]}]";
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      assertThatThrownBy(() -> client.complete("m", "p", null, 10, Duration.ofSeconds(5)))
          .isInstanceOf(VlmException.class);
    }
  }

  @Test
  void unicodeAndNewlinesInContent_survive() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = chatBody("\"line one\\nline two — héllo 😀\\tend\"");
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      assertThat(client.complete("m", "p", null, 10, Duration.ofSeconds(5)))
          .isEqualTo("line one\nline two — héllo 😀\tend");
    }
  }

  // -------------------------------------------------------------------------
  // Retry-After handling
  // -------------------------------------------------------------------------

  @Test
  void nonNumericRetryAfter_fallsBackToBackoff() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.retryAfter = "not-a-number";
      vlm.status = 429;
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      VlmException error = catchThrowableOfType(VlmException.class,
          () -> client.complete("m", "p", null, 10, Duration.ofSeconds(5)));
      assertThat(error.getMessage()).contains("429");
      assertThat(vlm.calls.get()).isEqualTo(6); // 1 try + 5 retries
    }
  }

  @Test
  void hugeRetryAfter_isClampedNotSleptForDays() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.retryAfter = "100000000000"; // ~3170 years of seconds
      vlm.status = 429;
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      // With a hostile Retry-After honored verbatim this call sleeps forever.
      // The client must clamp the wait (to no more than the per-call timeout)
      // and give up after the usual retries.
      assertTimeoutPreemptively(Duration.ofSeconds(10),
          () -> assertThatThrownBy(
              () -> client.complete("m", "p", null, 10, Duration.ofMillis(50)))
              .isInstanceOf(VlmException.class));
    }
  }

  @Test
  void retryAfterHonored_whenReasonable() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.retryAfter = "0";
      vlm.status = 429;
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      assertThatThrownBy(() -> client.complete("m", "p", null, 10, Duration.ofSeconds(5)))
          .isInstanceOf(VlmException.class);
      assertThat(vlm.calls.get()).isEqualTo(6);
    }
  }

  // -------------------------------------------------------------------------
  // Endpoint URL shapes
  // -------------------------------------------------------------------------

  @Test
  void endpointTrailingSlash_noDoubleSlash() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = chatBody("\"ok\"");
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url() + "/", Duration.ofMillis(1));
      assertThat(client.complete("m", "p", null, 10, Duration.ofSeconds(5))).isEqualTo("ok");
      assertThat(vlm.paths.get(0)).isEqualTo("/v1/chat/completions");
    }
  }

  @Test
  void endpointAlreadyCompletionsPath_usedVerbatim() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = chatBody("\"ok\"");
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url() + "/v1/chat/completions", Duration.ofMillis(1));
      assertThat(client.complete("m", "p", null, 10, Duration.ofSeconds(5))).isEqualTo("ok");
      assertThat(vlm.paths.get(0)).isEqualTo("/v1/chat/completions");
    }
  }

  @Test
  void endpointWithBasePath_completionsAppended() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = chatBody("\"ok\"");
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url() + "/models/llama", Duration.ofMillis(1));
      assertThat(client.complete("m", "p", null, 10, Duration.ofSeconds(5))).isEqualTo("ok");
      assertThat(vlm.paths.get(0)).isEqualTo("/models/llama/v1/chat/completions");
    }
  }

  @Test
  void endpointFullUrlWithQuery_usedVerbatim() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = chatBody("\"ok\"");
      OpenAiCompatVlmClient client = new OpenAiCompatVlmClient(
          vlm.url() + "/openai/deployments/gpt-4o/chat/completions?api-version=2024-10-21",
          Duration.ofMillis(1));
      assertThat(client.complete("m", "p", null, 10, Duration.ofSeconds(5))).isEqualTo("ok");
      assertThat(vlm.paths.get(0)).isEqualTo("/openai/deployments/gpt-4o/chat/completions");
      assertThat(vlm.queries.get(0)).isEqualTo("api-version=2024-10-21");
    }
  }

  @Test
  void endpointNonV1CompletionsPath_usedVerbatim() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = chatBody("\"ok\"");
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url() + "/v3/chat/completions", Duration.ofMillis(1));
      assertThat(client.complete("m", "p", null, 10, Duration.ofSeconds(5))).isEqualTo("ok");
      assertThat(vlm.paths.get(0)).isEqualTo("/v3/chat/completions");
    }
  }

  @Test
  void endpointVersionedBase_getsOnlyChatCompletions() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = chatBody("\"ok\"");
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url() + "/v3", Duration.ofMillis(1));
      assertThat(client.complete("m", "p", null, 10, Duration.ofSeconds(5))).isEqualTo("ok");
      assertThat(vlm.paths.get(0)).isEqualTo("/v3/chat/completions");
    }
  }

  // -------------------------------------------------------------------------
  // Bounded replies: size and time
  // -------------------------------------------------------------------------

  @Test
  void oversizedReply_isAnErrorNotBuffered() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = chatBody("\"" + "x".repeat(OpenAiCompatVlmClient.MAX_RESPONSE_BYTES) + "\"");
      OpenAiCompatVlmClient client = new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      VlmException error = catchThrowableOfType(VlmException.class,
          () -> client.complete("m", "p", null, 10, Duration.ofSeconds(10)));
      assertThat(error.getMessage()).contains("exceeds");
      assertThat(vlm.calls.get()).as("an oversized reply is not retried").isEqualTo(1);
    }
  }

  @Test
  void trickledReply_isBoundedByTheTimeout() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      // Headers at once, then one byte every 100ms: the request timeout alone
      // only covers the headers, so without a whole-reply deadline this call
      // takes ~10 seconds (and could be made to take days).
      vlm.body = chatBody("\"" + "x".repeat(60) + "\"");
      vlm.dripBytes = 1;
      vlm.dripMillis = 100;
      OpenAiCompatVlmClient client = new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      long start = System.nanoTime();
      assertThatThrownBy(() -> client.complete("m", "p", null, 10, Duration.ofMillis(500)))
          .isInstanceOf(VlmException.class);
      assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(4));
      assertThat(vlm.calls.get()).as("a timeout is not retried").isEqualTo(1);
    }
  }

  // -------------------------------------------------------------------------
  // Request shape: lazy crops, typed params, headers
  // -------------------------------------------------------------------------

  private static VlmRequest request(VlmImage image, List<Header> headers) {
    return new VlmRequest("m", "describe", image, 77, OptionalDouble.of(0.5),
        OptionalDouble.of(0.25), OptionalLong.of(-3), headers, Duration.ofSeconds(5));
  }

  @Test
  void cropBytes_areSentAsAnExactDataUri() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = chatBody("\"ok\"");
      byte[] png = new byte[3001];
      for (int i = 0; i < png.length; i++) {
        png[i] = (byte) (i * 31);
      }
      OpenAiCompatVlmClient client = new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      client.complete(request(new VlmImage.Bytes("image/x-test\"\\", ByteString.copyFrom(png)),
          List.of()));

      Map<String, Object> sent = Json.asObject(Json.parse(vlm.requestBodies.get(0)));
      Map<String, Object> message = Json.asObject(Json.asArray(sent.get("messages")).get(0));
      Map<String, Object> imagePart = Json.asObject(Json.asArray(message.get("content")).get(1));
      String url = Json.asString(Json.asObject(imagePart.get("image_url")).get("url"));
      assertThat(url).isEqualTo("data:image/x-test\"\\;base64,"
          + Base64.getEncoder().encodeToString(png));
      assertThat(sent).containsEntry("max_tokens", 77.0).containsEntry("temperature", 0.5)
          .containsEntry("top_p", 0.25).containsEntry("seed", -3.0);
    }
  }

  @Test
  void headerTheJdkRefuses_isReportedByNameOnly() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      OpenAiCompatVlmClient client = new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      VlmException error = catchThrowableOfType(VlmException.class, () -> client.complete(
          request(null, List.of(new Header("X-Token", "secret-value\r\nX-Evil: 1")))));
      assertThat(error.getMessage()).contains("X-Token").doesNotContain("secret-value");
      assertThat(vlm.calls.get()).isZero();
    }
  }

  @Test
  void safeMessage_leavesOutWhatTheEndpointSent() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.status = 403;
      vlm.body = "<html>internal admin console token=abc123</html>";
      OpenAiCompatVlmClient client = new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      VlmException error = catchThrowableOfType(VlmException.class,
          () -> client.complete("m", "p", null, 10, Duration.ofSeconds(5)));
      assertThat(error.getMessage()).contains("HTTP 403").contains("admin console");
      assertThat(error.safeMessage()).contains("HTTP 403").doesNotContain("admin")
          .doesNotContain("abc123");
    }
  }

  @Test
  void redirect_isAnErrorNotFollowed() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      // The client never follows redirects (HttpClient.Redirect.NEVER): a
      // misconfigured endpoint fails loudly instead of silently POSTing the
      // document elsewhere.
      vlm.status = 302;
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      VlmException error = catchThrowableOfType(VlmException.class,
          () -> client.complete("m", "p", null, 10, Duration.ofSeconds(5)));
      assertThat(error.getMessage()).contains("302");
      assertThat(vlm.calls.get()).isEqualTo(1); // 302 is not in the retry set
    }
  }

  // -------------------------------------------------------------------------
  // Deeply nested / hostile bodies must be VlmException, never a crash
  // -------------------------------------------------------------------------

  @Test
  void deeplyNested200Body_throwsVlmExceptionNotStackOverflow() throws Exception {
    try (RawVlmServer vlm = new RawVlmServer()) {
      vlm.body = "[".repeat(200_000) + "]".repeat(200_000);
      OpenAiCompatVlmClient client =
          new OpenAiCompatVlmClient(vlm.url(), Duration.ofMillis(1));
      assertThatThrownBy(() -> client.complete("m", "p", null, 10, Duration.ofSeconds(5)))
          .isInstanceOf(VlmException.class);
    }
  }
}
