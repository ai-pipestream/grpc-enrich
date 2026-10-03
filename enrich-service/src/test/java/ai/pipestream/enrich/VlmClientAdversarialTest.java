package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import ai.pipestream.enrich.vlm.OpenAiCompatVlmClient;
import ai.pipestream.enrich.vlm.VlmClient.VlmException;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
        synchronized (paths) {
          paths.add(exchange.getRequestURI().getPath());
          queries.add(exchange.getRequestURI().getRawQuery());
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
