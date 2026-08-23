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
    final AtomicInteger calls = new AtomicInteger();
    volatile int status = 200;
    volatile String body = "";
    volatile String retryAfter;

    RawVlmServer() throws IOException {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext("/", exchange -> {
        calls.incrementAndGet();
        synchronized (paths) {
          paths.add(exchange.getRequestURI().getPath());
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        if (retryAfter != null) {
          exchange.getResponseHeaders().set("Retry-After", retryAfter);
        }
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(bytes);
        }
      });
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
