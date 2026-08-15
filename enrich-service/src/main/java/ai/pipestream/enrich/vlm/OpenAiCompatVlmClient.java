package ai.pipestream.enrich.vlm;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * VLM client for any OpenAI-compatible chat-completions endpoint (llama.cpp's
 * server, OVMS, vLLM, and friends). The endpoint is a base URL such as
 * {@code http://vlm:8080}; the request goes to
 * {@code <endpoint>/v1/chat/completions} unless the endpoint already ends with
 * that path.
 *
 * <p>Retry behavior mirrors Docling's {@code api_image_request} (urllib3
 * {@code Retry(total=5, connect=5, read=0, backoff_factor=0.1,
 * status_forcelist=(429, 500, 502, 503, 504))}): up to 5 retries on those
 * statuses and on connection-level failures (a starting vLLM endpoint commonly
 * drops connections), with exponential backoff of 0.1s, 0.2s, 0.4s, 0.8s, 1.6s,
 * honoring a {@code Retry-After} header when present (clamped to the per-call
 * timeout, so a hostile or buggy endpoint cannot park a worker for days). Other 4xx, per-request
 * timeouts, and unparseable 200 bodies are not retried. The caller's timeout
 * still bounds each attempt (as it does in Docling, where the timeout is
 * per-request too); retries can add up to 5 extra attempts plus ~3.1s of
 * backoff on top of one timed-out attempt.
 */
public final class OpenAiCompatVlmClient implements VlmClient {

  private static final String COMPLETIONS_PATH = "/v1/chat/completions";
  private static final int MAX_RETRIES = 5;
  private static final Duration DEFAULT_BASE_BACKOFF = Duration.ofMillis(100);
  private static final Set<Integer> RETRYABLE_STATUSES = Set.of(429, 500, 502, 503, 504);

  private final String completionsUrl;
  private final HttpClient http;
  private final Duration baseBackoff;

  public OpenAiCompatVlmClient(String endpoint) {
    this(endpoint, DEFAULT_BASE_BACKOFF);
  }

  /** Test seam: {@code baseBackoff} shrinks the retry waits. */
  public OpenAiCompatVlmClient(String endpoint, Duration baseBackoff) {
    String trimmed = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
    this.completionsUrl =
        trimmed.endsWith(COMPLETIONS_PATH) ? trimmed : trimmed + COMPLETIONS_PATH;
    this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    this.baseBackoff = baseBackoff;
  }

  @Override
  public String complete(String model, String prompt, String imageDataUri, int maxTokens,
      Duration timeout)
      throws VlmException {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(completionsUrl))
            .timeout(timeout)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(
                requestBody(model, prompt, imageDataUri, maxTokens)))
            .build();
    for (int attempt = 0; ; attempt++) {
      final HttpResponse<String> response;
      try {
        response = http.send(request, HttpResponse.BodyHandlers.ofString());
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
        throw new VlmException("interrupted waiting for the VLM endpoint", interrupt);
      } catch (IOException failure) {
        if (attempt < MAX_RETRIES && isRetryable(failure)) {
          sleep(backoff(attempt));
          continue;
        }
        throw new VlmException("VLM endpoint call failed: " + failure.getMessage(), failure);
      } catch (Exception failure) {
        throw new VlmException("VLM endpoint call failed: " + failure.getMessage(), failure);
      }
      if (response.statusCode() != 200) {
        if (attempt < MAX_RETRIES && RETRYABLE_STATUSES.contains(response.statusCode())) {
          sleep(retryWait(response, attempt, timeout));
          continue;
        }
        throw new VlmException(
            "VLM endpoint answered HTTP " + response.statusCode() + ": "
                + snippet(response.body()));
      }
      return extractContent(response.body());
    }
  }

  /**
   * Connection-level failures (refused, reset, connect timeout) are retryable,
   * like Docling's {@code connect=5}; a per-request read timeout is not, like
   * Docling's {@code read=0}.
   */
  private static boolean isRetryable(IOException failure) {
    return !(failure instanceof HttpTimeoutException)
        || failure instanceof HttpConnectTimeoutException;
  }

  /** Docling parity: backoff_factor * 2^attempt → 0.1s, 0.2s, 0.4s, 0.8s, 1.6s. */
  private Duration backoff(int attempt) {
    return Duration.ofMillis(baseBackoff.toMillis() << attempt);
  }

  /** The wait before the next attempt: Retry-After when present and sane,
   * else exponential backoff. Retry-After is clamped to the per-call timeout
   * (a negative value is ignored): a hostile or buggy endpoint must not be
   * able to park a worker thread for days by answering 429 with a huge
   * Retry-After. */
  private Duration retryWait(HttpResponse<?> response, int attempt, Duration timeout) {
    Duration wait = retryAfter(response).filter(delay -> !delay.isNegative())
        .orElse(backoff(attempt));
    return wait.compareTo(timeout) > 0 ? timeout : wait;
  }

  private static Optional<Duration> retryAfter(HttpResponse<?> response) {
    Optional<String> header = response.headers().firstValue("Retry-After");
    if (header.isEmpty()) {
      return Optional.empty();
    }
    try {
      return Optional.of(Duration.ofSeconds(Long.parseLong(header.get().trim())));
    } catch (NumberFormatException ignored) {
      return Optional.empty();
    }
  }

  private static void sleep(Duration delay) throws VlmException {
    try {
      Thread.sleep(delay);
    } catch (InterruptedException interrupt) {
      Thread.currentThread().interrupt();
      throw new VlmException("interrupted backing off from the VLM endpoint", interrupt);
    }
  }

  private static String requestBody(String model, String prompt, String imageDataUri,
      int maxTokens) {
    StringBuilder body = new StringBuilder(256 + prompt.length());
    body.append('{');
    if (model != null && !model.isEmpty()) {
      body.append("\"model\":").append(Json.quote(model)).append(',');
    }
    body.append("\"messages\":[{\"role\":\"user\",\"content\":[");
    body.append("{\"type\":\"text\",\"text\":").append(Json.quote(prompt)).append('}');
    if (imageDataUri != null) {
      body.append(",{\"type\":\"image_url\",\"image_url\":{\"url\":")
          .append(Json.quote(imageDataUri))
          .append("}}");
    }
    body.append("]}],\"max_tokens\":").append(maxTokens).append('}');
    return body.toString();
  }

  /** Reads choices[0].message.content out of the chat-completions reply. */
  static String extractContent(String body) throws VlmException {
    final Map<String, Object> root;
    try {
      root = Json.asObject(Json.parse(body));
    } catch (IllegalArgumentException bad) {
      throw new VlmException("unparseable VLM response: " + snippet(body), bad);
    }
    if (root.containsKey("error")) {
      throw new VlmException("VLM endpoint returned an error: " + snippet(body));
    }
    try {
      List<Object> choices = Json.asArray(root.get("choices"));
      Map<String, Object> first = Json.asObject(choices.get(0));
      Map<String, Object> message = Json.asObject(first.get("message"));
      return Json.asString(message.get("content"));
    } catch (RuntimeException shape) {
      throw new VlmException("VLM response had no choices[0].message.content: " + snippet(body),
          shape);
    }
  }

  private static String snippet(String body) {
    if (body == null) {
      return "";
    }
    return body.length() <= 200 ? body : body.substring(0, 200) + "...";
  }
}
