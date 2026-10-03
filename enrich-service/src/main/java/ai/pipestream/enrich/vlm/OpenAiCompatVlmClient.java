package ai.pipestream.enrich.vlm;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * VLM client for any OpenAI-compatible chat-completions endpoint (llama.cpp's
 * server, OVMS, vLLM, and friends). The endpoint is a base URL such as
 * {@code http://vlm:8080} or a full endpoint URL; {@link VlmEndpoint} has the
 * rule for which is which.
 *
 * <p>Retry behavior: up to 5 retries on HTTP 429/500/502/503/504 and on
 * connection-level failures (a starting vLLM endpoint commonly drops
 * connections), with exponential backoff of 0.1s, 0.2s, 0.4s, 0.8s, 1.6s,
 * honoring a {@code Retry-After} header when present (clamped to the
 * per-call timeout, which the engine never lets a caller raise above the
 * server's own, so a hostile or buggy endpoint cannot park a worker for
 * days). Other 4xx, per-request timeouts, oversized bodies, and unparseable
 * 200 bodies are not retried. The timeout bounds each attempt individually,
 * response body included; retries can add up to 5 extra attempts plus ~3.1s
 * of backoff on top of one timed-out attempt.
 *
 * <p><b>Bounded replies.</b> A reply is read into memory up to
 * {@link #MAX_RESPONSE_BYTES} (a 4096-token answer is a few tens of KiB) and
 * the exchange is cancelled past that, so an endpoint cannot stream
 * gigabytes into the heap. An interrupt (the RPC was cancelled) aborts the
 * exchange at once.
 */
public final class OpenAiCompatVlmClient implements VlmClient {

  /** Cap on one response body; far above any real chat-completions reply. */
  public static final int MAX_RESPONSE_BYTES = 4 << 20;

  private static final int MAX_RETRIES = 5;
  private static final Duration DEFAULT_BASE_BACKOFF = Duration.ofMillis(100);
  private static final Set<Integer> RETRYABLE_STATUSES = Set.of(429, 500, 502, 503, 504);

  /**
   * One HttpClient for the process: HttpClient is thread-safe and pools
   * connections per destination, and a client instance is created per
   * enrichment request, so sharing avoids a fresh connection pool (and its
   * selector machinery) per document.
   */
  private static final HttpClient SHARED_HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  private final URI completionsUri;
  private final HttpClient http;
  private final Duration baseBackoff;

  public OpenAiCompatVlmClient(String endpoint) {
    this(endpoint, DEFAULT_BASE_BACKOFF);
  }

  /**
   * Test seam: {@code baseBackoff} shrinks the retry waits.
   *
   * @throws IllegalArgumentException when {@code endpoint} is not an http or
   *     https URL with a host
   */
  public OpenAiCompatVlmClient(String endpoint, Duration baseBackoff) {
    this.completionsUri = VlmEndpoint.completionsUri(endpoint);
    this.http = SHARED_HTTP;
    this.baseBackoff = baseBackoff;
  }

  @Override
  public String complete(VlmRequest call) throws VlmException {
    HttpRequest request = buildRequest(call);
    for (int attempt = 0; ; attempt++) {
      final HttpResponse<byte[]> response;
      try {
        response = send(request, call.timeout());
      } catch (IOException failure) {
        if (attempt < MAX_RETRIES && isRetryable(failure)) {
          sleep(backoff(attempt));
          continue;
        }
        // The JDK's message can quote the response (a malformed status line
        // is repeated verbatim), so only the exception type is safe to report.
        throw new VlmException(
            "VLM endpoint call failed (" + failure.getClass().getSimpleName() + ")",
            failure.getMessage(), failure);
      }
      String body = new String(response.body(), StandardCharsets.UTF_8);
      if (response.statusCode() != 200) {
        if (attempt < MAX_RETRIES && RETRYABLE_STATUSES.contains(response.statusCode())) {
          sleep(retryWait(response, attempt, call.timeout()));
          continue;
        }
        throw new VlmException(
            "VLM endpoint answered HTTP " + response.statusCode(), snippet(body), null);
      }
      return extractContent(body);
    }
  }

  /**
   * The request for one call. A header the JDK refuses is reported by name
   * only: its value is usually a credential, and the JDK's own message
   * repeats it.
   */
  private HttpRequest buildRequest(VlmRequest call) throws VlmException {
    HttpRequest.Builder builder = HttpRequest.newBuilder(completionsUri)
        .timeout(call.timeout())
        .header("Content-Type", "application/json")
        .POST(requestBody(call));
    for (Header header : call.headers()) {
      try {
        builder.header(header.name(), header.value());
      } catch (IllegalArgumentException refused) {
        throw new VlmException("request header " + header.name() + " is not a valid HTTP header");
      }
    }
    return builder.build();
  }

  /**
   * One attempt under a hard deadline. The request timeout only covers the
   * wait for the response headers, so a body that trickles in would
   * otherwise hold the call (and its concurrency slot) for as long as the
   * endpoint likes.
   */
  private HttpResponse<byte[]> send(HttpRequest request, Duration timeout)
      throws IOException, VlmException {
    CompletableFuture<HttpResponse<byte[]>> pending =
        http.sendAsync(request, info -> new BoundedBody(MAX_RESPONSE_BYTES));
    try {
      return pending.get(nanos(timeout), TimeUnit.NANOSECONDS);
    } catch (InterruptedException interrupt) {
      pending.cancel(true);
      Thread.currentThread().interrupt();
      throw new VlmException("interrupted waiting for the VLM endpoint", interrupt);
    } catch (TimeoutException late) {
      pending.cancel(true);
      throw new HttpTimeoutException("VLM endpoint did not finish answering in time");
    } catch (ExecutionException failed) {
      Throwable cause = failed.getCause();
      while (cause instanceof CompletionException && cause.getCause() != null) {
        cause = cause.getCause();
      }
      if (cause instanceof ResponseTooLarge) {
        throw new VlmException(
            "VLM response body exceeds the " + MAX_RESPONSE_BYTES + "-byte limit");
      }
      if (cause instanceof IOException io) {
        throw io;
      }
      throw new VlmException(
          "VLM endpoint call failed (" + cause.getClass().getSimpleName() + ")",
          cause.getMessage(), cause);
    }
  }

  private static long nanos(Duration timeout) {
    try {
      return timeout.toNanos();
    } catch (ArithmeticException huge) {
      return Long.MAX_VALUE;
    }
  }

  /**
   * Connection-level failures (refused, reset, connect timeout) are
   * retryable; a per-request read timeout is not.
   */
  private static boolean isRetryable(IOException failure) {
    return !(failure instanceof HttpTimeoutException)
        || failure instanceof HttpConnectTimeoutException;
  }

  /** Exponential backoff: base * 2^attempt gives 0.1s, 0.2s, 0.4s, 0.8s, 1.6s. */
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

  /**
   * The chat-completions body. A crop is base64-encoded here, for this call
   * only, and sent as its own buffer between the JSON around it, so no
   * second copy of the encoded image is built.
   */
  private static HttpRequest.BodyPublisher requestBody(VlmRequest call) {
    String prompt = call.prompt();
    StringBuilder head = new StringBuilder(256 + prompt.length());
    head.append('{');
    if (call.model() != null && !call.model().isEmpty()) {
      head.append("\"model\":").append(Json.quote(call.model())).append(',');
    }
    head.append("\"messages\":[{\"role\":\"user\",\"content\":[");
    head.append("{\"type\":\"text\",\"text\":").append(Json.quote(prompt)).append('}');
    byte[] base64 = null;
    switch (call.image()) {
      case null -> { }
      case VlmImage.DataUri inline -> head
          .append(",{\"type\":\"image_url\",\"image_url\":{\"url\":")
          .append(Json.quote(inline.uri()))
          .append("}}");
      case VlmImage.Bytes crop -> {
        String mime = crop.mimetype().isEmpty() ? "image/png" : crop.mimetype();
        String prefix = Json.quote("data:" + mime + ";base64,");
        head.append(",{\"type\":\"image_url\",\"image_url\":{\"url\":")
            .append(prefix, 0, prefix.length() - 1);
        base64 = Base64.getEncoder().encode(crop.data().toByteArray());
      }
    }
    StringBuilder tail = new StringBuilder(96);
    if (base64 != null) {
      tail.append("\"}}");
    }
    tail.append("]}],\"max_tokens\":").append(call.maxTokens());
    call.temperature().ifPresent(value -> tail.append(",\"temperature\":").append(value));
    call.topP().ifPresent(value -> tail.append(",\"top_p\":").append(value));
    call.seed().ifPresent(value -> tail.append(",\"seed\":").append(value));
    tail.append('}');
    if (base64 == null) {
      return HttpRequest.BodyPublishers.ofString(head.append(tail).toString());
    }
    return HttpRequest.BodyPublishers.concat(
        HttpRequest.BodyPublishers.ofByteArray(head.toString().getBytes(StandardCharsets.UTF_8)),
        HttpRequest.BodyPublishers.ofByteArray(base64),
        HttpRequest.BodyPublishers.ofByteArray(tail.toString().getBytes(StandardCharsets.UTF_8)));
  }

  /** Reads choices[0].message.content out of the chat-completions reply. */
  static String extractContent(String body) throws VlmException {
    final Map<String, Object> root;
    try {
      root = Json.asObject(Json.parse(body));
    } catch (IllegalArgumentException bad) {
      throw new VlmException("unparseable VLM response", snippet(body), bad);
    }
    if (root.containsKey("error")) {
      throw new VlmException("VLM endpoint returned an error", snippet(body), null);
    }
    try {
      List<Object> choices = Json.asArray(root.get("choices"));
      Map<String, Object> first = Json.asObject(choices.get(0));
      Map<String, Object> message = Json.asObject(first.get("message"));
      return Json.asString(message.get("content"));
    } catch (RuntimeException shape) {
      throw new VlmException("VLM response had no choices[0].message.content", snippet(body),
          shape);
    }
  }

  private static String snippet(String body) {
    if (body == null) {
      return "";
    }
    return body.length() <= 200 ? body : body.substring(0, 200) + "...";
  }

  /** The body outgrew {@link #MAX_RESPONSE_BYTES}. */
  private static final class ResponseTooLarge extends IOException {
    ResponseTooLarge() {
      super("response body over the limit");
    }
  }

  /**
   * Collects a response body up to {@code limit} bytes. One byte more
   * cancels the subscription, which aborts the exchange, and fails the body.
   */
  private static final class BoundedBody implements HttpResponse.BodySubscriber<byte[]> {
    private final CompletableFuture<byte[]> body = new CompletableFuture<>();
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private final int limit;
    private Flow.Subscription subscription;

    BoundedBody(int limit) {
      this.limit = limit;
    }

    @Override
    public CompletionStage<byte[]> getBody() {
      return body;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      this.subscription = subscription;
      subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(List<ByteBuffer> items) {
      if (body.isDone()) {
        return;
      }
      for (ByteBuffer item : items) {
        if (item.remaining() > limit - buffer.size()) {
          subscription.cancel();
          body.completeExceptionally(new ResponseTooLarge());
          return;
        }
        byte[] chunk = new byte[item.remaining()];
        item.get(chunk);
        buffer.write(chunk, 0, chunk.length);
      }
    }

    @Override
    public void onError(Throwable failure) {
      body.completeExceptionally(failure);
    }

    @Override
    public void onComplete() {
      body.complete(buffer.toByteArray());
    }
  }
}
