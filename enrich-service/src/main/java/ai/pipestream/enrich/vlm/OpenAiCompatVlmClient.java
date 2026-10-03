package ai.pipestream.enrich.vlm;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
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
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

/**
 * VLM client for any OpenAI-compatible chat-completions endpoint (llama.cpp's
 * server, OVMS, vLLM, and friends). The endpoint is a base URL such as
 * {@code http://vlm:8080} or a full endpoint URL; {@link VlmEndpoint} has the
 * rule for which is which.
 *
 * <p>Retry behavior: up to 5 retries on HTTP 429/500/502/503/504 and on
 * connection-level failures (a starting vLLM endpoint commonly drops
 * connections), with exponential backoff of 0.1s, 0.2s, 0.4s, 0.8s, 1.6s,
 * honoring a {@code Retry-After} header when present. Other 4xx, per-request
 * timeouts, oversized bodies, and unparseable 200 bodies are not retried.
 *
 * <p><b>One deadline per call.</b> The per-call timeout (which the engine
 * never lets a caller raise above the server's own) bounds the whole call:
 * every attempt, response body included, and every wait between attempts.
 * Each attempt gets only the time remaining, and a wait that would reach the
 * deadline ends the call with the last failure instead, so a hostile or
 * buggy endpoint answering 429 with a long {@code Retry-After} cannot hold a
 * process-wide VLM slot for more than one timeout.
 *
 * <p><b>Bounded replies.</b> A reply is read into memory up to
 * {@link #MAX_RESPONSE_BYTES} (a 4096-token answer is a few tens of KiB) and
 * the exchange is cancelled past that, so an endpoint cannot stream
 * gigabytes into the heap. An interrupt (the RPC was cancelled) aborts the
 * exchange at once.
 *
 * <p><b>Pinned clients.</b> {@link #pinned} binds a client to one checked IP
 * address for a caller-chosen endpoint: the request goes to that address
 * literally, so the JDK never resolves the host name again, and the host
 * name still goes out as the Host header and as the TLS server name, which
 * is also the name the server certificate is checked against. Setting the
 * Host header needs {@code jdk.httpclient.allowRestrictedHeaders=host},
 * which {@link #allowHostHeader()} sets; it has to run before the JDK HTTP
 * client is first used, so the server calls it first thing.
 */
public final class OpenAiCompatVlmClient implements VlmClient {

  private static final String RESTRICTED_HEADERS_PROPERTY =
      "jdk.httpclient.allowRestrictedHeaders";

  static {
    allowHostHeader();
  }

  /** Builds shared-pool clients, and pinned ones for caller endpoints. */
  public static final VlmClient.Factory FACTORY = new VlmClient.Factory() {
    @Override
    public VlmClient create(String endpoint) {
      return new OpenAiCompatVlmClient(endpoint);
    }

    @Override
    public VlmClient createPinned(String endpoint, InetAddress address) {
      return pinned(endpoint, address);
    }
  };

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
  /** Whether {@link #http} is this client's own, to shut down on close. */
  private final boolean ownsHttp;
  /** The Host header a pinned client sends; null to let the JDK set it. */
  private final String hostHeader;
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
    this(VlmEndpoint.completionsUri(endpoint), SHARED_HTTP, false, null, baseBackoff);
  }

  private OpenAiCompatVlmClient(URI completionsUri, HttpClient http, boolean ownsHttp,
      String hostHeader, Duration baseBackoff) {
    this.completionsUri = completionsUri;
    this.http = http;
    this.ownsHttp = ownsHttp;
    this.hostHeader = hostHeader;
    this.baseBackoff = baseBackoff;
  }

  /**
   * Lets this process's JDK HTTP client send a Host header of its own, which
   * a pinned client needs. Idempotent; has no effect once the JDK HTTP
   * client has been used, so call it at startup.
   */
  public static void allowHostHeader() {
    String allowed = System.getProperty(RESTRICTED_HEADERS_PROPERTY);
    if (allowed == null || allowed.isBlank()) {
      System.setProperty(RESTRICTED_HEADERS_PROPERTY, "host");
    } else if (List.of(allowed.split(",")).stream()
        .noneMatch(name -> name.strip().equalsIgnoreCase("host"))) {
      System.setProperty(RESTRICTED_HEADERS_PROPERTY, allowed + ",host");
    }
  }

  /**
   * A client for {@code endpoint} that connects to {@code address} only:
   * see the class comment. An endpoint whose host is already an IP literal
   * needs no pinning and uses the shared pool.
   *
   * @throws IllegalArgumentException when {@code endpoint} is not an http or
   *     https URL with a host, or this JVM does not let the client set the
   *     Host header
   */
  public static OpenAiCompatVlmClient pinned(String endpoint, InetAddress address) {
    try {
      return pinned(endpoint, address, DEFAULT_BASE_BACKOFF, SSLContext.getDefault());
    } catch (NoSuchAlgorithmException noTls) {
      throw new IllegalStateException("no default TLS context", noTls);
    }
  }

  /** Test seam for {@link #pinned(String, InetAddress)}: the retry backoff
   * and the TLS context that decides which certificates are trusted. */
  public static OpenAiCompatVlmClient pinned(
      String endpoint, InetAddress address, Duration baseBackoff, SSLContext tls) {
    URI named = VlmEndpoint.completionsUri(endpoint);
    String host = named.getHost();
    if (PublicAddress.literal(host) != null) {
      return new OpenAiCompatVlmClient(named, SHARED_HTTP, false, null, baseBackoff);
    }
    // Rebuilt from the raw bytes so an IPv6 scope id never ends up in the URL.
    String literal;
    try {
      literal = InetAddress.getByAddress(address.getAddress()).getHostAddress();
    } catch (UnknownHostException impossible) {
      throw new IllegalArgumentException("unusable address");
    }
    if (literal.contains(":")) {
      literal = "[" + literal + "]";
    }
    int port = named.getPort();
    String scheme = named.getScheme().toLowerCase(Locale.ROOT);
    boolean defaultPort = port == -1
        || (scheme.equals("http") && port == 80)
        || (scheme.equals("https") && port == 443);
    URI target = URI.create(scheme + "://" + literal + (port == -1 ? "" : ":" + port)
        + (named.getRawPath() == null ? "" : named.getRawPath())
        + (named.getRawQuery() == null ? "" : "?" + named.getRawQuery()));
    String hostHeader = defaultPort ? host : host + ":" + port;
    try {
      HttpRequest.newBuilder(target).header("Host", hostHeader);
    } catch (IllegalArgumentException restricted) {
      throw new IllegalArgumentException("this JVM does not let the VLM client set the Host"
          + " header; start it with -D" + RESTRICTED_HEADERS_PROPERTY + "=host");
    }
    SSLParameters tlsParameters = tls.getDefaultSSLParameters();
    tlsParameters.setServerNames(List.of(new SNIHostName(host)));
    // HTTP/1.1, so the Host header is what the server sees (HTTP/2 would
    // send the address as :authority).
    HttpClient own = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .version(HttpClient.Version.HTTP_1_1)
        .sslContext(tls)
        .sslParameters(tlsParameters)
        .build();
    return new OpenAiCompatVlmClient(target, own, true, hostHeader, baseBackoff);
  }

  @Override
  public void close() {
    if (ownsHttp) {
      http.shutdown();
    }
  }

  @Override
  public String complete(VlmRequest call) throws VlmException {
    HttpRequest request = buildRequest(call);
    // Differences of nanoTime stay correct across overflow, so even a
    // Long.MAX_VALUE budget needs no special case.
    long deadline = System.nanoTime() + nanos(call.timeout());
    for (int attempt = 0; ; attempt++) {
      final HttpResponse<byte[]> response;
      try {
        response = send(request, remaining(deadline));
      } catch (IOException failure) {
        // The JDK's message can quote the response (a malformed status line
        // is repeated verbatim), so only the exception type is safe to report.
        VlmException failed = new VlmException(
            "VLM endpoint call failed (" + failure.getClass().getSimpleName() + ")",
            failure.getMessage(), failure);
        if (attempt < MAX_RETRIES && isRetryable(failure)) {
          waitToRetry(backoff(attempt), deadline, failed);
          continue;
        }
        throw failed;
      }
      String body = new String(response.body(), StandardCharsets.UTF_8);
      if (response.statusCode() != 200) {
        VlmException failed = new VlmException(
            "VLM endpoint answered HTTP " + response.statusCode(), snippet(body), null);
        if (attempt < MAX_RETRIES && RETRYABLE_STATUSES.contains(response.statusCode())) {
          waitToRetry(retryWait(response, attempt), deadline, failed);
          continue;
        }
        throw failed;
      }
      return extractContent(body);
    }
  }

  /** The time left before {@code deadline}, a {@link System#nanoTime()} value;
   * zero or negative once it has passed. */
  private static Duration remaining(long deadline) {
    return Duration.ofNanos(deadline - System.nanoTime());
  }

  /** Sleeps {@code wait} before the next attempt, or throws {@code failure}
   * when the call's deadline would pass first: an attempt with no time left
   * could only time out. */
  private static void waitToRetry(Duration wait, long deadline, VlmException failure)
      throws VlmException {
    if (wait.compareTo(remaining(deadline)) >= 0) {
      throw failure;
    }
    sleep(wait);
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
    if (hostHeader != null) {
      builder.header("Host", hostHeader);
    }
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

  /** The wait before the next attempt: Retry-After when present and not
   * negative, else exponential backoff. The call's deadline bounds it (see
   * {@link #waitToRetry}), so a huge Retry-After ends the call rather than
   * parking a worker thread for days. */
  private Duration retryWait(HttpResponse<?> response, int attempt) {
    return retryAfter(response).filter(delay -> !delay.isNegative())
        .orElse(backoff(attempt));
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
