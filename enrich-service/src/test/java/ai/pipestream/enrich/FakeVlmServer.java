package ai.pipestream.enrich;

import ai.pipestream.enrich.vlm.Json;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * An in-process fake of an OpenAI-compatible VLM endpoint. Tests configure
 * what content it returns (optionally per request), whether it gates
 * responses on a latch (to prove streaming), or what status it answers with
 * (fixed or per 1-based call index). Records every request so tests can
 * assert the model name, that image bytes rode along, the headers, and the
 * attempt count.
 */
final class FakeVlmServer implements AutoCloseable {

  record RecordedRequest(String model, boolean hasImage, int maxTokens, String prompt,
      String body, Map<String, List<String>> headers) {

    /** The values of one request header, matched case-insensitively. */
    List<String> header(String name) {
      return headers.entrySet().stream()
          .filter(entry -> entry.getKey().equalsIgnoreCase(name))
          .flatMap(entry -> entry.getValue().stream())
          .toList();
    }
  }

  private final HttpServer server;
  private final java.util.concurrent.ExecutorService executor =
      java.util.concurrent.Executors.newCachedThreadPool();
  final List<RecordedRequest> requests = new java.util.ArrayList<>();
  private final AtomicInteger callCount = new AtomicInteger();

  /** Supplies the assistant content for a request body. */
  Function<String, String> responder = body -> "fake description";
  /** When non-null, the request whose 1-based index is in this map waits on
   * the latch before responding. */
  Map<Integer, CountDownLatch> gates = Map.of();
  /** When non-200, every request fails with this status. */
  int status = 200;
  /** Per-call status override (1-based call index → HTTP status); defaults to
   * {@link #status} so transient failures can be scripted. */
  Function<Integer, Integer> statusForCall = call -> status;
  /** The body sent with a non-200 status. */
  volatile String errorBody = "{\"error\":{\"message\":\"model unavailable\"}}";
  /** When non-null, the exact body of every 200 reply, instead of a
   * chat-completions envelope around the responder's content. */
  volatile String rawOkBody;
  /** When non-null, sent as Retry-After with every non-200 reply. */
  volatile String retryAfter;

  FakeVlmServer() {
    try {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    } catch (IOException failure) {
      throw new IllegalStateException(failure);
    }
    server.createContext("/v1/chat/completions", exchange -> {
      String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
      int call = callCount.incrementAndGet();
      record(body, Map.copyOf(exchange.getRequestHeaders()));
      CountDownLatch gate = gates.get(call);
      if (gate != null) {
        try {
          gate.await();
        } catch (InterruptedException interrupt) {
          Thread.currentThread().interrupt();
        }
      }
      byte[] response;
      int code = statusForCall.apply(call);
      if (code != 200) {
        response = errorBody.getBytes(StandardCharsets.UTF_8);
      } else if (rawOkBody != null) {
        response = rawOkBody.getBytes(StandardCharsets.UTF_8);
      } else {
        response = ("{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":"
                + Json.quote(responder.apply(body)) + "}}]}")
            .getBytes(StandardCharsets.UTF_8);
      }
      if (code != 200 && retryAfter != null) {
        exchange.getResponseHeaders().set("Retry-After", retryAfter);
      }
      exchange.sendResponseHeaders(code, response.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(response);
      }
    });
    server.setExecutor(executor);
    server.start();
  }

  private synchronized void record(String body, Map<String, List<String>> headers) {
    String model = "";
    boolean hasImage = false;
    int maxTokens = 0;
    String prompt = "";
    try {
      Map<String, Object> root = Json.asObject(Json.parse(body));
      Object modelValue = root.get("model");
      model = modelValue instanceof String ? (String) modelValue : "";
      Object maxTokensValue = root.get("max_tokens");
      maxTokens = maxTokensValue instanceof Number ? ((Number) maxTokensValue).intValue() : 0;
      hasImage = body.contains("\"image_url\"");
      List<Object> messages = Json.asArray(root.get("messages"));
      List<Object> content = Json.asArray(Json.asObject(messages.get(0)).get("content"));
      for (Object part : content) {
        Map<String, Object> partMap = Json.asObject(part);
        if ("text".equals(partMap.get("type"))) {
          prompt = Json.asString(partMap.get("text"));
          break;
        }
      }
    } catch (RuntimeException ignored) {
      // The record is diagnostic; a parse failure must not fail the test here.
    }
    requests.add(new RecordedRequest(model, hasImage, maxTokens, prompt, body, headers));
  }

  /** Requests received so far, safe to read from any thread. */
  int calls() {
    return callCount.get();
  }

  /** A snapshot of the recorded requests, safe to read from any thread. */
  synchronized List<RecordedRequest> recorded() {
    return List.copyOf(requests);
  }

  String url() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @Override
  public void close() {
    server.stop(0);
    executor.shutdownNow();
  }
}
