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
 * responses on a latch (to prove streaming), or whether it answers 500.
 * Records every request so tests can assert the model name and that image
 * bytes rode along.
 */
final class FakeVlmServer implements AutoCloseable {

  record RecordedRequest(String model, boolean hasImage, String body) {}

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

  FakeVlmServer() {
    try {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    } catch (IOException failure) {
      throw new IllegalStateException(failure);
    }
    server.createContext("/v1/chat/completions", exchange -> {
      String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
      int call = callCount.incrementAndGet();
      record(body);
      CountDownLatch gate = gates.get(call);
      if (gate != null) {
        try {
          gate.await();
        } catch (InterruptedException interrupt) {
          Thread.currentThread().interrupt();
        }
      }
      byte[] response;
      if (status != 200) {
        response = "{\"error\":{\"message\":\"model unavailable\"}}".getBytes(StandardCharsets.UTF_8);
      } else {
        response = ("{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":"
                + Json.quote(responder.apply(body)) + "}}]}")
            .getBytes(StandardCharsets.UTF_8);
      }
      exchange.sendResponseHeaders(status, response.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(response);
      }
    });
    server.setExecutor(executor);
    server.start();
  }

  private synchronized void record(String body) {
    String model = "";
    boolean hasImage = false;
    try {
      Map<String, Object> root = Json.asObject(Json.parse(body));
      Object modelValue = root.get("model");
      model = modelValue instanceof String ? (String) modelValue : "";
      hasImage = body.contains("\"image_url\"");
    } catch (RuntimeException ignored) {
      // The record is diagnostic; a parse failure must not fail the test here.
    }
    requests.add(new RecordedRequest(model, hasImage, body));
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
