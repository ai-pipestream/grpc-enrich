package ai.pipestream.enrich.server;

import ai.pipestream.document.v1.Document;
import ai.pipestream.enrich.v1.DocumentChunk;
import ai.pipestream.enrich.v1.EnrichDocumentRequest;
import ai.pipestream.enrich.v1.EnrichDocumentResponse;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.ItemImage;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.google.protobuf.util.JsonFormat;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * HTTP front end for the EnrichService: a thin shim that drives the existing
 * {@link EnrichServiceImpl} through an in-process StreamObserver harness, so
 * the gRPC stream semantics (options first, crops before the completing
 * chunk, per-item events as VLM calls return) are reused, never forked.
 * Bodies are canonical proto3 JSON, parsed and printed with protobuf's
 * JsonFormat; Gson only splits the {@code {"options":…, "document":…,
 * "item_images":…}} envelope into the sub-documents JsonFormat then parses.
 * GetServiceInfo stays gRPC-only.
 *
 * <p>Endpoints: {@code POST /v1/enrich} collects every stream event and
 * returns them as one {@code {"events":[…]}} JSON document;
 * {@code POST /v1/enrich/stream} answers chunked NDJSON, one flushed line per
 * EnrichDocumentResponse event as the stream produces it, with a final
 * {@code {"error":…}} line when the stream fails mid-flight;
 * {@code GET /healthz} is a static 200 "ok".
 */
public final class EnrichHttpServer implements AutoCloseable {

  private static final String DONE = "DONE";

  /** NDJSON needs one line per event, so the compact printer everywhere. */
  private static final JsonFormat.Printer PRINTER =
      JsonFormat.printer().omittingInsignificantWhitespace();

  private final HttpServer server;

  public EnrichHttpServer(int port, EnrichServiceImpl service, Executor executor)
      throws IOException {
    server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
    server.createContext("/healthz", EnrichHttpServer::healthz);
    server.createContext("/v1/enrich", exchange -> enrichSync(service, exchange));
    server.createContext("/v1/enrich/stream", exchange -> enrichStream(service, exchange));
    server.setExecutor(executor);
  }

  public void start() {
    server.start();
  }

  public int getPort() {
    return server.getAddress().getPort();
  }

  @Override
  public void close() {
    server.stop(0);
  }

  /** The parsed request envelope: options, an optional document, and crops. */
  private record Envelope(EnrichOptions options, Document document, List<ItemImage> images) {}

  /**
   * Splits the envelope with Gson (which preserves numeric lexemes exactly)
   * and lets JsonFormat parse each proto message, so integer fields survive
   * the round trip and every proto3 JSON rule (enum names, base64 bytes,
   * both camelCase and snake_case keys) applies unchanged.
   */
  private static Envelope parseEnvelope(String body) throws InvalidProtocolBufferException {
    final JsonObject root;
    try {
      root = JsonParser.parseString(body).getAsJsonObject();
    } catch (RuntimeException bad) {
      throw new InvalidProtocolBufferException("body is not a JSON object");
    }
    EnrichOptions.Builder options = EnrichOptions.newBuilder();
    if (root.has("options")) {
      JsonFormat.parser().merge(member(root, "options"), options);
    }
    Document document = null;
    if (root.has("document")) {
      if (options.hasDocument()) {
        throw new InvalidProtocolBufferException(
            "the document was supplied both inline in options and at the top level");
      }
      Document.Builder builder = Document.newBuilder();
      JsonFormat.parser().merge(member(root, "document"), builder);
      document = builder.build();
    }
    List<ItemImage> images = new ArrayList<>();
    JsonElement itemImages = root.has("item_images") ? root.get("item_images")
        : root.get("itemImages");
    if (itemImages != null) {
      if (!itemImages.isJsonArray()) {
        throw new InvalidProtocolBufferException("item_images must be a JSON array");
      }
      for (JsonElement element : itemImages.getAsJsonArray()) {
        ItemImage.Builder image = ItemImage.newBuilder();
        JsonFormat.parser().merge(element.toString(), image);
        images.add(image.build());
      }
    }
    return new Envelope(options.build(), document, images);
  }

  private static String member(JsonObject root, String name)
      throws InvalidProtocolBufferException {
    JsonElement element = root.get(name);
    if (!element.isJsonObject()) {
      throw new InvalidProtocolBufferException(name + " must be a JSON object");
    }
    return element.toString();
  }

  /**
   * One in-process gRPC client: plays options → ItemImage crops → the
   * completing DocumentChunk → half-close, exactly as a wire client would.
   * Events, the terminal error, or DONE land on the inbox in stream order.
   */
  private static final class Harness implements StreamObserver<EnrichDocumentResponse> {
    final BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();

    @Override
    public void onNext(EnrichDocumentResponse event) {
      inbox.add(event);
    }

    @Override
    public void onError(Throwable error) {
      inbox.add(error);
    }

    @Override
    public void onCompleted() {
      inbox.add(DONE);
    }
  }

  private static Harness drive(EnrichServiceImpl service, Envelope envelope) {
    Harness harness = new Harness();
    StreamObserver<EnrichDocumentRequest> requester = service.enrichDocument(harness);
    requester.onNext(EnrichDocumentRequest.newBuilder().setOptions(envelope.options()).build());
    // With an inline document in options the service starts immediately, as on
    // the wire; a top-level document goes the chunk route so crops apply and
    // the assembled-bytes cap is enforced.
    if (envelope.document() != null) {
      for (ItemImage image : envelope.images()) {
        requester.onNext(EnrichDocumentRequest.newBuilder().setImage(image).build());
      }
      requester.onNext(EnrichDocumentRequest.newBuilder()
          .setChunk(DocumentChunk.newBuilder()
              .setData(envelope.document().toByteString())
              .setComplete(true))
          .build());
    }
    requester.onCompleted();
    return harness;
  }

  private static Object take(BlockingQueue<Object> inbox) throws IOException {
    try {
      return inbox.take();
    } catch (InterruptedException interrupt) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted while waiting for the enrichment stream", interrupt);
    }
  }

  private static void enrichSync(EnrichServiceImpl service, HttpExchange exchange)
      throws IOException {
    if (!"POST".equals(exchange.getRequestMethod())) {
      sendError(exchange, 405, "POST only");
      return;
    }
    String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    final Envelope envelope;
    try {
      envelope = parseEnvelope(body);
    } catch (InvalidProtocolBufferException bad) {
      sendError(exchange, 400, bad.getMessage());
      return;
    }
    Harness harness = drive(service, envelope);
    List<EnrichDocumentResponse> events = new ArrayList<>();
    while (true) {
      Object item = take(harness.inbox);
      if (item instanceof EnrichDocumentResponse event) {
        events.add(event);
      } else if (item instanceof Throwable error) {
        Status status = Status.fromThrowable(error);
        sendError(exchange, httpStatus(status), status.getDescription());
        return;
      } else {
        break;
      }
    }
    StringBuilder json = new StringBuilder("{\"events\":[");
    for (int i = 0; i < events.size(); i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append(PRINTER.print(events.get(i)));
    }
    json.append("]}");
    sendJson(exchange, 200, json.toString());
  }

  private static void enrichStream(EnrichServiceImpl service, HttpExchange exchange)
      throws IOException {
    if (!"POST".equals(exchange.getRequestMethod())) {
      sendError(exchange, 405, "POST only");
      return;
    }
    String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    final Envelope envelope;
    try {
      envelope = parseEnvelope(body);
    } catch (InvalidProtocolBufferException bad) {
      sendError(exchange, 400, bad.getMessage());
      return;
    }
    Harness harness = drive(service, envelope);
    OutputStream out = null;
    while (true) {
      Object item = take(harness.inbox);
      if (item instanceof EnrichDocumentResponse event) {
        if (out == null) {
          exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
          exchange.sendResponseHeaders(200, 0); // chunked: length unknown
          out = exchange.getResponseBody();
        }
        out.write((PRINTER.print(event) + "\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
      } else if (item instanceof Throwable error) {
        Status status = Status.fromThrowable(error);
        if (out == null) {
          // Failed before the first event: a real HTTP status still applies.
          sendError(exchange, httpStatus(status), status.getDescription());
        } else {
          out.write(("{\"error\":" + quote(status.getDescription()) + "}\n")
              .getBytes(StandardCharsets.UTF_8));
          out.flush();
          out.close();
        }
        return;
      } else {
        if (out == null) {
          exchange.sendResponseHeaders(200, -1); // no events at all
        } else {
          out.close();
        }
        return;
      }
    }
  }

  private static void healthz(HttpExchange exchange) throws IOException {
    if (!"GET".equals(exchange.getRequestMethod())) {
      sendError(exchange, 405, "GET only");
      return;
    }
    byte[] ok = "ok".getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "text/plain");
    exchange.sendResponseHeaders(200, ok.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(ok);
    }
  }

  private static int httpStatus(Status status) {
    return switch (status.getCode()) {
      case INVALID_ARGUMENT -> 400;
      case RESOURCE_EXHAUSTED -> 413;
      default -> 500;
    };
  }

  private static void sendError(HttpExchange exchange, int status, String message)
      throws IOException {
    sendJson(exchange, status, "{\"error\":" + quote(message) + "}");
  }

  /** JSON-quotes a string with JsonFormat rather than by hand. */
  private static String quote(String text) throws InvalidProtocolBufferException {
    return PRINTER.print(StringValue.of(text == null ? "" : text));
  }

  private static void sendJson(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }
}
