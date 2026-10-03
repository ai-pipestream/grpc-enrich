package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.document.v1.DocItemLabel;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.ImageRef;
import ai.pipestream.document.v1.PictureItem;
import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.engine.EnrichmentEngine;
import ai.pipestream.enrich.server.EnrichHttpServer;
import ai.pipestream.enrich.server.EnrichServiceImpl;
import ai.pipestream.enrich.v1.EnrichComplete;
import ai.pipestream.enrich.v1.EnrichDocumentRequest;
import ai.pipestream.enrich.v1.EnrichDocumentResponse;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.EnrichServiceGrpc;
import ai.pipestream.enrich.v1.ItemAnnotation;
import ai.pipestream.enrich.v1.ItemSkipped;
import ai.pipestream.enrich.vlm.OpenAiCompatVlmClient;
import ai.pipestream.enrich.vlm.VlmClient;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * One in-process grpc-enrich for a test: engine, service, an in-process gRPC
 * server and stub, and optionally the HTTP front end, all closed together.
 * The knobs the hardening tests turn (endpoint policy, process-wide
 * concurrency, timeout, byte cap, VLM client) are constructor arguments.
 */
final class InProcessEnrich implements AutoCloseable {

  static final String PNG_DATA_URI = "data:image/png;base64,"
      + Base64.getEncoder().encodeToString(new byte[] {(byte) 0x89, 'P', 'N', 'G', 1, 2, 3});

  final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  final EnrichmentEngine engine;
  final EnrichServiceImpl service;
  final EnrichServiceGrpc.EnrichServiceStub stub;
  private final Server server;
  private final ManagedChannel channel;
  private EnrichHttpServer http;

  private InProcessEnrich(VlmClient.Factory clients, EndpointPolicy endpoints,
      int defaultConcurrency, int maxConcurrency, Duration timeout, long maxDocumentBytes)
      throws IOException {
    engine = new EnrichmentEngine(clients, endpoints, defaultConcurrency, maxConcurrency,
        timeout, executor);
    service = new EnrichServiceImpl(maxDocumentBytes, engine, executor,
        endpoints.defaultEndpoint(), maxConcurrency);
    String name = InProcessServerBuilder.generateName();
    server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build()
        .start();
    channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    stub = EnrichServiceGrpc.newStub(channel);
  }

  /** The real HTTP VLM client with a 5ms backoff, defaults for the rest. */
  static InProcessEnrich start(EndpointPolicy endpoints) throws IOException {
    return start(endpoints, 4, 16, Duration.ofSeconds(10), 64L * 1024 * 1024);
  }

  static InProcessEnrich start(EndpointPolicy endpoints, int defaultConcurrency,
      int maxConcurrency, Duration timeout, long maxDocumentBytes) throws IOException {
    return start(endpoint -> new OpenAiCompatVlmClient(endpoint, Duration.ofMillis(5)),
        endpoints, defaultConcurrency, maxConcurrency, timeout, maxDocumentBytes);
  }

  static InProcessEnrich start(VlmClient.Factory clients, EndpointPolicy endpoints,
      int defaultConcurrency, int maxConcurrency, Duration timeout, long maxDocumentBytes)
      throws IOException {
    return new InProcessEnrich(clients, endpoints, defaultConcurrency, maxConcurrency, timeout,
        maxDocumentBytes);
  }

  /** Starts the HTTP front end on an ephemeral port and returns its base URL. */
  String httpBase() throws IOException {
    if (http == null) {
      http = new EnrichHttpServer(0, service, executor);
      http.start();
    }
    return "http://127.0.0.1:" + http.getPort();
  }

  @Override
  public void close() {
    if (http != null) {
      http.close();
    }
    channel.shutdownNow();
    server.shutdownNow();
    executor.shutdownNow();
  }

  /** Everything one RPC produced: its events and how it ended. */
  record Collected(
      List<EnrichDocumentResponse> events, StatusRuntimeException error, boolean completed) {

    List<ItemAnnotation> annotations() {
      return events.stream()
          .filter(EnrichDocumentResponse::hasAnnotation)
          .map(EnrichDocumentResponse::getAnnotation)
          .toList();
    }

    List<ItemSkipped> skips() {
      return events.stream()
          .filter(EnrichDocumentResponse::hasSkipped)
          .map(EnrichDocumentResponse::getSkipped)
          .toList();
    }

    EnrichComplete complete() {
      return events.get(events.size() - 1).getComplete();
    }
  }

  /** Sends {@code options} alone (an inline document), half-closes, collects. */
  Collected run(EnrichOptions options) throws InterruptedException {
    return run(List.of(options(options)));
  }

  /** Sends every request, half-closes, and waits for the RPC to terminate. */
  Collected run(List<EnrichDocumentRequest> requests) throws InterruptedException {
    BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
    StreamObserver<EnrichDocumentRequest> requester = stub.enrichDocument(inbox(inbox));
    requests.forEach(requester::onNext);
    requester.onCompleted();
    return collect(inbox);
  }

  /** A response observer that drops every event, error, and DONE on {@code inbox}. */
  static StreamObserver<EnrichDocumentResponse> inbox(BlockingQueue<Object> inbox) {
    return new StreamObserver<>() {
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
        inbox.add("DONE");
      }
    };
  }

  /** Drains {@code inbox} until the RPC ends. */
  static Collected collect(BlockingQueue<Object> inbox) throws InterruptedException {
    List<EnrichDocumentResponse> events = new ArrayList<>();
    while (true) {
      Object item = inbox.poll(30, TimeUnit.SECONDS);
      assertThat(item).as("RPC did not terminate within 30s").isNotNull();
      if (item instanceof EnrichDocumentResponse event) {
        events.add(event);
      } else if (item instanceof StatusRuntimeException error) {
        return new Collected(events, error, false);
      } else {
        return new Collected(events, null, true);
      }
    }
  }

  static EnrichDocumentRequest options(EnrichOptions options) {
    return EnrichDocumentRequest.newBuilder().setOptions(options).build();
  }

  static PictureItem picture(String selfRef) {
    return PictureItem.newBuilder()
        .setSelfRef(selfRef)
        .setLabel(DocItemLabel.DOC_ITEM_LABEL_PICTURE)
        .setImage(ImageRef.newBuilder().setMimetype("image/png").setUri(PNG_DATA_URI))
        .build();
  }

  static Document pictures(int count) {
    Document.Builder document = Document.newBuilder().setName("test");
    for (int i = 0; i < count; i++) {
      document.addPictures(picture("#/pictures/" + i));
    }
    return document.build();
  }
}
