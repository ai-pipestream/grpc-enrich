package ai.pipestream.enrich.server;

import ai.pipestream.document.v1.Document;
import ai.pipestream.enrich.engine.EnrichmentEngine;
import ai.pipestream.enrich.v1.EnrichDocumentRequest;
import ai.pipestream.enrich.v1.EnrichDocumentResponse;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.EnrichServiceGrpc;
import ai.pipestream.enrich.v1.GetServiceInfoRequest;
import ai.pipestream.enrich.v1.GetServiceInfoResponse;
import ai.pipestream.enrich.v1.ItemImage;
import ai.pipestream.enrich.v1.UiInfo;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The EnrichService stream handler. The first client message must carry
 * EnrichOptions; when it also carries the document inline, enrichment starts
 * immediately -- events flow before the client has finished sending. When the
 * document arrives as DocumentChunk messages, enrichment starts on the chunk
 * marked complete (crops must precede it). RPC-level failures are
 * INVALID_ARGUMENT / RESOURCE_EXHAUSTED; a failed VLM call is an ItemSkipped
 * event, never an RPC error.
 */
public final class EnrichServiceImpl extends EnrichServiceGrpc.EnrichServiceImplBase {

  public static final String SERVICE_VERSION = "0.1.0";
  public static final String API_VERSION = "v1";

  // Frontend advertisement for the shared demo shell; same UiInfo shape in
  // every ai-pipestream grpc service.
  public static final UiInfo UI_INFO = UiInfo.newBuilder()
      .setTitle("Enrich")
      .setPath("/ui/enrich")
      .setDescription("Document in, stream of typed ItemAnnotation enrichment events out")
      .build();

  final AtomicLong enriched = new AtomicLong();
  final AtomicLong rejected = new AtomicLong();

  private final long maxDocumentBytes;
  private final EnrichmentEngine engine;
  private final ExecutorService executor;
  private final String defaultEndpoint;
  private final int maxConcurrentVlmCalls;

  public EnrichServiceImpl(
      long maxDocumentBytes,
      EnrichmentEngine engine,
      ExecutorService executor,
      String defaultEndpoint,
      int maxConcurrentVlmCalls) {
    this.maxDocumentBytes = maxDocumentBytes;
    this.engine = engine;
    this.executor = executor;
    this.defaultEndpoint = defaultEndpoint;
    this.maxConcurrentVlmCalls = maxConcurrentVlmCalls;
  }

  @Override
  public StreamObserver<EnrichDocumentRequest> enrichDocument(
      StreamObserver<EnrichDocumentResponse> responseObserver) {
    return new StreamObserver<>() {
      private final Object sendLock = new Object();
      // A ByteString rope: concat is cheap and parseFrom reads it directly,
      // so chunked uploads are never copied into an intermediate array.
      private ByteString chunks = ByteString.EMPTY;
      private final Map<String, ItemImage> crops = new HashMap<>();
      private EnrichOptions options;
      private boolean started;
      private boolean terminated;

      @Override
      public void onNext(EnrichDocumentRequest request) {
        if (terminated) {
          return;
        }
        if (options == null) {
          if (!request.hasOptions()) {
            fail(Status.INVALID_ARGUMENT,
                "first message on the stream must carry EnrichOptions");
            return;
          }
          options = request.getOptions();
          if (options.hasDocument()) {
            start(options.getDocument());
          }
          return;
        }
        switch (request.getRequestCase()) {
          case OPTIONS -> fail(Status.INVALID_ARGUMENT, "EnrichOptions was already received");
          case CHUNK -> onChunk(request.getChunk().getData(),
              request.getChunk().getComplete());
          case IMAGE -> crops.put(request.getImage().getSelfRef(), request.getImage());
          default -> fail(Status.INVALID_ARGUMENT, "empty request message");
        }
      }

      private void onChunk(ByteString data, boolean complete) {
        if (started) {
          fail(Status.INVALID_ARGUMENT, "document was already received");
          return;
        }
        if (chunks.size() + data.size() > maxDocumentBytes) {
          rejected.incrementAndGet();
          fail(Status.RESOURCE_EXHAUSTED, "assembled document exceeds the byte cap of "
              + maxDocumentBytes);
          return;
        }
        chunks = chunks.concat(data);
        if (complete) {
          final Document document;
          try {
            document = Document.parseFrom(chunks);
          } catch (InvalidProtocolBufferException bad) {
            fail(Status.INVALID_ARGUMENT,
                "chunk bytes do not parse as a Document: " + bad.getMessage());
            return;
          }
          start(document);
        }
      }

      @Override
      public void onError(Throwable error) {
        terminated = true;
      }

      @Override
      public void onCompleted() {
        if (terminated) {
          return;
        }
        if (!started) {
          if (chunks.isEmpty()) {
            fail(Status.INVALID_ARGUMENT,
                "stream ended without a document: send EnrichOptions with an inline document "
                    + "or DocumentChunk messages");
          } else {
            fail(Status.INVALID_ARGUMENT,
                "stream ended without a chunk marked complete");
          }
        }
        // When started, the engine completes the response after EnrichComplete.
      }

      private void start(Document document) {
        started = true;
        enriched.incrementAndGet();
        Map<String, ItemImage> cropsSnapshot = Map.copyOf(crops);
        executor.execute(() -> {
          engine.enrich(document, cropsSnapshot, options, this::emit);
          synchronized (sendLock) {
            if (!terminated) {
              terminated = true;
              responseObserver.onCompleted();
            }
          }
        });
      }

      private void emit(EnrichDocumentResponse event) {
        synchronized (sendLock) {
          if (!terminated) {
            try {
              responseObserver.onNext(event);
            } catch (RuntimeException closed) {
              terminated = true;
            }
          }
        }
      }

      private void fail(Status status, String detail) {
        synchronized (sendLock) {
          if (!terminated) {
            terminated = true;
            responseObserver.onError(status.withDescription(detail).asRuntimeException());
          }
        }
      }
    };
  }

  @Override
  public void getServiceInfo(
      GetServiceInfoRequest request, StreamObserver<GetServiceInfoResponse> responseObserver) {
    responseObserver.onNext(GetServiceInfoResponse.newBuilder()
        .setServiceVersion(SERVICE_VERSION)
        .setApiVersion(API_VERSION)
        .setDefaultVlmEndpoint(defaultEndpoint)
        .setMaxDocumentBytes(maxDocumentBytes)
        .setMaxConcurrentVlmCalls(maxConcurrentVlmCalls)
        .setUi(UI_INFO)
        .build());
    responseObserver.onCompleted();
  }
}
