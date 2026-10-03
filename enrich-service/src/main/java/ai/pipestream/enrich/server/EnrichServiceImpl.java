package ai.pipestream.enrich.server;

import ai.pipestream.document.v1.Document;
import ai.pipestream.enrich.engine.EnrichmentEngine;
import ai.pipestream.enrich.engine.ItemSelector;
import ai.pipestream.enrich.v1.EnrichDocumentRequest;
import ai.pipestream.enrich.v1.EnrichDocumentResponse;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.EnrichServiceGrpc;
import ai.pipestream.enrich.v1.GetServiceInfoRequest;
import ai.pipestream.enrich.v1.GetServiceInfoResponse;
import ai.pipestream.enrich.v1.ItemImage;
import ai.pipestream.enrich.v1.UiInfo;
import ai.pipestream.enrich.vlm.VlmEndpoint;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
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
 * INVALID_ARGUMENT / RESOURCE_EXHAUSTED / PERMISSION_DENIED; a failed VLM
 * call is an ItemSkipped event, never an RPC error.
 *
 * <p><b>Memory bound.</b> The byte cap covers everything one call holds: the
 * document, inline or assembled from chunks, plus its ItemImage crops, and
 * at most {@link #MAX_CROPS} crops. Crops after enrichment started are a
 * protocol error rather than bytes held for nothing.
 *
 * <p><b>Cancellation.</b> A client cancel or an expired deadline cancels the
 * enrichment: in-flight VLM calls are interrupted and queued ones never
 * start, instead of running to completion with their events dropped.
 */
public final class EnrichServiceImpl extends EnrichServiceGrpc.EnrichServiceImplBase {

  public static final String SERVICE_VERSION = "0.1.0";
  public static final String API_VERSION = "v1";

  /** Most ItemImage crops one call may send: each costs a map entry beyond
   * its bytes, so tiny crops must be bounded by count, not only by size. */
  public static final int MAX_CROPS = 100_000;

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
      private final EnrichmentEngine.Cancellation cancellation =
          new EnrichmentEngine.Cancellation();
      // A ByteString rope: concat is cheap and parseFrom reads it directly,
      // so chunked uploads are never copied into an intermediate array.
      private ByteString chunks = ByteString.EMPTY;
      private final Map<String, ItemImage> crops = new HashMap<>();
      // Serialized bytes of the crops held, counted against the byte cap.
      private long cropBytes;
      private EnrichOptions options;
      private boolean started;
      // Written under sendLock; volatile so onNext sees a cancel at once.
      private volatile boolean terminated;

      {
        // grpc-java only tells a handler about a cancel or an expired
        // deadline through this callback once the client has half-closed
        // (onError is not called then), and it may only be registered
        // before this observer is returned.
        if (responseObserver instanceof ServerCallStreamObserver<EnrichDocumentResponse> call) {
          call.setOnCancelHandler(this::cancel);
        }
      }

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
          if (options.hasChartExtraction()
              && ItemSelector.enabledChartOutputs(options.getChartExtraction()).isEmpty()) {
            // Docling's ChartExtractionVlmEngineOptions validator, same rule.
            fail(Status.INVALID_ARGUMENT, "chart_extraction enables no output: at least one of "
                + "csv, summary, or code must be true");
            return;
          }
          Status refusal = engine.validate(options);
          if (!refusal.isOk()) {
            fail(refusal, refusal.getDescription());
            return;
          }
          if (options.hasDocument()) {
            // The gRPC message limit only bounds the whole frame (cap plus
            // framing), and the HTTP shim has no frame limit at all, so the
            // cap is checked here, exactly.
            if (options.getDocument().getSerializedSize() > maxDocumentBytes) {
              rejected.incrementAndGet();
              fail(Status.RESOURCE_EXHAUSTED, "inline document exceeds the byte cap of "
                  + maxDocumentBytes);
              return;
            }
            start(options.getDocument());
          }
          return;
        }
        switch (request.getRequestCase()) {
          case OPTIONS -> fail(Status.INVALID_ARGUMENT, "EnrichOptions was already received");
          case CHUNK -> onChunk(request.getChunk().getData(),
              request.getChunk().getComplete());
          case IMAGE -> onImage(request.getImage());
          default -> fail(Status.INVALID_ARGUMENT, "empty request message");
        }
      }

      private void onImage(ItemImage image) {
        if (started) {
          fail(Status.INVALID_ARGUMENT, "ItemImage arrived after enrichment started; send crops"
              + " before the chunk marked complete (an inline document takes no crops)");
          return;
        }
        ItemImage replaced = crops.get(image.getSelfRef());
        if (replaced == null && crops.size() >= MAX_CROPS) {
          rejected.incrementAndGet();
          fail(Status.RESOURCE_EXHAUSTED, "more than " + MAX_CROPS + " ItemImage crops");
          return;
        }
        long held = cropBytes + image.getSerializedSize()
            - (replaced == null ? 0 : replaced.getSerializedSize());
        if (chunks.size() + held > maxDocumentBytes) {
          rejected.incrementAndGet();
          fail(Status.RESOURCE_EXHAUSTED, "document chunks plus ItemImage crops exceed the byte"
              + " cap of " + maxDocumentBytes);
          return;
        }
        crops.put(image.getSelfRef(), image);
        cropBytes = held;
      }

      private void onChunk(ByteString data, boolean complete) {
        if (started) {
          fail(Status.INVALID_ARGUMENT, "document was already received");
          return;
        }
        if (chunks.size() + cropBytes + data.size() > maxDocumentBytes) {
          rejected.incrementAndGet();
          fail(Status.RESOURCE_EXHAUSTED, "assembled document plus ItemImage crops exceed the"
              + " byte cap of " + maxDocumentBytes);
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
        cancel();
      }

      /** The client is gone: stop sending, and stop the VLM work. */
      private void cancel() {
        synchronized (sendLock) {
          terminated = true;
        }
        cancellation.cancel();
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
          engine.enrich(document, cropsSnapshot, options, this::emit, cancellation);
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

      /** Ends the RPC with an error. Enrichment already running for it has
       * nobody left to report to, so it is cancelled too. */
      private void fail(Status status, String detail) {
        synchronized (sendLock) {
          if (terminated) {
            return;
          }
          terminated = true;
          responseObserver.onError(status.withDescription(detail).asRuntimeException());
        }
        cancellation.cancel();
      }
    };
  }

  /** The byte cap on a document plus its crops. */
  long maxDocumentBytes() {
    return maxDocumentBytes;
  }

  @Override
  public void getServiceInfo(
      GetServiceInfoRequest request, StreamObserver<GetServiceInfoResponse> responseObserver) {
    responseObserver.onNext(GetServiceInfoResponse.newBuilder()
        .setServiceVersion(SERVICE_VERSION)
        .setApiVersion(API_VERSION)
        .setDefaultVlmEndpoint(VlmEndpoint.origin(defaultEndpoint))
        .setMaxDocumentBytes(maxDocumentBytes)
        .setMaxConcurrentVlmCalls(maxConcurrentVlmCalls)
        .setUi(UI_INFO)
        .build());
    responseObserver.onCompleted();
  }
}
