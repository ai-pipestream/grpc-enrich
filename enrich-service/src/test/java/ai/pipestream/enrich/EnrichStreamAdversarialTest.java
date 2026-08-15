package ai.pipestream.enrich;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.pipestream.document.v1.DocItemLabel;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.ImageRef;
import ai.pipestream.document.v1.PictureItem;
import ai.pipestream.enrich.engine.EnrichmentEngine;
import ai.pipestream.enrich.server.EnrichServiceImpl;
import ai.pipestream.enrich.v1.DocumentChunk;
import ai.pipestream.enrich.v1.EnrichDocumentRequest;
import ai.pipestream.enrich.v1.EnrichDocumentResponse;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.EnrichServiceGrpc;
import ai.pipestream.enrich.v1.ItemAnnotation;
import ai.pipestream.enrich.v1.ItemSkipped;
import ai.pipestream.enrich.vlm.OpenAiCompatVlmClient;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Adversarial stream and concurrency cases: many items at low and high
 * concurrency, a VLM call that hangs, client cancellation mid-stream,
 * duplicate self_refs, and the stream protocol edge cases.
 */
class EnrichStreamAdversarialTest {

  private static final String PNG_DATA_URI =
      "data:image/png;base64," + Base64.getEncoder().encodeToString(new byte[] {1, 2, 3, 4});

  private final List<AutoCloseable> cleanups = new ArrayList<>();

  @AfterEach
  void tearDown() throws Exception {
    for (AutoCloseable cleanup : cleanups) {
      cleanup.close();
    }
  }

  private EnrichServiceGrpc.EnrichServiceStub startService(String defaultEndpoint)
      throws Exception {
    String name = InProcessServerBuilder.generateName();
    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    EnrichmentEngine engine = new EnrichmentEngine(
        endpoint -> new OpenAiCompatVlmClient(endpoint, Duration.ofMillis(5)), defaultEndpoint,
        4, 16, Duration.ofSeconds(10), executor);
    EnrichServiceImpl service =
        new EnrichServiceImpl(64L * 1024 * 1024, engine, executor, defaultEndpoint, 16);
    Server server =
        InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
    ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    cleanups.add(() -> {
      channel.shutdownNow();
      server.shutdownNow();
      executor.shutdownNow();
    });
    return EnrichServiceGrpc.newStub(channel);
  }

  private record Collected(
      List<EnrichDocumentResponse> events, StatusRuntimeException error, boolean completed) {}

  private Collected runRpc(
      EnrichServiceGrpc.EnrichServiceStub stub, List<EnrichDocumentRequest> requests)
      throws InterruptedException {
    BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
    StreamObserver<EnrichDocumentRequest> requester =
        stub.enrichDocument(new StreamObserver<>() {
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
        });
    requests.forEach(requester::onNext);
    requester.onCompleted();

    List<EnrichDocumentResponse> events = new ArrayList<>();
    while (true) {
      Object item = inbox.poll(30, TimeUnit.SECONDS);
      assertNotNull(item, "RPC did not terminate within 30s");
      if (item instanceof EnrichDocumentResponse event) {
        events.add(event);
      } else if (item instanceof StatusRuntimeException error) {
        return new Collected(events, error, false);
      } else {
        return new Collected(events, null, true);
      }
    }
  }

  private static EnrichDocumentRequest optionsRequest(EnrichOptions options) {
    return EnrichDocumentRequest.newBuilder().setOptions(options).build();
  }

  private static PictureItem picture(String selfRef) {
    return PictureItem.newBuilder()
        .setSelfRef(selfRef)
        .setLabel(DocItemLabel.DOC_ITEM_LABEL_PICTURE)
        .setImage(ImageRef.newBuilder().setUri(PNG_DATA_URI))
        .build();
  }

  private static Document documentWithPictures(int count) {
    Document.Builder document = Document.newBuilder().setName("test");
    for (int i = 0; i < count; i++) {
      document.addPictures(picture("#/pictures/" + i));
    }
    return document.build();
  }

  private static List<ItemAnnotation> annotations(Collected result) {
    return result.events().stream()
        .filter(EnrichDocumentResponse::hasAnnotation)
        .map(EnrichDocumentResponse::getAnnotation)
        .toList();
  }

  private static List<ItemSkipped> skips(Collected result) {
    return result.events().stream()
        .filter(EnrichDocumentResponse::hasSkipped)
        .map(EnrichDocumentResponse::getSkipped)
        .toList();
  }

  private static EnrichDocumentResponse complete(Collected result) {
    return result.events().stream()
        .filter(EnrichDocumentResponse::hasComplete)
        .findFirst()
        .orElseThrow(() -> new AssertionError("no EnrichComplete event"));
  }

  // -------------------------------------------------------------------------
  // Concurrency
  // -------------------------------------------------------------------------

  @Test
  void fiftyItemsConcurrencyOne_allAnnotatedExactlyOnce() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "described";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setConcurrency(1)
          .setDocument(documentWithPictures(50))
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      assertTrue(result.completed());
      List<ItemAnnotation> annotations = annotations(result);
      assertEquals(50, annotations.size());
      // Event order across items is not contractual, even at concurrency 1
      // (the proto and engine javadoc both say out-of-order is legal): the
      // guarantee is exactly one annotation per selected item.
      assertEquals(50, annotations.stream().map(ItemAnnotation::getSelfRef)
          .distinct().count());
      for (int i = 0; i < 50; i++) {
        int index = i;
        assertTrue(annotations.stream()
            .anyMatch(a -> a.getSelfRef().equals("#/pictures/" + index)));
      }
      assertEquals(50, complete(result).getComplete().getSucceeded());
      assertEquals(0, complete(result).getComplete().getFailed());
      assertEquals(50, vlm.requests.size());
    }
  }

  @Test
  void fiftyItemsHighConcurrency_allAnnotatedExactlyOnce() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "described";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setConcurrency(16)
          .setDocument(documentWithPictures(50))
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      assertTrue(result.completed());
      assertEquals(50, annotations(result).size());
      assertEquals(50, annotations(result).stream().map(ItemAnnotation::getSelfRef)
          .distinct().count());
      assertEquals(50, complete(result).getComplete().getSucceeded());
      assertEquals(0, complete(result).getComplete().getFailed());
      // Exactly one terminal complete event, and it is the last event.
      assertTrue(result.events().get(result.events().size() - 1).hasComplete());
      assertEquals(50, vlm.requests.size());
    }
  }

  @Test
  void hangingVlmCall_timeoutFiresAndStreamCompletes() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      CountDownLatch never = new CountDownLatch(1);
      vlm.gates = java.util.Map.of(1, never);
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setTimeoutSeconds(1)
          .setDocument(documentWithPictures(1))
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      assertTrue(result.completed());
      assertEquals(1, skips(result).size());
      assertEquals(ai.pipestream.enrich.v1.SkipReason.SKIP_REASON_VLM_ERROR,
          skips(result).get(0).getReason());
      assertEquals(1, complete(result).getComplete().getSkipped());
      never.countDown();
    }
  }

  // -------------------------------------------------------------------------
  // Duplicate self_refs: each item must get ITS annotation in the document
  // -------------------------------------------------------------------------

  @Test
  void duplicateSelfRefs_patchesLandOnTheRightItems() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      AtomicInteger counter = new AtomicInteger();
      vlm.responder = body -> "description " + counter.getAndIncrement();
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      // Two distinct pictures carrying the same (pathological) self_ref.
      Document document = Document.newBuilder().setName("test")
          .addPictures(picture("#/pictures/0"))
          .addPictures(picture("#/pictures/0"))
          .build();
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setConcurrency(1)
          .setReturnDocument(true)
          .setDocument(document)
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      assertEquals(2, annotations(result).size());
      Document patched = complete(result).getComplete().getDocument();
      // Which picture's call returned first is not deterministic, but each
      // picture must carry ITS OWN annotation: the two responses must land
      // one-per-picture, never both collapsed onto the last one.
      assertEquals(
          java.util.Set.of("description 0", "description 1"),
          java.util.Set.of(
              patched.getPictures(0).getAnnotations(0).getDescription().getText(),
              patched.getPictures(1).getAnnotations(0).getDescription().getText()));
    }
  }

  @Test
  void uint32OptionsAboveIntMax_stillEnrich() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "described";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      // Java proto exposes uint32 as signed int: -1 is 4294967295 on the
      // wire. Neither a wrapped concurrency nor a wrapped timeout may break
      // enrichment (a negative Duration would fail every VLM call).
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setConcurrency(-1)
          .setTimeoutSeconds(-1)
          .setDocument(documentWithPictures(3))
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      assertTrue(result.completed());
      assertEquals(3, annotations(result).size());
      assertEquals(0, complete(result).getComplete().getFailed());
    }
  }

  // -------------------------------------------------------------------------
  // Client cancellation mid-stream
  // -------------------------------------------------------------------------

  @Test
  void clientCancelsMidStream_serverKeepsServingOtherRpcs() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "described";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());

      CountDownLatch firstEvent = new CountDownLatch(1);
      CountDownLatch terminated = new CountDownLatch(1);
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDocument(documentWithPictures(10))
          .build();
      ClientCallStreamObserver<EnrichDocumentRequest> requester =
          (ClientCallStreamObserver<EnrichDocumentRequest>)
              stub.enrichDocument(new StreamObserver<>() {
                @Override
                public void onNext(EnrichDocumentResponse event) {
                  firstEvent.countDown();
                }

                @Override
                public void onError(Throwable error) {
                  terminated.countDown();
                }

                @Override
                public void onCompleted() {
                  terminated.countDown();
                }
              });
      requester.onNext(optionsRequest(options));
      assertTrue(firstEvent.await(10, TimeUnit.SECONDS));
      requester.cancel("test cancellation", null);
      assertTrue(terminated.await(10, TimeUnit.SECONDS),
          "server never noticed the cancellation");

      // The server must not be wedged: a fresh RPC works normally.
      Collected result = runRpc(stub, List.of(optionsRequest(options)));
      assertNull(result.error());
      assertTrue(result.completed());
      assertEquals(10, annotations(result).size());
    }
  }

  // -------------------------------------------------------------------------
  // Stream protocol edges
  // -------------------------------------------------------------------------

  @Test
  void optionsSentTwice_invalidArgument() throws Exception {
    EnrichServiceGrpc.EnrichServiceStub stub = startService("");
    EnrichOptions options = EnrichOptions.newBuilder()
        .setDoPictureDescription(true)
        .setDocument(documentWithPictures(1))
        .build();
    Collected result = runRpc(stub,
        List.of(optionsRequest(options), optionsRequest(options)));
    assertNotNull(result.error());
    assertEquals(Status.Code.INVALID_ARGUMENT, result.error().getStatus().getCode());
  }

  @Test
  void chunkBeforeOptions_invalidArgument() throws Exception {
    EnrichServiceGrpc.EnrichServiceStub stub = startService("");
    Collected result = runRpc(stub, List.of(EnrichDocumentRequest.newBuilder()
        .setChunk(DocumentChunk.newBuilder()
            .setData(ByteString.copyFromUtf8("x"))
            .setComplete(true))
        .build()));
    assertNotNull(result.error());
    assertEquals(Status.Code.INVALID_ARGUMENT, result.error().getStatus().getCode());
  }

  @Test
  void emptyStream_invalidArgument() throws Exception {
    EnrichServiceGrpc.EnrichServiceStub stub = startService("");
    Collected result = runRpc(stub, List.of());
    assertNotNull(result.error());
    assertEquals(Status.Code.INVALID_ARGUMENT, result.error().getStatus().getCode());
  }

  @Test
  void optionsWithoutDocumentThenHalfClose_invalidArgument() throws Exception {
    EnrichServiceGrpc.EnrichServiceStub stub = startService("");
    Collected result = runRpc(stub, List.of(optionsRequest(
        EnrichOptions.newBuilder().setDoPictureDescription(true).build())));
    assertNotNull(result.error());
    assertEquals(Status.Code.INVALID_ARGUMENT, result.error().getStatus().getCode());
  }

  @Test
  void chunksAfterCompleteChunk_invalidArgument() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      byte[] documentBytes = documentWithPictures(1).toByteArray();
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .build();
      List<EnrichDocumentRequest> requests = List.of(
          optionsRequest(options),
          EnrichDocumentRequest.newBuilder().setChunk(DocumentChunk.newBuilder()
              .setData(ByteString.copyFrom(documentBytes)).setComplete(true)).build(),
          EnrichDocumentRequest.newBuilder().setChunk(DocumentChunk.newBuilder()
              .setData(ByteString.copyFromUtf8("late")).setComplete(true)).build());
      Collected result = runRpc(stub, requests);
      assertNotNull(result.error());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.error().getStatus().getCode());
    }
  }

  @Test
  void documentExactlyAtByteCap_accepted() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "described";
      // Start a service whose cap equals the exact serialized document size.
      byte[] documentBytes = documentWithPictures(1).toByteArray();
      String name = InProcessServerBuilder.generateName();
      ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
      EnrichmentEngine engine = new EnrichmentEngine(
          endpoint -> new OpenAiCompatVlmClient(endpoint, Duration.ofMillis(5)), vlm.url(),
          4, 16, Duration.ofSeconds(10), executor);
      EnrichServiceImpl service = new EnrichServiceImpl(
          documentBytes.length, engine, executor, vlm.url(), 16);
      Server server = InProcessServerBuilder.forName(name).directExecutor()
          .addService(service).build().start();
      ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
      cleanups.add(() -> {
        channel.shutdownNow();
        server.shutdownNow();
        executor.shutdownNow();
      });
      EnrichServiceGrpc.EnrichServiceStub stub = EnrichServiceGrpc.newStub(channel);

      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .build();
      Collected result = runRpc(stub, List.of(
          optionsRequest(options),
          EnrichDocumentRequest.newBuilder().setChunk(DocumentChunk.newBuilder()
              .setData(ByteString.copyFrom(documentBytes)).setComplete(true)).build()));
      assertNull(result.error());
      assertTrue(result.completed());
      assertEquals(1, annotations(result).size());
    }
  }
}
