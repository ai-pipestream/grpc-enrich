package ai.pipestream.enrich;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.pipestream.document.v1.BoundingBox;
import ai.pipestream.document.v1.CodeItem;
import ai.pipestream.document.v1.CodeLanguageLabel;
import ai.pipestream.document.v1.DocItemLabel;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.FormulaItem;
import ai.pipestream.document.v1.ImageRef;
import ai.pipestream.document.v1.PageItem;
import ai.pipestream.document.v1.PictureAnnotation;
import ai.pipestream.document.v1.PictureClassificationClass;
import ai.pipestream.document.v1.PictureClassificationData;
import ai.pipestream.document.v1.PictureItem;
import ai.pipestream.document.v1.ProvenanceItem;
import ai.pipestream.document.v1.Size;
import ai.pipestream.document.v1.TextItemBase;
import ai.pipestream.enrich.engine.EnrichmentEngine;
import ai.pipestream.enrich.server.EnrichServiceImpl;
import ai.pipestream.enrich.v1.ChartPreset;
import ai.pipestream.enrich.v1.DocumentChunk;
import ai.pipestream.enrich.v1.EnrichDocumentRequest;
import ai.pipestream.enrich.v1.EnrichDocumentResponse;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.EnrichServiceGrpc;
import ai.pipestream.enrich.v1.GetServiceInfoRequest;
import ai.pipestream.enrich.v1.GetServiceInfoResponse;
import ai.pipestream.enrich.v1.ItemAnnotation;
import ai.pipestream.enrich.v1.ItemImage;
import ai.pipestream.enrich.v1.ItemSkipped;
import ai.pipestream.enrich.v1.PictureDescriptionPreset;
import ai.pipestream.enrich.v1.SkipReason;
import ai.pipestream.enrich.vlm.OpenAiCompatVlmClient;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests of the EnrichDocument stream against a fake VLM HTTP
 * endpoint (no network, no model weights). Covers the definition of done:
 * describe on one picture, skip-on-missing-image with the RPC still OK,
 * chart extraction as typed cells, VLM endpoint down, and proof the stream
 * is live rather than a batch.
 */
class EnrichServiceTest {

  private static final byte[] PNG_BYTES = new byte[] {(byte) 0x89, 'P', 'N', 'G', 1, 2, 3, 4};
  private static final String PNG_DATA_URI =
      "data:image/png;base64," + Base64.getEncoder().encodeToString(PNG_BYTES);

  private final List<AutoCloseable> cleanups = new ArrayList<>();

  @AfterEach
  void tearDown() throws Exception {
    for (AutoCloseable cleanup : cleanups) {
      cleanup.close();
    }
  }

  // -------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------

  private static PictureItem pictureWithImage(String selfRef) {
    return PictureItem.newBuilder()
        .setSelfRef(selfRef)
        .setLabel(DocItemLabel.DOC_ITEM_LABEL_PICTURE)
        .setImage(ImageRef.newBuilder()
            .setMimetype("image/png")
            .setDpi(72)
            .setSize(Size.newBuilder().setWidth(8).setHeight(8))
            .setUri(PNG_DATA_URI))
        .build();
  }

  private static Document documentOnePictureOneParagraph() {
    return Document.newBuilder()
        .setName("test")
        .addPictures(pictureWithImage("#/pictures/0"))
        .addTexts(ai.pipestream.document.v1.BaseTextItem.newBuilder()
            .setText(ai.pipestream.document.v1.TextItem.newBuilder()
                .setBase(TextItemBase.newBuilder()
                    .setSelfRef("#/texts/0")
                    .setLabel(DocItemLabel.DOC_ITEM_LABEL_PARAGRAPH)
                    .setText("a paragraph"))))
        .build();
  }

  private EnrichServiceGrpc.EnrichServiceStub startService(String defaultEndpoint)
      throws Exception {
    return startService(defaultEndpoint, 64L * 1024 * 1024);
  }

  private EnrichServiceGrpc.EnrichServiceStub startService(
      String defaultEndpoint, long maxDocumentBytes) throws Exception {
    String name = InProcessServerBuilder.generateName();
    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    EnrichmentEngine engine = new EnrichmentEngine(
        endpoint -> new OpenAiCompatVlmClient(endpoint, Duration.ofMillis(5)), defaultEndpoint,
        4, 16, Duration.ofSeconds(10), executor);
    EnrichServiceImpl service =
        new EnrichServiceImpl(maxDocumentBytes, engine, executor, defaultEndpoint, 16);
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

  /** One collected terminal fact about the RPC: events, error, or completion. */
  private record Collected(
      List<EnrichDocumentResponse> events, StatusRuntimeException error, boolean completed) {}

  /** Sends every request, half-closes, and waits for the RPC to terminate. */
  private Collected runRpc(
      EnrichServiceGrpc.EnrichServiceStub stub, List<EnrichDocumentRequest> requests)
      throws InterruptedException {
    BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
    StreamObserver<EnrichDocumentResponse> observer = new StreamObserver<>() {
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
    StreamObserver<EnrichDocumentRequest> requester = stub.enrichDocument(observer);
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

  /** Drives the unary GetServiceInfo RPC to completion and returns the response. */
  private static GetServiceInfoResponse unaryGetServiceInfo(
      EnrichServiceGrpc.EnrichServiceStub stub) throws InterruptedException {
    BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
    stub.getServiceInfo(GetServiceInfoRequest.getDefaultInstance(), new StreamObserver<>() {
      @Override
      public void onNext(GetServiceInfoResponse response) {
        inbox.add(response);
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

    GetServiceInfoResponse response = null;
    while (true) {
      Object item = inbox.poll(30, TimeUnit.SECONDS);
      assertNotNull(item, "RPC did not terminate within 30s");
      if (item instanceof GetServiceInfoResponse info) {
        response = info;
      } else if (item instanceof Throwable error) {
        return fail("GetServiceInfo failed", error);
      } else {
        return response;
      }
    }
  }

  // -------------------------------------------------------------------------
  // Definition-of-done cases
  // -------------------------------------------------------------------------

  @Test
  void getServiceInfo_advertisesUi() throws Exception {
    EnrichServiceGrpc.EnrichServiceStub stub = startService("");

    GetServiceInfoResponse info = unaryGetServiceInfo(stub);

    assertEquals("Enrich", info.getUi().getTitle());
    assertEquals("/ui/enrich", info.getUi().getPath());
    assertEquals("Document in, stream of typed ItemAnnotation enrichment events out",
        info.getUi().getDescription());
  }

  @Test
  void describeOn_onePictureOneParagraph() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "a QR code on a white background";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());

      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setPictureDescriptionPreset(PictureDescriptionPreset.PICTURE_DESCRIPTION_PRESET_SMOLVLM)
          .setDocument(documentOnePictureOneParagraph())
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error(), "RPC must be OK: " + result.error());
      assertTrue(result.completed());
      assertEquals(1, result.events().get(0).getStarted().getPictureDescriptions());
      List<ItemAnnotation> annotations = annotations(result);
      assertEquals(1, annotations.size());
      assertEquals("#/pictures/0", annotations.get(0).getSelfRef());
      assertEquals("a QR code on a white background",
          annotations.get(0).getDescription().getText());
      assertEquals("smolvlm", annotations.get(0).getModel());
      assertTrue(skips(result).isEmpty());
      assertEquals(1, result.events().get(result.events().size() - 1)
          .getComplete().getSucceeded());
      // The text item is untouched: no annotation event references it.
      assertTrue(annotations.stream().noneMatch(a -> a.getSelfRef().equals("#/texts/0")));
      // The fake saw the describe model and the picture bytes.
      assertEquals(1, vlm.requests.size());
      assertEquals("smolvlm", vlm.requests.get(0).model());
      assertTrue(vlm.requests.get(0).hasImage());
    }
  }

  @Test
  void missingImage_itemSkippedRpcOk() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      Document document = Document.newBuilder()
          .setName("test")
          .addPictures(PictureItem.newBuilder()
              .setSelfRef("#/pictures/0")
              .setLabel(DocItemLabel.DOC_ITEM_LABEL_PICTURE))
          .build();
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDocument(document)
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error(), "a missing image must not fail the RPC");
      List<ItemSkipped> skips = skips(result);
      assertEquals(1, skips.size());
      assertEquals("#/pictures/0", skips.get(0).getSelfRef());
      assertEquals(SkipReason.SKIP_REASON_NO_IMAGE, skips.get(0).getReason());
      assertEquals(1, result.events().get(result.events().size() - 1).getComplete().getSkipped());
      assertTrue(vlm.requests.isEmpty(), "no VLM call may be made without image bytes");
    }
  }

  @Test
  void unfetchableUri_itemSkippedRpcOk() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      Document document = Document.newBuilder()
          .setName("test")
          .addPictures(PictureItem.newBuilder()
              .setSelfRef("#/pictures/0")
              .setLabel(DocItemLabel.DOC_ITEM_LABEL_PICTURE)
              .setImage(ImageRef.newBuilder()
                  .setMimetype("image/png")
                  .setUri("https://example.com/qr.png")))
          .build();
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDocument(document)
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      assertEquals(1, skips(result).size());
      assertEquals(SkipReason.SKIP_REASON_UNFETCHABLE_URI, skips(result).get(0).getReason());
    }
  }

  @Test
  void chartExtraction_typedCellsNotCsvOnly() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "year,sales,profit\n2023,10,3";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      PictureItem chart = pictureWithImage("#/pictures/0").toBuilder()
          .setLabel(DocItemLabel.DOC_ITEM_LABEL_CHART)
          .build();
      Document document = Document.newBuilder().setName("test").addPictures(chart).build();
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoChartExtraction(true)
          .setChartPreset(ChartPreset.CHART_PRESET_GRANITE_VISION_CHART2CSV)
          .setDocument(document)
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      List<ItemAnnotation> annotations = annotations(result);
      assertEquals(1, annotations.size());
      var table = annotations.get(0).getChartTable().getTable();
      assertEquals(2, table.getNumRows());
      assertEquals(3, table.getNumCols());
      assertEquals(6, table.getTableCellsCount());
      assertTrue(table.getTableCells(0).getColumnHeader());
      assertEquals("year", table.getTableCells(0).getText());
      assertFalse(table.getTableCells(3).getColumnHeader());
      assertEquals("3", table.getTableCells(5).getText());
      assertEquals(1, table.getTableCells(5).getStartRowOffsetIdx());
      assertEquals(2, table.getTableCells(5).getStartColOffsetIdx());
      assertEquals(2, table.getGridCount());
      // The raw CSV rides along, but the typed cells are the representation.
      assertEquals("year,sales,profit\n2023,10,3", annotations.get(0).getChartTable().getCsv());
      assertEquals("granite-vision-chart2csv", vlm.requests.get(0).model());
    }
  }

  @Test
  void vlmEndpointDown_itemSkippedRpcOk() throws Exception {
    int closedPort;
    try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
      closedPort = socket.getLocalPort();
    }
    EnrichServiceGrpc.EnrichServiceStub stub = startService("http://127.0.0.1:" + closedPort);
    EnrichOptions options = EnrichOptions.newBuilder()
        .setDoPictureDescription(true)
        .setDocument(documentOnePictureOneParagraph())
        .build();
    Collected result = runRpc(stub, List.of(optionsRequest(options)));

    assertNull(result.error(), "a dead VLM endpoint must not fail the RPC");
    assertTrue(annotations(result).isEmpty());
    List<ItemSkipped> skips = skips(result);
    assertEquals(1, skips.size());
    assertEquals(SkipReason.SKIP_REASON_VLM_ERROR, skips.get(0).getReason());
    assertEquals(1, result.events().get(result.events().size() - 1).getComplete().getSkipped());
  }

  @Test
  void vlmServerError_itemSkippedRpcOk() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.status = 500;
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDocument(documentOnePictureOneParagraph())
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      assertEquals(1, skips(result).size());
      assertEquals(SkipReason.SKIP_REASON_VLM_ERROR, skips(result).get(0).getReason());
      assertEquals(6, vlm.requests.size(), "persistent 500 is retried 5 times, then skipped");
    }
  }

  @Test
  void vlmTransientError_retriesAndSucceeds() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.statusForCall = call -> call == 1 ? 503 : 200;
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDocument(documentOnePictureOneParagraph())
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      assertTrue(skips(result).isEmpty());
      assertEquals(1, annotations(result).size());
      assertEquals(2, vlm.requests.size(), "a transient 503 is retried and the item enriches");
    }
  }

  @Test
  void codeAndFormulaEnrichment() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> body.contains("normalize")
          ? "print('hi')"
          : "x^2 + y^2 = z^2";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      Document document = Document.newBuilder()
          .setName("test")
          .addTexts(ai.pipestream.document.v1.BaseTextItem.newBuilder()
              .setCode(CodeItem.newBuilder()
                  .setSelfRef("#/texts/0")
                  .setLabel(DocItemLabel.DOC_ITEM_LABEL_CODE)
                  .setText("print( 'hi' )")))
          .addTexts(ai.pipestream.document.v1.BaseTextItem.newBuilder()
              .setFormula(FormulaItem.newBuilder()
                  .setBase(TextItemBase.newBuilder()
                      .setSelfRef("#/texts/1")
                      .setLabel(DocItemLabel.DOC_ITEM_LABEL_FORMULA)
                      .setText("x2+y2=z2"))))
          .build();
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoCodeEnrichment(true)
          .setDoFormulaEnrichment(true)
          .setDocument(document)
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      assertEquals(1, result.events().get(0).getStarted().getCodeEnrichments());
      assertEquals(1, result.events().get(0).getStarted().getFormulaEnrichments());
      assertEquals(2, annotations(result).size());
      ItemAnnotation code = annotations(result).stream()
          .filter(a -> a.getSelfRef().equals("#/texts/0")).findFirst().orElseThrow();
      assertEquals("print('hi')", code.getCode().getText());
      ItemAnnotation formula = annotations(result).stream()
          .filter(a -> a.getSelfRef().equals("#/texts/1")).findFirst().orElseThrow();
      assertEquals("x^2 + y^2 = z^2", formula.getFormula().getText());
      assertEquals(2, result.events().get(result.events().size() - 1)
          .getComplete().getSucceeded());
      // Text-only enrichment sends no image, keeps the existing text in the
      // prompt, and uses Docling's code/formula generation budget.
      assertEquals(2, vlm.requests.size());
      for (var request : vlm.requests) {
        assertFalse(request.hasImage());
        assertEquals(2048, request.maxTokens());
      }
      assertTrue(vlm.requests.stream()
          .anyMatch(r -> r.prompt().contains("print( 'hi' )")));
      assertTrue(vlm.requests.stream()
          .anyMatch(r -> r.prompt().contains("x2+y2=z2")));
    }
  }

  @Test
  void codeWithCrop_barePromptImageLanguageToken() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "<_Python_>print('hi')</code><end_of_utterance>extra";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      Document document = Document.newBuilder()
          .setName("test")
          .addTexts(ai.pipestream.document.v1.BaseTextItem.newBuilder()
              .setCode(CodeItem.newBuilder()
                  .setSelfRef("#/texts/0")
                  .setLabel(DocItemLabel.DOC_ITEM_LABEL_CODE)
                  .setText("print( 'hi' )")))
          .build();
      Collected result = runRpc(stub, List.of(
          optionsRequest(EnrichOptions.newBuilder().setDoCodeEnrichment(true).build()),
          EnrichDocumentRequest.newBuilder()
              .setImage(ItemImage.newBuilder()
                  .setSelfRef("#/texts/0")
                  .setMimetype("image/png")
                  .setData(ByteString.copyFrom(PNG_BYTES)))
              .build(),
          EnrichDocumentRequest.newBuilder()
              .setChunk(DocumentChunk.newBuilder()
                  .setData(document.toByteString())
                  .setComplete(true))
              .build()));

      assertNull(result.error(), "RPC must be OK: " + result.error());
      // Docling sends the image crop with the bare prompt "<code>".
      assertEquals(1, vlm.requests.size());
      assertTrue(vlm.requests.get(0).hasImage());
      assertEquals("<code>", vlm.requests.get(0).prompt());
      assertEquals(2048, vlm.requests.get(0).maxTokens());
      // Output post-processing: language token out, sentinels stripped.
      assertEquals(1, annotations(result).size());
      var code = annotations(result).get(0).getCode();
      assertEquals("print('hi')", code.getText());
      assertEquals(CodeLanguageLabel.CODE_LANGUAGE_LABEL_PYTHON, code.getLanguage());
      assertEquals("Python", code.getLanguageRaw());
    }
  }

  @Test
  void formulaWithCrop_barePromptAndPostProcessing() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder =
          body -> "<loc_0><loc_0><loc_500><loc_500>x^2 + y^2</formula><end_of_utterance>";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      Document document = Document.newBuilder()
          .setName("test")
          .addTexts(ai.pipestream.document.v1.BaseTextItem.newBuilder()
              .setFormula(FormulaItem.newBuilder()
                  .setBase(TextItemBase.newBuilder()
                      .setSelfRef("#/texts/0")
                      .setLabel(DocItemLabel.DOC_ITEM_LABEL_FORMULA)
                      .setText("x2+y2"))))
          .build();
      Collected result = runRpc(stub, List.of(
          optionsRequest(EnrichOptions.newBuilder().setDoFormulaEnrichment(true).build()),
          EnrichDocumentRequest.newBuilder()
              .setImage(ItemImage.newBuilder()
                  .setSelfRef("#/texts/0")
                  .setMimetype("image/png")
                  .setData(ByteString.copyFrom(PNG_BYTES)))
              .build(),
          EnrichDocumentRequest.newBuilder()
              .setChunk(DocumentChunk.newBuilder()
                  .setData(document.toByteString())
                  .setComplete(true))
              .build()));

      assertNull(result.error(), "RPC must be OK: " + result.error());
      assertEquals(1, vlm.requests.size());
      assertTrue(vlm.requests.get(0).hasImage());
      assertEquals("<formula>", vlm.requests.get(0).prompt());
      assertEquals(2048, vlm.requests.get(0).maxTokens());
      assertEquals("x^2 + y^2", annotations(result).get(0).getFormula().getText());
    }
  }

  @Test
  void returnDocument_setsCodeLanguage() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "<_Java_>int x = 1;";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      Document document = Document.newBuilder()
          .setName("test")
          .addTexts(ai.pipestream.document.v1.BaseTextItem.newBuilder()
              .setCode(CodeItem.newBuilder()
                  .setSelfRef("#/texts/0")
                  .setLabel(DocItemLabel.DOC_ITEM_LABEL_CODE)
                  .setText("int  x=1")))
          .build();
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoCodeEnrichment(true)
          .setReturnDocument(true)
          .setDocument(document)
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      var patched = result.events().get(result.events().size() - 1)
          .getComplete().getDocument().getTexts(0).getCode();
      assertEquals("int x = 1;", patched.getText());
      assertEquals(CodeLanguageLabel.CODE_LANGUAGE_LABEL_JAVA, patched.getCodeLanguage());
      assertEquals("Java", patched.getCodeLanguageRaw());
    }
  }

  @Test
  void belowAreaThreshold_itemSkipped() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      PictureItem tiny = pictureWithImage("#/pictures/0").toBuilder()
          .addProv(ProvenanceItem.newBuilder()
              .setPageNo(1)
              .setBbox(BoundingBox.newBuilder().setL(0).setT(0).setR(10).setB(10)))
          .build();
      Document document = Document.newBuilder()
          .setName("test")
          .addPictures(tiny)
          .putPages(1, PageItem.newBuilder()
              .setPageNo(1)
              .setSize(Size.newBuilder().setWidth(1000).setHeight(1000))
              .build())
          .build();
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setPictureDescriptionAreaThreshold(0.5)
          .setDocument(document)
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      assertEquals(1, skips(result).size());
      assertEquals(SkipReason.SKIP_REASON_BELOW_AREA_THRESHOLD,
          skips(result).get(0).getReason());
      assertTrue(vlm.requests.isEmpty());
    }
  }

  /** A picture covering ~4% of its page on a 1000x1000 page. */
  private static Document documentWithSmallPicture(double bboxExtent) {
    return Document.newBuilder()
        .setName("test")
        .addPictures(pictureWithImage("#/pictures/0").toBuilder()
            .addProv(ProvenanceItem.newBuilder()
                .setPageNo(1)
                .setBbox(BoundingBox.newBuilder()
                    .setL(0).setT(0).setR(bboxExtent).setB(bboxExtent)))
            .build())
        .putPages(1, PageItem.newBuilder()
            .setPageNo(1)
            .setSize(Size.newBuilder().setWidth(1000).setHeight(1000))
            .build())
        .build();
  }

  @Test
  void defaultAreaThreshold_docling005() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "big enough";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      // 200x200 on a 1000x1000 page = 4% < Docling's default 0.05: skipped
      // even though no threshold was set explicitly.
      EnrichOptions small = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDocument(documentWithSmallPicture(200))
          .build();
      Collected skippedResult = runRpc(stub, List.of(optionsRequest(small)));
      assertNull(skippedResult.error());
      assertEquals(1, skips(skippedResult).size());
      assertEquals(SkipReason.SKIP_REASON_BELOW_AREA_THRESHOLD,
          skips(skippedResult).get(0).getReason());
      assertTrue(vlm.requests.isEmpty());

      // 300x300 = 9% >= 0.05: described.
      EnrichOptions big = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDocument(documentWithSmallPicture(300))
          .build();
      Collected describedResult = runRpc(stub, List.of(optionsRequest(big)));
      assertNull(describedResult.error());
      assertEquals(1, annotations(describedResult).size());
    }
  }

  @Test
  void negativeAreaThreshold_disables() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "tiny but described";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setPictureDescriptionAreaThreshold(-1)
          .setDocument(documentWithSmallPicture(200))
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      assertTrue(skips(result).isEmpty());
      assertEquals(1, annotations(result).size());
    }
  }

  private static PictureItem classifiedPicture(String selfRef, String topClass) {
    return pictureWithImage(selfRef).toBuilder()
        .addAnnotations(PictureAnnotation.newBuilder()
            .setClassification(PictureClassificationData.newBuilder()
                .setKind("classification")
                .addPredictedClasses(PictureClassificationClass.newBuilder()
                    .setClassName(topClass)
                    .setConfidence(0.9))))
        .build();
  }

  @Test
  void chartGate_supportedTopPrediction() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "year,sales\n2023,10";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      // Label is PICTURE; the top classification prediction is a Docling
      // supported chart type, so chart extraction runs.
      Document document = Document.newBuilder().setName("test")
          .addPictures(classifiedPicture("#/pictures/0", "bar_chart"))
          .build();
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoChartExtraction(true)
          .setChartPreset(ChartPreset.CHART_PRESET_GRANITE_VISION_CHART2CSV)
          .setDocument(document)
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      assertEquals(1, result.events().get(0).getStarted().getChartExtractions());
      assertEquals(1, annotations(result).size());
      assertTrue(annotations(result).get(0).hasChartTable());
      assertEquals(1, vlm.requests.size());
      // Docling's exact chart prompt and the chart generation budget.
      assertEquals("Convert the information in this chart into a data table in CSV format.",
          vlm.requests.get(0).prompt());
      assertEquals(4096, vlm.requests.get(0).maxTokens());
    }
  }

  @Test
  void chartGate_unsupportedTopPrediction_notAChart() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "a scatter plot";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      // "scatter_chart" contains "chart" but is not in Docling's
      // SUPPORTED_CHART_TYPES: no chart job, description runs instead.
      Document document = Document.newBuilder().setName("test")
          .addPictures(classifiedPicture("#/pictures/0", "scatter_chart"))
          .build();
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoChartExtraction(true)
          .setDoPictureDescription(true)
          .setDocument(document)
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      assertEquals(0, result.events().get(0).getStarted().getChartExtractions());
      assertEquals(1, result.events().get(0).getStarted().getPictureDescriptions());
      assertEquals(1, annotations(result).size());
      assertTrue(annotations(result).get(0).hasDescription());
    }
  }

  @Test
  void presetPrompts_matchDocling() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "caption";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());

      EnrichOptions smolvlm = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setPictureDescriptionPreset(
              PictureDescriptionPreset.PICTURE_DESCRIPTION_PRESET_SMOLVLM)
          .setDocument(documentOnePictureOneParagraph())
          .build();
      Collected smolvlmResult = runRpc(stub, List.of(optionsRequest(smolvlm)));
      assertNull(smolvlmResult.error());
      assertEquals("Describe this image in a few sentences.",
          vlm.requests.get(vlm.requests.size() - 1).prompt());
      assertEquals(200, vlm.requests.get(vlm.requests.size() - 1).maxTokens());

      EnrichOptions granite = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setPictureDescriptionPreset(
              PictureDescriptionPreset.PICTURE_DESCRIPTION_PRESET_GRANITE_VISION)
          .setDocument(documentOnePictureOneParagraph())
          .build();
      Collected graniteResult = runRpc(stub, List.of(optionsRequest(granite)));
      assertNull(graniteResult.error());
      assertEquals("What is shown in this image?",
          vlm.requests.get(vlm.requests.size() - 1).prompt());
    }
  }

  @Test
  void returnDocument_appliesAnnotations() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "a QR code";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setReturnDocument(true)
          .setDocument(documentOnePictureOneParagraph())
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertNull(result.error());
      var complete = result.events().get(result.events().size() - 1).getComplete();
      assertTrue(complete.hasDocument());
      var picture = complete.getDocument().getPictures(0);
      assertEquals(1, picture.getAnnotationsCount());
      assertEquals("a QR code", picture.getAnnotations(0).getDescription().getText());
      // The paragraph is untouched.
      assertEquals("a paragraph", complete.getDocument().getTexts(0).getText().getBase()
          .getText());
    }
  }

  @Test
  void chunkedUpload_enrichesAfterCompleteChunk() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "chunked picture";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      byte[] bytes = documentOnePictureOneParagraph().toByteArray();
      int half = bytes.length / 2;
      List<EnrichDocumentRequest> requests = List.of(
          optionsRequest(EnrichOptions.newBuilder().setDoPictureDescription(true).build()),
          EnrichDocumentRequest.newBuilder()
              .setChunk(DocumentChunk.newBuilder()
                  .setData(ByteString.copyFrom(bytes, 0, half)))
              .build(),
          EnrichDocumentRequest.newBuilder()
              .setChunk(DocumentChunk.newBuilder()
                  .setData(ByteString.copyFrom(bytes, half, bytes.length - half))
                  .setComplete(true))
              .build());
      Collected result = runRpc(stub, requests);

      assertNull(result.error(), "RPC must be OK: " + result.error());
      assertEquals(1, annotations(result).size());
      assertEquals("chunked picture", annotations(result).get(0).getDescription().getText());
    }
  }

  // -------------------------------------------------------------------------
  // Error matrix
  // -------------------------------------------------------------------------

  @Test
  void firstMessageNotOptions_invalidArgument() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      Collected result = runRpc(stub, List.of(EnrichDocumentRequest.newBuilder()
          .setChunk(DocumentChunk.newBuilder().setData(ByteString.copyFromUtf8("x")))
          .build()));
      assertNotNull(result.error());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.error().getStatus().getCode());
    }
  }

  @Test
  void noDocument_invalidArgument() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      Collected result = runRpc(stub, List.of(
          optionsRequest(EnrichOptions.newBuilder().setDoPictureDescription(true).build())));
      assertNotNull(result.error());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.error().getStatus().getCode());
    }
  }

  @Test
  void chunkBytesNotADocument_invalidArgument() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      Collected result = runRpc(stub, List.of(
          optionsRequest(EnrichOptions.newBuilder().setDoPictureDescription(true).build()),
          EnrichDocumentRequest.newBuilder()
              .setChunk(DocumentChunk.newBuilder()
                  .setData(ByteString.copyFromUtf8("this is not a document"))
                  .setComplete(true))
              .build()));
      // Garbage bytes may or may not parse as a proto; if they do parse the
      // enrichment simply finds no items. Only assert the RPC terminates.
      assertTrue(result.completed() || result.error() != null);
    }
  }

  @Test
  void overByteCap_resourceExhausted() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url(), 64);
      byte[] big = new byte[1024];
      Collected result = runRpc(stub, List.of(
          optionsRequest(EnrichOptions.newBuilder().setDoPictureDescription(true).build()),
          EnrichDocumentRequest.newBuilder()
              .setChunk(DocumentChunk.newBuilder().setData(ByteString.copyFrom(big)))
              .build()));
      assertNotNull(result.error());
      assertEquals(Status.Code.RESOURCE_EXHAUSTED, result.error().getStatus().getCode());
    }
  }

  // -------------------------------------------------------------------------
  // The stream is the product: fail if someone turns it back into a batch
  // -------------------------------------------------------------------------

  @Test
  void eventsFlowBeforeClientHalfCloses() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "live caption";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());

      BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
      StreamObserver<EnrichDocumentResponse> observer = new StreamObserver<>() {
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
      StreamObserver<EnrichDocumentRequest> requester = stub.enrichDocument(observer);
      // Send the options + inline document and deliberately do NOT half-close.
      requester.onNext(optionsRequest(EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDocument(documentOnePictureOneParagraph())
          .build()));

      // A batched implementation waits for onCompleted before doing anything;
      // this test fails if the stream ever works that way.
      Object first = inbox.poll(10, TimeUnit.SECONDS);
      assertNotNull(first, "no event arrived before the client half-closed");
      assertTrue(first instanceof EnrichDocumentResponse event && event.hasStarted(),
          "first event must be EnrichStarted, got: " + first);

      EnrichDocumentResponse complete = null;
      while (complete == null) {
        Object item = inbox.poll(10, TimeUnit.SECONDS);
        assertNotNull(item, "EnrichComplete never arrived");
        assertTrue(item instanceof EnrichDocumentResponse, "RPC failed: " + item);
        EnrichDocumentResponse event = (EnrichDocumentResponse) item;
        if (event.hasComplete()) {
          complete = event;
        }
      }
      assertEquals(1, complete.getComplete().getSucceeded());

      // The server completes the response once the trailer is emitted, even
      // though the client never half-closed.
      assertEquals("DONE", inbox.poll(10, TimeUnit.SECONDS));
      try {
        requester.onCompleted();
      } catch (StatusRuntimeException alreadyClosed) {
        // The server already completed the call; half-closing after that is a no-op.
      }
    }
  }

  @Test
  void perItemEventsFlowBeforeComplete() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      CountDownLatch secondCallGate = new CountDownLatch(1);
      vlm.gates = Map.of(2, secondCallGate);
      vlm.responder = body -> "caption";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());

      Document document = Document.newBuilder()
          .setName("test")
          .addPictures(pictureWithImage("#/pictures/0"))
          .addPictures(pictureWithImage("#/pictures/1"))
          .build();
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setConcurrency(2)
          .setDocument(document)
          .build();

      BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
      StreamObserver<EnrichDocumentResponse> observer = new StreamObserver<>() {
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
      StreamObserver<EnrichDocumentRequest> requester = stub.enrichDocument(observer);
      requester.onNext(optionsRequest(options));
      requester.onCompleted();

      // With the second VLM call still gated, the first item's annotation
      // must already be on the stream: item N is never held for item N-1.
      boolean sawAnnotationBeforeRelease = false;
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (System.nanoTime() < deadline) {
        Object item = inbox.poll(200, TimeUnit.MILLISECONDS);
        if (item instanceof EnrichDocumentResponse event && event.hasAnnotation()) {
          sawAnnotationBeforeRelease = true;
          break;
        }
        assertFalse(item instanceof StatusRuntimeException, "RPC failed: " + item);
      }
      assertTrue(sawAnnotationBeforeRelease,
          "no per-item event arrived while a later VLM call was still in flight");
      secondCallGate.countDown();

      // Now the rest drains: a second annotation, then the trailer.
      int annotations = 0;
      boolean completed = false;
      while (!completed) {
        Object item = inbox.poll(10, TimeUnit.SECONDS);
        assertNotNull(item, "RPC did not finish after the gate opened");
        if (item instanceof EnrichDocumentResponse event) {
          if (event.hasAnnotation()) {
            annotations++;
          }
          if (event.hasComplete()) {
            assertEquals(2, event.getComplete().getSucceeded());
          }
        } else {
          completed = true;
        }
      }
      assertEquals(1, annotations, "exactly one annotation remained after the gate opened");
    }
  }
}
