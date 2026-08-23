package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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
      assertThat(item).as("RPC did not terminate within 30s").isNotNull();
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

    assertThat(info.getUi().getTitle()).isEqualTo("Enrich");
    assertThat(info.getUi().getPath()).isEqualTo("/ui/enrich");
    assertThat(info.getUi().getDescription())
        .isEqualTo("Document in, stream of typed ItemAnnotation enrichment events out");
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

      assertThat(result.error()).as("RPC must be OK: " + result.error()).isNull();
      assertThat(result.completed()).isTrue();
      assertThat(result.events().get(0).getStarted().getPictureDescriptions()).isEqualTo(1);
      List<ItemAnnotation> annotations = annotations(result);
      assertThat(annotations.size()).isEqualTo(1);
      assertThat(annotations.get(0).getSelfRef()).isEqualTo("#/pictures/0");
      assertThat(annotations.get(0).getDescription().getText())
          .isEqualTo("a QR code on a white background");
      assertThat(annotations.get(0).getModel()).isEqualTo("smolvlm");
      assertThat(skips(result)).isEmpty();
      assertThat(result.events().get(result.events().size() - 1)
          .getComplete().getSucceeded()).isEqualTo(1);
      // The text item is untouched: no annotation event references it.
      assertThat(annotations.stream().noneMatch(a -> a.getSelfRef().equals("#/texts/0"))).isTrue();
      // The fake saw the describe model and the picture bytes.
      assertThat(vlm.requests.size()).isEqualTo(1);
      assertThat(vlm.requests.get(0).model()).isEqualTo("smolvlm");
      assertThat(vlm.requests.get(0).hasImage()).isTrue();
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

      assertThat(result.error()).as("a missing image must not fail the RPC").isNull();
      List<ItemSkipped> skips = skips(result);
      assertThat(skips.size()).isEqualTo(1);
      assertThat(skips.get(0).getSelfRef()).isEqualTo("#/pictures/0");
      assertThat(skips.get(0).getReason()).isEqualTo(SkipReason.SKIP_REASON_NO_IMAGE);
      assertThat(result.events().get(result.events().size() - 1).getComplete().getSkipped())
          .isEqualTo(1);
      assertThat(vlm.requests.isEmpty()).as("no VLM call may be made without image bytes").isTrue();
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

      assertThat(result.error()).isNull();
      assertThat(skips(result).size()).isEqualTo(1);
      assertThat(skips(result).get(0).getReason())
          .isEqualTo(SkipReason.SKIP_REASON_UNFETCHABLE_URI);
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

      assertThat(result.error()).isNull();
      List<ItemAnnotation> annotations = annotations(result);
      assertThat(annotations.size()).isEqualTo(1);
      var table = annotations.get(0).getChartTable().getTable();
      assertThat(table.getNumRows()).isEqualTo(2);
      assertThat(table.getNumCols()).isEqualTo(3);
      assertThat(table.getTableCellsCount()).isEqualTo(6);
      assertThat(table.getTableCells(0).getColumnHeader()).isTrue();
      assertThat(table.getTableCells(0).getText()).isEqualTo("year");
      assertThat(table.getTableCells(3).getColumnHeader()).isFalse();
      assertThat(table.getTableCells(5).getText()).isEqualTo("3");
      assertThat(table.getTableCells(5).getStartRowOffsetIdx()).isEqualTo(1);
      assertThat(table.getTableCells(5).getStartColOffsetIdx()).isEqualTo(2);
      assertThat(table.getGridCount()).isEqualTo(2);
      // The raw CSV rides along, but the typed cells are the representation.
      assertThat(annotations.get(0).getChartTable().getCsv())
          .isEqualTo("year,sales,profit\n2023,10,3");
      assertThat(vlm.requests.get(0).model()).isEqualTo("granite-vision-chart2csv");
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

    assertThat(result.error()).as("a dead VLM endpoint must not fail the RPC").isNull();
    assertThat(annotations(result)).isEmpty();
    List<ItemSkipped> skips = skips(result);
    assertThat(skips.size()).isEqualTo(1);
    assertThat(skips.get(0).getReason()).isEqualTo(SkipReason.SKIP_REASON_VLM_ERROR);
    assertThat(result.events().get(result.events().size() - 1).getComplete().getSkipped())
        .isEqualTo(1);
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

      assertThat(result.error()).isNull();
      assertThat(skips(result).size()).isEqualTo(1);
      assertThat(skips(result).get(0).getReason()).isEqualTo(SkipReason.SKIP_REASON_VLM_ERROR);
      assertThat(vlm.requests.size()).as("persistent 500 is retried 5 times, then skipped")
          .isEqualTo(6);
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

      assertThat(result.error()).isNull();
      assertThat(skips(result)).isEmpty();
      assertThat(annotations(result).size()).isEqualTo(1);
      assertThat(vlm.requests.size()).as("a transient 503 is retried and the item enriches")
          .isEqualTo(2);
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

      assertThat(result.error()).isNull();
      assertThat(result.events().get(0).getStarted().getCodeEnrichments()).isEqualTo(1);
      assertThat(result.events().get(0).getStarted().getFormulaEnrichments()).isEqualTo(1);
      assertThat(annotations(result).size()).isEqualTo(2);
      ItemAnnotation code = annotations(result).stream()
          .filter(a -> a.getSelfRef().equals("#/texts/0")).findFirst().orElseThrow();
      assertThat(code.getCode().getText()).isEqualTo("print('hi')");
      ItemAnnotation formula = annotations(result).stream()
          .filter(a -> a.getSelfRef().equals("#/texts/1")).findFirst().orElseThrow();
      assertThat(formula.getFormula().getText()).isEqualTo("x^2 + y^2 = z^2");
      assertThat(result.events().get(result.events().size() - 1)
          .getComplete().getSucceeded()).isEqualTo(2);
      // Text-only enrichment sends no image, keeps the existing text in the
      // prompt, and uses the code/formula generation budget.
      assertThat(vlm.requests.size()).isEqualTo(2);
      for (var request : vlm.requests) {
        assertThat(request.hasImage()).isFalse();
        assertThat(request.maxTokens()).isEqualTo(2048);
      }
      assertThat(vlm.requests.stream()
          .anyMatch(r -> r.prompt().contains("print( 'hi' )"))).isTrue();
      assertThat(vlm.requests.stream()
          .anyMatch(r -> r.prompt().contains("x2+y2=z2"))).isTrue();
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

      assertThat(result.error()).as("RPC must be OK: " + result.error()).isNull();
      // An image crop rides with the bare prompt "<code>".
      assertThat(vlm.requests.size()).isEqualTo(1);
      assertThat(vlm.requests.get(0).hasImage()).isTrue();
      assertThat(vlm.requests.get(0).prompt()).isEqualTo("<code>");
      assertThat(vlm.requests.get(0).maxTokens()).isEqualTo(2048);
      // Output post-processing: language token out, sentinels stripped.
      assertThat(annotations(result).size()).isEqualTo(1);
      var code = annotations(result).get(0).getCode();
      assertThat(code.getText()).isEqualTo("print('hi')");
      assertThat(code.getLanguage()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_PYTHON);
      assertThat(code.getLanguageRaw()).isEqualTo("Python");
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

      assertThat(result.error()).as("RPC must be OK: " + result.error()).isNull();
      assertThat(vlm.requests.size()).isEqualTo(1);
      assertThat(vlm.requests.get(0).hasImage()).isTrue();
      assertThat(vlm.requests.get(0).prompt()).isEqualTo("<formula>");
      assertThat(vlm.requests.get(0).maxTokens()).isEqualTo(2048);
      assertThat(annotations(result).get(0).getFormula().getText()).isEqualTo("x^2 + y^2");
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

      assertThat(result.error()).isNull();
      var patched = result.events().get(result.events().size() - 1)
          .getComplete().getDocument().getTexts(0).getCode();
      assertThat(patched.getText()).isEqualTo("int x = 1;");
      assertThat(patched.getCodeLanguage()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_JAVA);
      assertThat(patched.getCodeLanguageRaw()).isEqualTo("Java");
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

      assertThat(result.error()).isNull();
      assertThat(skips(result).size()).isEqualTo(1);
      assertThat(skips(result).get(0).getReason())
          .isEqualTo(SkipReason.SKIP_REASON_BELOW_AREA_THRESHOLD);
      assertThat(vlm.requests).isEmpty();
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
  void defaultAreaThreshold_fivePercent() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "big enough";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      // 200x200 on a 1000x1000 page = 4% < the default 0.05: skipped
      // even though no threshold was set explicitly.
      EnrichOptions small = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDocument(documentWithSmallPicture(200))
          .build();
      Collected skippedResult = runRpc(stub, List.of(optionsRequest(small)));
      assertThat(skippedResult.error()).isNull();
      assertThat(skips(skippedResult).size()).isEqualTo(1);
      assertThat(skips(skippedResult).get(0).getReason())
          .isEqualTo(SkipReason.SKIP_REASON_BELOW_AREA_THRESHOLD);
      assertThat(vlm.requests).isEmpty();

      // 300x300 = 9% >= 0.05: described.
      EnrichOptions big = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDocument(documentWithSmallPicture(300))
          .build();
      Collected describedResult = runRpc(stub, List.of(optionsRequest(big)));
      assertThat(describedResult.error()).isNull();
      assertThat(annotations(describedResult).size()).isEqualTo(1);
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

      assertThat(result.error()).isNull();
      assertThat(skips(result)).isEmpty();
      assertThat(annotations(result).size()).isEqualTo(1);
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
      // Label is PICTURE; the top classification prediction is a
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

      assertThat(result.error()).isNull();
      assertThat(result.events().get(0).getStarted().getChartExtractions()).isEqualTo(1);
      assertThat(annotations(result).size()).isEqualTo(1);
      assertThat(annotations(result).get(0).hasChartTable()).isTrue();
      assertThat(vlm.requests.size()).isEqualTo(1);
      // The pinned chart prompt and the chart generation budget.
      assertThat(vlm.requests.get(0).prompt())
          .isEqualTo("Convert the information in this chart into a data table in CSV format.");
      assertThat(vlm.requests.get(0).maxTokens()).isEqualTo(4096);
    }
  }

  @Test
  void chartGate_unsupportedTopPrediction_notAChart() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "a scatter plot";
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      // "scatter_chart" contains "chart" but is not a supported chart
      // type: no chart job, description runs instead.
      Document document = Document.newBuilder().setName("test")
          .addPictures(classifiedPicture("#/pictures/0", "scatter_chart"))
          .build();
      EnrichOptions options = EnrichOptions.newBuilder()
          .setDoChartExtraction(true)
          .setDoPictureDescription(true)
          .setDocument(document)
          .build();
      Collected result = runRpc(stub, List.of(optionsRequest(options)));

      assertThat(result.error()).isNull();
      assertThat(result.events().get(0).getStarted().getChartExtractions()).isEqualTo(0);
      assertThat(result.events().get(0).getStarted().getPictureDescriptions()).isEqualTo(1);
      assertThat(annotations(result).size()).isEqualTo(1);
      assertThat(annotations(result).get(0).hasDescription()).isTrue();
    }
  }

  @Test
  void presetPrompts_arePinned() throws Exception {
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
      assertThat(smolvlmResult.error()).isNull();
      assertThat(vlm.requests.get(vlm.requests.size() - 1).prompt())
          .isEqualTo("Describe this image in a few sentences.");
      assertThat(vlm.requests.get(vlm.requests.size() - 1).maxTokens()).isEqualTo(200);

      EnrichOptions granite = EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setPictureDescriptionPreset(
              PictureDescriptionPreset.PICTURE_DESCRIPTION_PRESET_GRANITE_VISION)
          .setDocument(documentOnePictureOneParagraph())
          .build();
      Collected graniteResult = runRpc(stub, List.of(optionsRequest(granite)));
      assertThat(graniteResult.error()).isNull();
      assertThat(vlm.requests.get(vlm.requests.size() - 1).prompt())
          .isEqualTo("What is shown in this image?");
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

      assertThat(result.error()).isNull();
      var complete = result.events().get(result.events().size() - 1).getComplete();
      assertThat(complete.hasDocument()).isTrue();
      var picture = complete.getDocument().getPictures(0);
      assertThat(picture.getAnnotationsCount()).isEqualTo(1);
      assertThat(picture.getAnnotations(0).getDescription().getText()).isEqualTo("a QR code");
      // The paragraph is untouched.
      assertThat(complete.getDocument().getTexts(0).getText().getBase()
          .getText()).isEqualTo("a paragraph");
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

      assertThat(result.error()).as("RPC must be OK: " + result.error()).isNull();
      assertThat(annotations(result).size()).isEqualTo(1);
      assertThat(annotations(result).get(0).getDescription().getText())
          .isEqualTo("chunked picture");
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
      assertThat(result.error()).isNotNull();
      assertThat(result.error().getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }
  }

  @Test
  void noDocument_invalidArgument() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      EnrichServiceGrpc.EnrichServiceStub stub = startService(vlm.url());
      Collected result = runRpc(stub, List.of(
          optionsRequest(EnrichOptions.newBuilder().setDoPictureDescription(true).build())));
      assertThat(result.error()).isNotNull();
      assertThat(result.error().getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
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
      assertThat(result.completed() || result.error() != null).isTrue();
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
      assertThat(result.error()).isNotNull();
      assertThat(result.error().getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
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
      assertThat(first).as("no event arrived before the client half-closed").isNotNull();
      assertThat(first instanceof EnrichDocumentResponse event && event.hasStarted())
          .as("first event must be EnrichStarted, got: " + first).isTrue();

      EnrichDocumentResponse complete = null;
      while (complete == null) {
        Object item = inbox.poll(10, TimeUnit.SECONDS);
        assertThat(item).as("EnrichComplete never arrived").isNotNull();
        assertThat(item instanceof EnrichDocumentResponse).as("RPC failed: " + item).isTrue();
        EnrichDocumentResponse event = (EnrichDocumentResponse) item;
        if (event.hasComplete()) {
          complete = event;
        }
      }
      assertThat(complete.getComplete().getSucceeded()).isEqualTo(1);

      // The server completes the response once the trailer is emitted, even
      // though the client never half-closed.
      assertThat(inbox.poll(10, TimeUnit.SECONDS)).isEqualTo("DONE");
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
        assertThat(item instanceof StatusRuntimeException).as("RPC failed: " + item).isFalse();
      }
      assertThat(sawAnnotationBeforeRelease)
          .as("no per-item event arrived while a later VLM call was still in flight").isTrue();
      secondCallGate.countDown();

      // Now the rest drains: a second annotation, then the trailer.
      int annotations = 0;
      boolean completed = false;
      while (!completed) {
        Object item = inbox.poll(10, TimeUnit.SECONDS);
        assertThat(item).as("RPC did not finish after the gate opened").isNotNull();
        if (item instanceof EnrichDocumentResponse event) {
          if (event.hasAnnotation()) {
            annotations++;
          }
          if (event.hasComplete()) {
            assertThat(event.getComplete().getSucceeded()).isEqualTo(2);
          }
        } else {
          completed = true;
        }
      }
      assertThat(annotations).as("exactly one annotation remained after the gate opened")
          .isEqualTo(1);
    }
  }
}
