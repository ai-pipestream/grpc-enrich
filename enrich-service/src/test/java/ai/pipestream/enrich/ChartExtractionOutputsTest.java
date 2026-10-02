package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.document.v1.CodeLanguageLabel;
import ai.pipestream.document.v1.DocItemLabel;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.ImageRef;
import ai.pipestream.document.v1.PictureItem;
import ai.pipestream.document.v1.Size;
import ai.pipestream.enrich.engine.EnrichmentEngine;
import ai.pipestream.enrich.server.EnrichServiceImpl;
import ai.pipestream.enrich.v1.ChartExtractionOptions;
import ai.pipestream.enrich.v1.ChartOutput;
import ai.pipestream.enrich.v1.EnrichComplete;
import ai.pipestream.enrich.v1.EnrichDocumentRequest;
import ai.pipestream.enrich.v1.EnrichDocumentResponse;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.EnrichServiceGrpc;
import ai.pipestream.enrich.v1.ItemAnnotation;
import ai.pipestream.enrich.v1.ItemSkipped;
import ai.pipestream.enrich.v1.SkipReason;
import ai.pipestream.enrich.vlm.OpenAiCompatVlmClient;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The Docling chart stage's three outputs (chart2csv, chart2summary,
 * chart2code) over the EnrichDocument stream, against a fake VLM endpoint:
 * one call per enabled output with Docling's prompts, Docling's
 * post-processing, per-output skips that never cost the other outputs, the
 * chart-only model and endpoint overrides, and the patched document.
 */
class ChartExtractionOutputsTest {

  private static final String PNG_DATA_URI = "data:image/png;base64,"
      + Base64.getEncoder().encodeToString(new byte[] {(byte) 0x89, 'P', 'N', 'G', 1, 2, 3});

  private static final String CSV_REPLY = "Here is the table:\n```csv\nyear,sales\n2023,10\n```\n";
  private static final String SUMMARY_REPLY = "A bar chart of sales by year.";
  private static final String CODE_REPLY =
      "Sure.\n```python\nimport matplotlib.pyplot as plt\nplt.bar([2023], [10])\n```\nDone.";

  private final List<AutoCloseable> cleanups = new ArrayList<>();

  @AfterEach
  void tearDown() throws Exception {
    for (AutoCloseable cleanup : cleanups) {
      cleanup.close();
    }
  }

  private static PictureItem chart(String selfRef) {
    return PictureItem.newBuilder()
        .setSelfRef(selfRef)
        .setLabel(DocItemLabel.DOC_ITEM_LABEL_CHART)
        .setImage(ImageRef.newBuilder()
            .setMimetype("image/png")
            .setSize(Size.newBuilder().setWidth(8).setHeight(8))
            .setUri(PNG_DATA_URI))
        .build();
  }

  private static PictureItem picture(String selfRef) {
    return chart(selfRef).toBuilder().setLabel(DocItemLabel.DOC_ITEM_LABEL_PICTURE).build();
  }

  /** Answers each chart prompt (special token or natural language) with its
   * canned reply, so the test does not depend on call order. */
  private static String replyFor(String body) {
    if (body.contains("<chart2summary>") || body.contains("Describe this chart")) {
      return SUMMARY_REPLY;
    }
    if (body.contains("<chart2code>") || body.contains("matplotlib")) {
      return CODE_REPLY;
    }
    return CSV_REPLY;
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

  private record Collected(List<EnrichDocumentResponse> events, StatusRuntimeException error) {

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

  private static Collected run(EnrichServiceGrpc.EnrichServiceStub stub, EnrichOptions options)
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
    requester.onNext(EnrichDocumentRequest.newBuilder().setOptions(options).build());
    requester.onCompleted();
    List<EnrichDocumentResponse> events = new ArrayList<>();
    while (true) {
      Object item = inbox.poll(30, TimeUnit.SECONDS);
      assertThat(item).as("RPC did not terminate within 30s").isNotNull();
      if (item instanceof EnrichDocumentResponse event) {
        events.add(event);
      } else if (item instanceof StatusRuntimeException error) {
        return new Collected(events, error);
      } else {
        return new Collected(events, null);
      }
    }
  }

  private static EnrichOptions.Builder chartOptions(ChartExtractionOptions chart, Document doc) {
    return EnrichOptions.newBuilder()
        .setDoChartExtraction(true)
        .setChartExtraction(chart)
        .setDocument(doc);
  }

  private static Document oneChart() {
    return Document.newBuilder().setName("charts").addPictures(chart("#/pictures/0")).build();
  }

  @Test
  void threeOutputs_oneCallEach_specialTokens_typedAnnotations() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = ChartExtractionOutputsTest::replyFor;
      Collected result = run(startService(vlm.url()), chartOptions(
          ChartExtractionOptions.newBuilder()
              .setSummary(true)
              .setCode(true)
              .setModel("granite-vision-4.1-4b")
              .build(),
          oneChart()).build());

      assertThat(result.error()).isNull();
      // One chart selected, answered by three events.
      assertThat(result.events().get(0).getStarted().getChartExtractions()).isEqualTo(1);
      assertThat(vlm.requests).hasSize(3);
      assertThat(vlm.requests.stream().map(FakeVlmServer.RecordedRequest::prompt))
          .containsExactlyInAnyOrder("<chart2csv>", "<chart2summary>", "<chart2code>");
      assertThat(vlm.requests).allSatisfy(request -> {
        assertThat(request.model()).isEqualTo("granite-vision-4.1-4b");
        assertThat(request.hasImage()).isTrue();
      });

      List<ItemAnnotation> annotations = result.annotations();
      assertThat(annotations).hasSize(3);
      assertThat(annotations).allSatisfy(annotation -> {
        assertThat(annotation.getSelfRef()).isEqualTo("#/pictures/0");
        assertThat(annotation.getModel()).isEqualTo("granite-vision-4.1-4b");
      });
      ItemAnnotation table = annotations.stream().filter(ItemAnnotation::hasChartTable)
          .findFirst().orElseThrow();
      // The fenced csv block is what gets parsed; the prose around it is not.
      assertThat(table.getChartTable().getCsv()).isEqualTo("year,sales\n2023,10");
      assertThat(table.getChartTable().getTable().getNumRows()).isEqualTo(2);
      assertThat(table.getChartTable().getTable().getTableCells(0).getColumnHeader()).isTrue();
      ItemAnnotation summary = annotations.stream().filter(ItemAnnotation::hasChartSummary)
          .findFirst().orElseThrow();
      assertThat(summary.getChartSummary().getText()).isEqualTo(SUMMARY_REPLY);
      ItemAnnotation code = annotations.stream().filter(ItemAnnotation::hasChartCode)
          .findFirst().orElseThrow();
      assertThat(code.getChartCode().getText())
          .isEqualTo("import matplotlib.pyplot as plt\nplt.bar([2023], [10])");
      assertThat(code.getChartCode().getLanguage())
          .isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_PYTHON);
      assertThat(result.complete().getSucceeded()).isEqualTo(3);
      assertThat(result.complete().getSkipped()).isZero();
    }
  }

  @Test
  void naturalLanguagePrompts_areDoclingsVerbatim() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = ChartExtractionOutputsTest::replyFor;
      Collected result = run(startService(vlm.url()), chartOptions(
          ChartExtractionOptions.newBuilder()
              .setSummary(true)
              .setCode(true)
              .setNaturalLanguagePrompts(true)
              .build(),
          oneChart()).build());

      assertThat(result.error()).isNull();
      assertThat(vlm.requests.stream().map(FakeVlmServer.RecordedRequest::prompt))
          .containsExactlyInAnyOrder(
              "Convert the information in this chart into a data table in CSV format "
                  + "with a header row and numeric values.",
              "Describe this chart in a few sentences.",
              "Write Python code using matplotlib that recreates this chart. "
                  + "Return only a fenced ```python code block.");
      assertThat(result.annotations()).hasSize(3);
    }
  }

  @Test
  void csvSwitchedOff_onlySummaryRuns() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = ChartExtractionOutputsTest::replyFor;
      Collected result = run(startService(vlm.url()), chartOptions(
          ChartExtractionOptions.newBuilder().setCsv(false).setSummary(true).build(),
          oneChart()).build());

      assertThat(result.error()).isNull();
      assertThat(vlm.requests).hasSize(1);
      assertThat(vlm.requests.get(0).prompt()).isEqualTo("<chart2summary>");
      assertThat(result.annotations()).singleElement()
          .satisfies(annotation -> assertThat(annotation.hasChartSummary()).isTrue());
    }
  }

  @Test
  void codeWithoutFence_skipsOnlyThatOutput() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> body.contains("<chart2code>")
          ? "plt.bar([1], [2])  # no fence"
          : body.contains("<chart2summary>") ? "   " : CSV_REPLY;
      Collected result = run(startService(vlm.url()), chartOptions(
          ChartExtractionOptions.newBuilder().setSummary(true).setCode(true).build(),
          oneChart()).build());

      assertThat(result.error()).isNull();
      // The table still lands; the unfenced code and the blank summary are
      // each skipped on their own, named by output.
      assertThat(result.annotations()).singleElement()
          .satisfies(annotation -> assertThat(annotation.hasChartTable()).isTrue());
      assertThat(result.skips()).hasSize(2);
      assertThat(result.skips()).allSatisfy(skip -> {
        assertThat(skip.getSelfRef()).isEqualTo("#/pictures/0");
        assertThat(skip.getReason()).isEqualTo(SkipReason.SKIP_REASON_VLM_ERROR);
      });
      assertThat(result.skips().stream().map(ItemSkipped::getChartOutput))
          .containsExactlyInAnyOrder(
              ChartOutput.CHART_OUTPUT_CODE, ChartOutput.CHART_OUTPUT_SUMMARY);
      assertThat(result.complete().getSucceeded()).isEqualTo(1);
      assertThat(result.complete().getSkipped()).isEqualTo(2);
    }
  }

  @Test
  void chartEndpoint_routesChartCallsOnly() throws Exception {
    try (FakeVlmServer general = new FakeVlmServer();
        FakeVlmServer chartModel = new FakeVlmServer()) {
      general.responder = body -> "a photo";
      chartModel.responder = ChartExtractionOutputsTest::replyFor;
      Document document = Document.newBuilder()
          .setName("mixed")
          .addPictures(chart("#/pictures/0"))
          .addPictures(picture("#/pictures/1"))
          .build();
      Collected result = run(startService(general.url()), chartOptions(
          ChartExtractionOptions.newBuilder()
              .setSummary(true)
              .setVlmEndpoint(chartModel.url())
              .build(),
          document).setDoPictureDescription(true).build());

      assertThat(result.error()).isNull();
      assertThat(chartModel.requests).hasSize(2);
      assertThat(general.requests).singleElement()
          .satisfies(request -> assertThat(request.prompt())
              .isEqualTo("Describe this image in a few sentences."));
      assertThat(result.annotations()).hasSize(3);
    }
  }

  @Test
  void noOutputEnabled_isInvalidArgument() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      Collected result = run(startService(vlm.url()), chartOptions(
          ChartExtractionOptions.newBuilder().setCsv(false).build(), oneChart()).build());

      assertThat(result.error()).isNotNull();
      assertThat(result.error().getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(result.error().getStatus().getDescription())
          .contains("at least one of csv, summary, or code");
      assertThat(vlm.requests).isEmpty();
    }
  }

  @Test
  void returnDocument_foldsSummaryAndCodeIntoMeta() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = ChartExtractionOutputsTest::replyFor;
      Collected result = run(startService(vlm.url()), chartOptions(
          ChartExtractionOptions.newBuilder()
              .setSummary(true)
              .setCode(true)
              .setModel("chart-model")
              .build(),
          oneChart()).setReturnDocument(true).build());

      assertThat(result.error()).isNull();
      PictureItem patched = result.complete().getDocument().getPictures(0);
      assertThat(patched.getMeta().getDescription().getText()).isEqualTo(SUMMARY_REPLY);
      assertThat(patched.getMeta().getDescription().getCreatedBy()).isEqualTo("chart-model");
      assertThat(patched.getMeta().getCode().getText()).startsWith("import matplotlib");
      assertThat(patched.getMeta().getCode().getLanguage())
          .isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_PYTHON);
      assertThat(patched.getMeta().getCode().getCreatedBy()).isEqualTo("chart-model");
      assertThat(patched.getAnnotationsList().stream().anyMatch(a -> a.hasTabularChart()))
          .isTrue();
    }
  }

  @Test
  void withoutChartExtractionOptions_theOriginalSingleCsvCallStays() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.responder = body -> "year,sales\n2023,10";
      Collected result = run(startService(vlm.url()), EnrichOptions.newBuilder()
          .setDoChartExtraction(true)
          .setChartPresetRaw("some-model")
          .setDocument(oneChart())
          .build());

      assertThat(result.error()).isNull();
      assertThat(vlm.requests).singleElement().satisfies(request -> {
        assertThat(request.prompt())
            .isEqualTo("Convert the information in this chart into a data table in CSV format.");
        assertThat(request.model()).isEqualTo("some-model");
      });
      assertThat(result.annotations()).singleElement()
          .satisfies(annotation -> assertThat(annotation.hasChartTable()).isTrue());
    }
  }
}
