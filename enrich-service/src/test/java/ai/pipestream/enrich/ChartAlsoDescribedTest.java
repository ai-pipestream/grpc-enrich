package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.document.v1.BoundingBox;
import ai.pipestream.document.v1.DocItemLabel;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.PageItem;
import ai.pipestream.document.v1.PictureItem;
import ai.pipestream.document.v1.ProvenanceItem;
import ai.pipestream.document.v1.Size;
import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.v1.ChartOutput;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.SkipReason;
import org.junit.jupiter.api.Test;

/**
 * Picture description and chart extraction are two jobs. A chart picture
 * with both requested gets both: the chart job never takes the description
 * job's place without a word, and a failed chart call does not leave the
 * picture with nothing.
 */
class ChartAlsoDescribedTest {

  private static PictureItem chart(String selfRef) {
    return InProcessEnrich.picture(selfRef).toBuilder()
        .setLabel(DocItemLabel.DOC_ITEM_LABEL_CHART)
        .build();
  }

  private static String reply(String body) {
    return body.contains("CSV format") ? "year,sales\n2023,10" : "a bar chart of sales";
  }

  @Test
  void chartPictureWithBothJobs_isDescribedAndExtracted() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.responder = ChartAlsoDescribedTest::reply;
      InProcessEnrich.Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDoChartExtraction(true)
          .setDocument(Document.newBuilder().setName("chart").addPictures(chart("#/pictures/0")))
          .build());

      assertThat(result.error()).isNull();
      assertThat(result.events().get(0).getStarted().getChartExtractions()).isEqualTo(1);
      assertThat(result.events().get(0).getStarted().getPictureDescriptions())
          .as("the chart is also selected for description")
          .isEqualTo(1);
      assertThat(result.annotations()).hasSize(2)
          .allSatisfy(annotation ->
              assertThat(annotation.getSelfRef()).isEqualTo("#/pictures/0"));
      assertThat(result.annotations()).anySatisfy(annotation ->
          assertThat(annotation.getChartTable().getTable().getNumRows()).isEqualTo(2));
      assertThat(result.annotations()).anySatisfy(annotation ->
          assertThat(annotation.getDescription().getText()).isEqualTo("a bar chart of sales"));
      assertThat(result.complete().getSucceeded()).isEqualTo(2);
    }
  }

  @Test
  void failedChartCall_stillLeavesTheDescription() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      // The chart call is answered with nothing a table can be read from.
      vlm.responder = body -> body.contains("CSV format") ? "" : "a bar chart of sales";
      InProcessEnrich.Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDoChartExtraction(true)
          .setDocument(Document.newBuilder().setName("chart").addPictures(chart("#/pictures/0")))
          .build());

      assertThat(result.error()).isNull();
      assertThat(result.skips()).singleElement().satisfies(skip -> {
        assertThat(skip.getReason()).isEqualTo(SkipReason.SKIP_REASON_VLM_ERROR);
        assertThat(skip.getChartOutput()).isEqualTo(ChartOutput.CHART_OUTPUT_CSV);
      });
      assertThat(result.annotations()).singleElement().satisfies(annotation ->
          assertThat(annotation.getDescription().getText()).isEqualTo("a bar chart of sales"));
    }
  }

  @Test
  void smallChartWithBothJobs_isExtractedAndItsDescriptionSkipIsReported() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.responder = ChartAlsoDescribedTest::reply;
      // 10x10 on a 100x100 page: 1% of the page, under the 5% default.
      PictureItem small = chart("#/pictures/0").toBuilder()
          .addProv(ProvenanceItem.newBuilder()
              .setPageNo(1)
              .setBbox(BoundingBox.newBuilder().setL(0).setT(0).setR(10).setB(10)))
          .build();
      Document document = Document.newBuilder()
          .setName("small-chart")
          .putPages(1, PageItem.newBuilder()
              .setPageNo(1)
              .setSize(Size.newBuilder().setWidth(100).setHeight(100))
              .build())
          .addPictures(small)
          .build();
      InProcessEnrich.Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDoChartExtraction(true)
          .setDocument(document)
          .build());

      assertThat(result.error()).isNull();
      assertThat(result.annotations()).singleElement()
          .satisfies(annotation -> assertThat(annotation.hasChartTable()).isTrue());
      assertThat(result.skips()).singleElement().satisfies(skip -> {
        assertThat(skip.getSelfRef()).isEqualTo("#/pictures/0");
        assertThat(skip.getReason()).isEqualTo(SkipReason.SKIP_REASON_BELOW_AREA_THRESHOLD);
        assertThat(skip.getChartOutput()).isEqualTo(ChartOutput.CHART_OUTPUT_UNSPECIFIED);
      });
    }
  }

  @Test
  void chartPictureWithOnlyTheChartJob_isNotDescribed() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.responder = ChartAlsoDescribedTest::reply;
      InProcessEnrich.Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoChartExtraction(true)
          .setDocument(Document.newBuilder().setName("chart").addPictures(chart("#/pictures/0")))
          .build());

      assertThat(result.error()).isNull();
      assertThat(vlm.calls()).isEqualTo(1);
      assertThat(result.events().get(0).getStarted().getPictureDescriptions()).isZero();
      assertThat(result.annotations()).singleElement()
          .satisfies(annotation -> assertThat(annotation.hasChartTable()).isTrue());
    }
  }
}
