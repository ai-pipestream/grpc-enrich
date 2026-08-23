package ai.pipestream.enrich;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.pipestream.document.v1.BoundingBox;
import ai.pipestream.document.v1.DocItemLabel;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.ImageRef;
import ai.pipestream.document.v1.PageItem;
import ai.pipestream.document.v1.PictureAnnotation;
import ai.pipestream.document.v1.PictureClassificationData;
import ai.pipestream.document.v1.PictureItem;
import ai.pipestream.document.v1.ProvenanceItem;
import ai.pipestream.document.v1.Size;
import ai.pipestream.enrich.engine.ItemSelector;
import ai.pipestream.enrich.engine.ItemSelector.Selection;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.SkipReason;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Adversarial area-threshold and selection cases: degenerate bboxes, missing
 * pages, hostile threshold values, multiple provenance boxes.
 */
class ItemSelectorAdversarialTest {

  private static final String PNG_DATA_URI =
      "data:image/png;base64," + Base64.getEncoder().encodeToString(new byte[] {1, 2, 3});

  private static PictureItem picture(String selfRef, double l, double t, double r, double b) {
    PictureItem.Builder picture = PictureItem.newBuilder()
        .setSelfRef(selfRef)
        .setLabel(DocItemLabel.DOC_ITEM_LABEL_PICTURE)
        .setImage(ImageRef.newBuilder().setUri(PNG_DATA_URI));
    if (!Double.isNaN(l)) {
      picture.addProv(ProvenanceItem.newBuilder()
          .setPageNo(1)
          .setBbox(BoundingBox.newBuilder().setL(l).setT(t).setR(r).setB(b)));
    }
    return picture.build();
  }

  private static Document documentOnPage(PictureItem picture, double width, double height) {
    return Document.newBuilder()
        .setName("test")
        .addPictures(picture)
        .putPages(1, PageItem.newBuilder()
            .setPageNo(1)
            .setSize(Size.newBuilder().setWidth(width).setHeight(height))
            .build())
        .build();
  }

  private static Selection describe(Document document, double threshold) {
    EnrichOptions.Builder options = EnrichOptions.newBuilder().setDoPictureDescription(true);
    if (!Double.isNaN(threshold)) {
      options.setPictureDescriptionAreaThreshold(threshold);
    }
    return ItemSelector.select(document, Map.of(), options.build());
  }

  private static Selection describeNaN(Document document) {
    return ItemSelector.select(document, Map.of(), EnrichOptions.newBuilder()
        .setDoPictureDescription(true)
        .setPictureDescriptionAreaThreshold(Double.NaN)
        .build());
  }

  @Test
  void invertedBbox_areaUsesAbsoluteExtents() {
    // r < l and b < t: the area must be computed from absolute extents, not
    // go negative (a negative ratio would always fail) or huge.
    Document document = documentOnPage(picture("#/pictures/0", 800, 800, 200, 200), 1000, 1000);
    Selection selection = describe(document, 0.05);
    assertEquals(1, selection.work().size());
    assertTrue(selection.skips().isEmpty());
  }

  @Test
  void zeroAreaBbox_belowThreshold() {
    Document document = documentOnPage(picture("#/pictures/0", 100, 100, 100, 100), 1000, 1000);
    Selection selection = describe(document, 0.05);
    assertTrue(selection.work().isEmpty());
    assertEquals(SkipReason.SKIP_REASON_BELOW_AREA_THRESHOLD,
        selection.skips().get(0).getReason());
  }

  @Test
  void negativeCoordinates_areaAbsolute() {
    Document document = documentOnPage(picture("#/pictures/0", -500, -500, 500, 500), 1000, 1000);
    // 1000x1000 on a 1000x1000 page = ratio 1.0.
    Selection selection = describe(document, 0.99);
    assertEquals(1, selection.work().size());
  }

  @Test
  void thresholdExactlyAtRatio_passes() {
    // 500x500 on 1000x1000 = 0.25; == threshold must pass (>= semantics).
    Document document = documentOnPage(picture("#/pictures/0", 0, 0, 500, 500), 1000, 1000);
    Selection selection = describe(document, 0.25);
    assertEquals(1, selection.work().size());
    assertTrue(selection.skips().isEmpty());
  }

  @Test
  void epsilonBelowThreshold_skips() {
    Document document = documentOnPage(picture("#/pictures/0", 0, 0, 500, 500), 1000, 1000);
    Selection selection = describe(document, 0.250001);
    assertTrue(selection.work().isEmpty());
    assertEquals(1, selection.skips().size());
  }

  @Test
  void provReferencingMissingPage_treatedAsFullPage() {
    Document document = Document.newBuilder()
        .setName("test")
        .addPictures(picture("#/pictures/0", 0, 0, 1, 1)) // tiny, but page 1 unknown
        .build();
    Selection selection = describe(document, 0.99);
    assertEquals(1, selection.work().size());
  }

  @Test
  void zeroPageSize_treatedAsFullPage() {
    Document document = documentOnPage(picture("#/pictures/0", 0, 0, 1, 1), 0, 0);
    Selection selection = describe(document, 0.99);
    assertEquals(1, selection.work().size());
  }

  @Test
  void onlyFirstProvCounts() {
    // prov[0] is tiny, prov[1] is huge: only prov[0] may drive the decision.
    PictureItem picture = picture("#/pictures/0", 0, 0, 10, 10).toBuilder()
        .addProv(ProvenanceItem.newBuilder()
            .setPageNo(1)
            .setBbox(BoundingBox.newBuilder().setL(0).setT(0).setR(1000).setB(1000)))
        .build();
    Document document = documentOnPage(picture, 1000, 1000);
    Selection selection = describe(document, 0.5);
    assertTrue(selection.work().isEmpty());
    assertEquals(SkipReason.SKIP_REASON_BELOW_AREA_THRESHOLD,
        selection.skips().get(0).getReason());
  }

  @Test
  void noProv_treatedAsFullPage() {
    Document document = documentOnPage(picture("#/pictures/0",
        Double.NaN, Double.NaN, Double.NaN, Double.NaN), 1000, 1000);
    Selection selection = describe(document, 0.99);
    assertEquals(1, selection.work().size());
  }

  @Test
  void nanThreshold_fallsBackToDefault() {
    // NaN is meaningless as a threshold; it must not silently skip every
    // picture (NaN comparisons are all false). Treat it like unset: the
    // default 0.05 applies — a 4% picture is skipped, a 9% one described.
    Document small = documentOnPage(picture("#/pictures/0", 0, 0, 200, 200), 1000, 1000);
    Selection smallSelection = describeNaN(small);
    assertEquals(1, smallSelection.skips().size());
    assertEquals(SkipReason.SKIP_REASON_BELOW_AREA_THRESHOLD,
        smallSelection.skips().get(0).getReason());

    Document big = documentOnPage(picture("#/pictures/0", 0, 0, 300, 300), 1000, 1000);
    Selection bigSelection = describeNaN(big);
    assertEquals(1, bigSelection.work().size());
  }

  @Test
  void hugeThreshold_skipsEverything() {
    Document document = documentOnPage(picture("#/pictures/0", 0, 0, 1000, 1000), 1000, 1000);
    Selection selection = describe(document, 1e9);
    assertTrue(selection.work().isEmpty());
    assertEquals(SkipReason.SKIP_REASON_BELOW_AREA_THRESHOLD,
        selection.skips().get(0).getReason());
  }

  @Test
  void thresholdAboveOne_skipsEvenFullPagePictures() {
    Document document = documentOnPage(picture("#/pictures/0", 0, 0, 1000, 1000), 1000, 1000);
    Selection selection = describe(document, 1.01);
    assertTrue(selection.work().isEmpty());
  }

  @Test
  void classificationWithEmptyPredictedClasses_notAChart() {
    PictureItem picture = picture("#/pictures/0", 0, 0, 1000, 1000).toBuilder()
        .addAnnotations(PictureAnnotation.newBuilder()
            .setClassification(PictureClassificationData.newBuilder().setKind("classification")))
        .build();
    Document document = documentOnPage(picture, 1000, 1000);
    Selection selection = ItemSelector.select(document, Map.of(),
        EnrichOptions.newBuilder().setDoChartExtraction(true).build());
    assertTrue(selection.work().isEmpty());
    // Not a chart and chart extraction doesn't describe: no skip either.
    assertTrue(selection.skips().isEmpty());
  }

  @Test
  void classificationWithEmptyClassName_notAChart() {
    PictureItem picture = picture("#/pictures/0", 0, 0, 1000, 1000).toBuilder()
        .addAnnotations(PictureAnnotation.newBuilder()
            .setClassification(PictureClassificationData.newBuilder()
                .setKind("classification")
                .addPredictedClasses(ai.pipestream.document.v1.PictureClassificationClass
                    .newBuilder().setClassName("").setConfidence(0.9))))
        .build();
    Document document = documentOnPage(picture, 1000, 1000);
    Selection selection = ItemSelector.select(document, Map.of(),
        EnrichOptions.newBuilder().setDoChartExtraction(true).build());
    assertTrue(selection.work().isEmpty());
  }

  @Test
  void chartClassCaseInsensitive() {
    PictureItem picture = picture("#/pictures/0", 0, 0, 1000, 1000).toBuilder()
        .addAnnotations(PictureAnnotation.newBuilder()
            .setClassification(PictureClassificationData.newBuilder()
                .setKind("classification")
                .addPredictedClasses(ai.pipestream.document.v1.PictureClassificationClass
                    .newBuilder().setClassName("Bar_Chart").setConfidence(0.9))))
        .build();
    Document document = documentOnPage(picture, 1000, 1000);
    Selection selection = ItemSelector.select(document, Map.of(),
        EnrichOptions.newBuilder().setDoChartExtraction(true).build());
    assertEquals(1, selection.work().size());
    assertEquals(ItemSelector.Kind.CHART, selection.work().get(0).kind());
  }

  @Test
  void secondPredictionChartType_notAChart() {
    // Only the TOP prediction counts.
    PictureItem picture = picture("#/pictures/0", 0, 0, 1000, 1000).toBuilder()
        .addAnnotations(PictureAnnotation.newBuilder()
            .setClassification(PictureClassificationData.newBuilder()
                .setKind("classification")
                .addPredictedClasses(ai.pipestream.document.v1.PictureClassificationClass
                    .newBuilder().setClassName("scatter_plot").setConfidence(0.9))
                .addPredictedClasses(ai.pipestream.document.v1.PictureClassificationClass
                    .newBuilder().setClassName("bar_chart").setConfidence(0.1))))
        .build();
    Document document = documentOnPage(picture, 1000, 1000);
    Selection selection = ItemSelector.select(document, Map.of(),
        EnrichOptions.newBuilder().setDoChartExtraction(true).build());
    assertTrue(selection.work().isEmpty());
  }
}
