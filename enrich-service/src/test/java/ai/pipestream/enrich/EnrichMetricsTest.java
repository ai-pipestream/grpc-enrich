package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.PictureItem;
import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.engine.EnrichMetrics;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.SkipReason;
import org.junit.jupiter.api.Test;

/**
 * The operator's view says what happened to the items, not only that a
 * document arrived. With the VLM down, the counters and the metrics line
 * show every item skipped and none annotated.
 */
class EnrichMetricsTest {

  @Test
  void vlmDown_metricsShowNothingWasAnnotated() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.status = 400;
      InProcessEnrich.Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDocument(InProcessEnrich.pictures(3))
          .build());
      assertThat(result.error()).isNull();
      assertThat(result.complete().getSkipped()).isEqualTo(3);

      EnrichMetrics metrics = enrich.engine.metrics();
      assertThat(metrics.documentsCompleted()).isEqualTo(1);
      assertThat(metrics.annotated()).isZero();
      assertThat(metrics.failed()).isZero();
      assertThat(metrics.skipped(SkipReason.SKIP_REASON_VLM_ERROR)).isEqualTo(3);
      assertThat(metrics.skipped(SkipReason.SKIP_REASON_NO_IMAGE)).isZero();
      assertThat(metrics.line())
          .contains("annotated=0")
          .contains("failed=0")
          .contains("vlm_error=3");
    }
  }

  @Test
  void countsAnnotationsAndSelectionSkipsAcrossDocuments() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      Document document = Document.newBuilder()
          .setName("mixed")
          .addPictures(InProcessEnrich.picture("#/pictures/0"))
          .addPictures(PictureItem.newBuilder().setSelfRef("#/pictures/1"))
          .build();
      for (int i = 0; i < 2; i++) {
        enrich.run(EnrichOptions.newBuilder()
            .setDoPictureDescription(true)
            .setDocument(document)
            .build());
      }

      EnrichMetrics metrics = enrich.engine.metrics();
      assertThat(metrics.documentsCompleted()).isEqualTo(2);
      assertThat(metrics.annotated()).isEqualTo(2);
      assertThat(metrics.skipped(SkipReason.SKIP_REASON_NO_IMAGE)).isEqualTo(2);
      assertThat(metrics.line())
          .contains("completed=2")
          .contains("annotated=2")
          .contains("no_image=2");
    }
  }

  @Test
  void aDistinctVlmFailureIsLoggedOnce() {
    EnrichMetrics metrics = new EnrichMetrics();
    assertThat(metrics.firstSighting("VLM endpoint returned HTTP 503")).isTrue();
    assertThat(metrics.firstSighting("VLM endpoint returned HTTP 503")).isFalse();
    assertThat(metrics.firstSighting("VLM call timed out")).isTrue();
  }

  @Test
  void theSetOfSeenFailuresIsBounded() {
    EnrichMetrics metrics = new EnrichMetrics();
    for (int i = 0; i < EnrichMetrics.MAX_REMEMBERED_FAILURES * 3; i++) {
      assertThat(metrics.firstSighting("failure " + i)).isTrue();
      assertThat(metrics.rememberedFailures())
          .isLessThanOrEqualTo(EnrichMetrics.MAX_REMEMBERED_FAILURES);
    }
  }
}
