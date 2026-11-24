package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.document.v1.BaseTextItem;
import ai.pipestream.document.v1.CodeItem;
import ai.pipestream.document.v1.DocItemLabel;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.PictureItem;
import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.v1.ChartExtractionOptions;
import ai.pipestream.enrich.v1.ChartPreset;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.ItemAnnotation;
import ai.pipestream.enrich.v1.PictureDescriptionPreset;
import org.junit.jupiter.api.Test;

/**
 * ItemAnnotation.model is the model that answered, not the name that was
 * asked for. llama.cpp and OVMS serve whatever is loaded whatever the request
 * calls it, so an annotation written by a Qwen model must not be stamped
 * "smolvlm" (and that name must not become the document's provenance). The
 * name that was sent has its own field.
 */
class ServedModelTest {

  private static final String SERVED = "Qwen2.5-VL-7B-Instruct-Q4_K_M.gguf";

  private static EnrichOptions.Builder describeWithSmolvlm() {
    return EnrichOptions.newBuilder()
        .setDoPictureDescription(true)
        .setPictureDescriptionPreset(PictureDescriptionPreset.PICTURE_DESCRIPTION_PRESET_SMOLVLM)
        .setDocument(InProcessEnrich.pictures(1));
  }

  @Test
  void annotationNamesTheModelThatAnswered_andKeepsTheNameThatWasAsked() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.servedModel = body -> SERVED;
      InProcessEnrich.Collected result = enrich.run(describeWithSmolvlm().build());

      assertThat(result.error()).isNull();
      // The request asked for the preset's alias.
      assertThat(vlm.recorded().get(0).model()).isEqualTo("smolvlm");
      ItemAnnotation annotation = result.annotations().get(0);
      assertThat(annotation.getModel()).isEqualTo(SERVED);
      assertThat(annotation.getRequestedModel()).isEqualTo("smolvlm");
      assertThat(annotation.getGeneration().getModel()).isEqualTo(SERVED);
    }
  }

  @Test
  void endpointThatNamesNoModel_leavesModelEmpty_neverTheRequestedName() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      InProcessEnrich.Collected result =
          enrich.run(describeWithSmolvlm().setReturnDocument(true).build());

      ItemAnnotation annotation = result.annotations().get(0);
      assertThat(annotation.getModel()).isEmpty();
      assertThat(annotation.getRequestedModel()).isEqualTo("smolvlm");
      assertThat(annotation.getGeneration().getModel()).isEmpty();
      // The document does not claim a model nobody reported either.
      PictureItem picture = result.complete().getDocument().getPictures(0);
      assertThat(picture.getAnnotations(0).getDescription().getProvenance()).isEmpty();
    }
  }

  @Test
  void requestThatNamesNoModel_stillReportsTheOneThatAnswered() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.servedModel = body -> SERVED;
      vlm.responder = body -> "x^2";
      Document document = Document.newBuilder()
          .setName("code")
          .addTexts(BaseTextItem.newBuilder()
              .setCode(CodeItem.newBuilder()
                  .setSelfRef("#/texts/0")
                  .setLabel(DocItemLabel.DOC_ITEM_LABEL_CODE)
                  .setText("print( 1 )")))
          .build();
      InProcessEnrich.Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoCodeEnrichment(true)
          .setDocument(document)
          .build());

      ItemAnnotation annotation = result.annotations().get(0);
      assertThat(annotation.getModel()).isEqualTo(SERVED);
      assertThat(annotation.getRequestedModel())
          .isEqualTo(vlm.recorded().get(0).model());
    }
  }

  @Test
  void returnedDocument_attributesTheDescriptionToTheModelThatAnswered() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.servedModel = body -> SERVED;
      InProcessEnrich.Collected result =
          enrich.run(describeWithSmolvlm().setReturnDocument(true).build());

      PictureItem picture = result.complete().getDocument().getPictures(0);
      assertThat(picture.getAnnotations(0).getDescription().getProvenance()).isEqualTo(SERVED);
      assertThat(picture.getSource(0).getGeneration().getModel()).isEqualTo(SERVED);
    }
  }

  @Test
  void returnedDocument_attributesAChartSummaryToTheModelThatAnswered() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.servedModel = body -> SERVED;
      vlm.responder = body -> "Sales rose from 10 to 12 between 2023 and 2024.";
      PictureItem chart = InProcessEnrich.picture("#/pictures/0").toBuilder()
          .setLabel(DocItemLabel.DOC_ITEM_LABEL_CHART)
          .build();
      EnrichOptions.Builder options = EnrichOptions.newBuilder()
          .setDoChartExtraction(true)
          .setChartPreset(ChartPreset.CHART_PRESET_GRANITE_VISION_CHART2CSV)
          .setChartExtraction(
              ChartExtractionOptions.newBuilder().setCsv(false).setSummary(true))
          .setReturnDocument(true)
          .setDocument(Document.newBuilder().setName("chart").addPictures(chart));
      InProcessEnrich.Collected result = enrich.run(options.build());

      assertThat(result.skips()).isEmpty();
      ItemAnnotation annotation = result.annotations().get(0);
      assertThat(annotation.getModel()).isEqualTo(SERVED);
      assertThat(annotation.getRequestedModel()).isEqualTo(vlm.recorded().get(0).model());
      assertThat(annotation.getRequestedModel()).isNotEmpty().isNotEqualTo(SERVED);
      PictureItem picture = result.complete().getDocument().getPictures(0);
      assertThat(picture.getMeta().getDescription().getCreatedBy()).isEqualTo(SERVED);
    }
  }

  @Test
  void modelFieldThatIsNotAName_isAnEndpointThatDidNotSay() throws Exception {
    for (String model : new String[] {
        "null", "42", "{\"id\":\"x\"}", "\"   \"", "\"" + "m".repeat(513) + "\""}) {
      try (FakeVlmServer vlm = new FakeVlmServer();
          InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
        vlm.rawOkBody = "{\"model\":" + model
            + ",\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"a caption\"}}]}";
        InProcessEnrich.Collected result = enrich.run(describeWithSmolvlm().build());

        assertThat(result.skips()).as(model).isEmpty();
        ItemAnnotation annotation = result.annotations().get(0);
        assertThat(annotation.getDescription().getText()).isEqualTo("a caption");
        assertThat(annotation.getModel()).as(model).isEmpty();
        assertThat(annotation.getRequestedModel()).isEqualTo("smolvlm");
      }
    }
  }
}
