package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.document.v1.CodeLanguageLabel;
import ai.pipestream.enrich.engine.CodeFormulaPostProcessor;
import ai.pipestream.enrich.engine.CodeFormulaPostProcessor.CodeResult;
import ai.pipestream.enrich.v1.ChartPreset;
import ai.pipestream.enrich.v1.CodeFormulaPreset;
import ai.pipestream.enrich.v1.EnrichComplete;
import ai.pipestream.enrich.v1.EnrichDocumentResponse;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.EnrichStarted;
import ai.pipestream.enrich.v1.ItemAnnotation;
import ai.pipestream.enrich.v1.ItemSkipped;
import ai.pipestream.enrich.v1.PictureDescription;
import ai.pipestream.enrich.v1.PictureDescriptionPreset;
import ai.pipestream.enrich.v1.SkipReason;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Test;

/**
 * Pins the wire-visible strings both surfaces expose: enum wire names, the
 * proto3 JSON field spellings JsonFormat prints on the HTTP surface, and the
 * language-token spellings the code post-processor maps. A protobuf or
 * grpc upgrade that drifts any of these strings must fail here, not in a
 * client.
 */
class WireShapeTest {

  private static final JsonFormat.Printer PRINTER =
      JsonFormat.printer().omittingInsignificantWhitespace();

  // -------------------------------------------------------------------------
  // Enum wire names
  // -------------------------------------------------------------------------

  @Test
  void skipReasonWireNames() {
    assertThat(SkipReason.SKIP_REASON_UNSPECIFIED.getValueDescriptor().getName())
        .isEqualTo("SKIP_REASON_UNSPECIFIED");
    assertThat(SkipReason.SKIP_REASON_NO_IMAGE.getValueDescriptor().getName())
        .isEqualTo("SKIP_REASON_NO_IMAGE");
    assertThat(SkipReason.SKIP_REASON_UNFETCHABLE_URI.getValueDescriptor().getName())
        .isEqualTo("SKIP_REASON_UNFETCHABLE_URI");
    assertThat(SkipReason.SKIP_REASON_BELOW_AREA_THRESHOLD.getValueDescriptor().getName())
        .isEqualTo("SKIP_REASON_BELOW_AREA_THRESHOLD");
    assertThat(SkipReason.SKIP_REASON_VLM_ERROR.getValueDescriptor().getName())
        .isEqualTo("SKIP_REASON_VLM_ERROR");
  }

  @Test
  void presetWireNames() {
    assertThat(PictureDescriptionPreset.PICTURE_DESCRIPTION_PRESET_SMOLVLM
        .getValueDescriptor().getName())
        .isEqualTo("PICTURE_DESCRIPTION_PRESET_SMOLVLM");
    assertThat(PictureDescriptionPreset.PICTURE_DESCRIPTION_PRESET_GRANITE_VISION
        .getValueDescriptor().getName())
        .isEqualTo("PICTURE_DESCRIPTION_PRESET_GRANITE_VISION");
    assertThat(ChartPreset.CHART_PRESET_GRANITE_VISION_CHART2CSV.getValueDescriptor().getName())
        .isEqualTo("CHART_PRESET_GRANITE_VISION_CHART2CSV");
    assertThat(CodeFormulaPreset.CODE_FORMULA_PRESET_CODE_FORMULA_V2
        .getValueDescriptor().getName())
        .isEqualTo("CODE_FORMULA_PRESET_CODE_FORMULA_V2");
  }

  // -------------------------------------------------------------------------
  // Proto3 JSON as served on the HTTP surface
  // -------------------------------------------------------------------------

  @Test
  void startedEventJson() throws Exception {
    EnrichDocumentResponse event = EnrichDocumentResponse.newBuilder()
        .setStarted(EnrichStarted.newBuilder().setPictureDescriptions(2).setChartExtractions(1))
        .build();
    assertThat(PRINTER.print(event))
        .isEqualTo("{\"started\":{\"pictureDescriptions\":2,\"chartExtractions\":1}}");
  }

  @Test
  void annotationEventJson() throws Exception {
    EnrichDocumentResponse event = EnrichDocumentResponse.newBuilder()
        .setAnnotation(ItemAnnotation.newBuilder()
            .setSelfRef("#/pictures/0")
            .setModel("smolvlm")
            .setDescription(PictureDescription.newBuilder().setText("a dog")))
        .build();
    assertThat(PRINTER.print(event))
        .isEqualTo("{\"annotation\":{\"selfRef\":\"#/pictures/0\",\"model\":\"smolvlm\","
            + "\"description\":{\"text\":\"a dog\"}}}");
  }

  @Test
  void skippedEventJson() throws Exception {
    EnrichDocumentResponse event = EnrichDocumentResponse.newBuilder()
        .setSkipped(ItemSkipped.newBuilder()
            .setSelfRef("#/pictures/3")
            .setReason(SkipReason.SKIP_REASON_VLM_ERROR)
            .setDetail("boom"))
        .build();
    assertThat(PRINTER.print(event))
        .isEqualTo("{\"skipped\":{\"selfRef\":\"#/pictures/3\","
            + "\"reason\":\"SKIP_REASON_VLM_ERROR\",\"detail\":\"boom\"}}");
  }

  @Test
  void completeEventJson() throws Exception {
    EnrichDocumentResponse event = EnrichDocumentResponse.newBuilder()
        .setComplete(EnrichComplete.newBuilder().setSucceeded(2).setSkipped(1))
        .build();
    assertThat(PRINTER.print(event))
        .isEqualTo("{\"complete\":{\"succeeded\":2,\"skipped\":1}}");
  }

  // -------------------------------------------------------------------------
  // Proto3 JSON accepted on the HTTP surface
  // -------------------------------------------------------------------------

  @Test
  void optionsParseAcceptsCamelCaseKeys() throws Exception {
    EnrichOptions.Builder options = EnrichOptions.newBuilder();
    JsonFormat.parser().merge(
        "{\"doPictureDescription\":true,\"pictureDescriptionAreaThreshold\":-1}", options);
    assertThat(options.getDoPictureDescription()).isTrue();
    assertThat(options.getPictureDescriptionAreaThreshold()).isEqualTo(-1.0);
  }

  @Test
  void optionsParseAcceptsSnakeCaseKeys() throws Exception {
    EnrichOptions.Builder options = EnrichOptions.newBuilder();
    JsonFormat.parser().merge(
        "{\"do_picture_description\":true,\"picture_description_area_threshold\":-1}", options);
    assertThat(options.getDoPictureDescription()).isTrue();
    assertThat(options.getPictureDescriptionAreaThreshold()).isEqualTo(-1.0);
  }

  @Test
  void optionsParseAcceptsEnumNames() throws Exception {
    EnrichOptions.Builder options = EnrichOptions.newBuilder();
    JsonFormat.parser().merge(
        "{\"pictureDescriptionPreset\":\"PICTURE_DESCRIPTION_PRESET_GRANITE_VISION\","
            + "\"chartPreset\":\"CHART_PRESET_GRANITE_VISION_CHART2CSV\"}", options);
    assertThat(options.getPictureDescriptionPreset())
        .isEqualTo(PictureDescriptionPreset.PICTURE_DESCRIPTION_PRESET_GRANITE_VISION);
    assertThat(options.getChartPreset())
        .isEqualTo(ChartPreset.CHART_PRESET_GRANITE_VISION_CHART2CSV);
  }

  // -------------------------------------------------------------------------
  // Language-token spellings on the code annotation path
  // -------------------------------------------------------------------------

  @Test
  void languageTokenSpellings_symbolLanguages() {
    CodeResult cpp = CodeFormulaPostProcessor.processCode("<_C++_>int x;");
    assertThat(cpp.language()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_C_PLUS_PLUS);
    assertThat(cpp.languageRaw()).isEqualTo("C++");

    CodeResult csharp = CodeFormulaPostProcessor.processCode("<_C#_>int x;");
    assertThat(csharp.language()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_C_SHARP);
    assertThat(csharp.languageRaw()).isEqualTo("C#");
  }

  @Test
  void languageTokenSpellings_caseIsPartOfTheContract() {
    // "bc" and "dc" are lowercase in the schema; "Bash" is capitalized.
    assertThat(CodeFormulaPostProcessor.processCode("<_bc_>1+1").language())
        .isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_BC);
    assertThat(CodeFormulaPostProcessor.processCode("<_dc_>1 1 +").language())
        .isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_DC);
    assertThat(CodeFormulaPostProcessor.processCode("<_Bash_>ls").language())
        .isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_BASH);
    assertThat(CodeFormulaPostProcessor.processCode("<_BASH_>ls").language())
        .isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN);
  }
}
