package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.document.v1.BaseTextItem;
import ai.pipestream.document.v1.CodeItem;
import ai.pipestream.document.v1.DocItemLabel;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.FormulaItem;
import ai.pipestream.document.v1.ImageRef;
import ai.pipestream.document.v1.TextItemBase;
import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.v1.AnnotationInput;
import ai.pipestream.enrich.v1.DocumentChunk;
import ai.pipestream.enrich.v1.EnrichDocumentRequest;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.ItemAnnotation;
import ai.pipestream.enrich.v1.ItemImage;
import ai.pipestream.enrich.v1.SkipReason;
import com.google.protobuf.ByteString;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * What the model was given is on the wire. A formula or code item with no
 * crop is still sent as a text-only prompt (the documented fallback), but
 * its annotation says TEXT_ONLY, so a transcription made from the text layer
 * alone cannot pass for one read off the image. A crop that names no item is
 * counted, and an image the server cannot fetch is a skip, not a text-only
 * call.
 */
class AnnotationInputTest {

  private static final ByteString PNG =
      ByteString.copyFrom(new byte[] {(byte) 0x89, 'P', 'N', 'G', 1, 2, 3, 4});

  private static BaseTextItem formula(String selfRef, String text) {
    return BaseTextItem.newBuilder()
        .setFormula(FormulaItem.newBuilder()
            .setBase(TextItemBase.newBuilder()
                .setSelfRef(selfRef)
                .setLabel(DocItemLabel.DOC_ITEM_LABEL_FORMULA)
                .setText(text)))
        .build();
  }

  private static BaseTextItem code(String selfRef, String text) {
    return BaseTextItem.newBuilder()
        .setCode(CodeItem.newBuilder()
            .setSelfRef(selfRef)
            .setLabel(DocItemLabel.DOC_ITEM_LABEL_CODE)
            .setText(text))
        .build();
  }

  private static EnrichDocumentRequest crop(String selfRef) {
    return EnrichDocumentRequest.newBuilder()
        .setImage(ItemImage.newBuilder().setSelfRef(selfRef).setMimetype("image/png").setData(PNG))
        .build();
  }

  private static EnrichDocumentRequest completeChunk(Document document) {
    return EnrichDocumentRequest.newBuilder()
        .setChunk(DocumentChunk.newBuilder().setData(document.toByteString()).setComplete(true))
        .build();
  }

  private static ItemAnnotation annotationFor(InProcessEnrich.Collected result, String selfRef) {
    return result.annotations().stream()
        .filter(annotation -> annotation.getSelfRef().equals(selfRef))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no annotation for " + selfRef + " in "
            + result.events()));
  }

  @Test
  void formulaAndCodeWithoutACrop_areMarkedTextOnly() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.responder = body -> "x^2";
      Document document = Document.newBuilder()
          .setName("no-crops")
          .addTexts(formula("#/texts/0", "x2"))
          .addTexts(code("#/texts/1", "print( 1 )"))
          .build();
      InProcessEnrich.Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoFormulaEnrichment(true)
          .setDoCodeEnrichment(true)
          .setDocument(document)
          .build());

      assertThat(result.error()).isNull();
      assertThat(vlm.recorded()).hasSize(2).allSatisfy(
          request -> assertThat(request.hasImage()).isFalse());
      assertThat(annotationFor(result, "#/texts/0").getInput())
          .as("a formula transcribed from its text layer alone says so")
          .isEqualTo(AnnotationInput.ANNOTATION_INPUT_TEXT_ONLY);
      assertThat(annotationFor(result, "#/texts/1").getInput())
          .isEqualTo(AnnotationInput.ANNOTATION_INPUT_TEXT_ONLY);
      assertThat(result.events().get(0).getStarted().getUnmatchedImages()).isZero();
    }
  }

  @Test
  void itemsSentWithAnImage_areMarkedImage() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.responder = body -> "x^2";
      Document document = Document.newBuilder()
          .setName("crops")
          .addPictures(InProcessEnrich.picture("#/pictures/0"))
          .addTexts(formula("#/texts/0", "x2"))
          .addTexts(code("#/texts/1", "print( 1 )"))
          .build();
      InProcessEnrich.Collected result = enrich.run(List.of(
          InProcessEnrich.options(EnrichOptions.newBuilder()
              .setDoPictureDescription(true)
              .setDoFormulaEnrichment(true)
              .setDoCodeEnrichment(true)
              .build()),
          crop("#/texts/0"),
          crop("#/texts/1"),
          completeChunk(document)));

      assertThat(result.error()).isNull();
      assertThat(vlm.recorded()).hasSize(3).allSatisfy(
          request -> assertThat(request.hasImage()).isTrue());
      assertThat(result.annotations()).hasSize(3).allSatisfy(annotation ->
          assertThat(annotation.getInput()).isEqualTo(AnnotationInput.ANNOTATION_INPUT_IMAGE));
      assertThat(result.events().get(0).getStarted().getUnmatchedImages()).isZero();
    }
  }

  @Test
  void cropUnderASelfRefNoItemHas_isCounted() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.responder = body -> "x^2";
      Document document = Document.newBuilder()
          .setName("mismatched")
          .addTexts(formula("#/texts/0", "x2"))
          .addTexts(BaseTextItem.newBuilder()
              .setText(ai.pipestream.document.v1.TextItem.newBuilder()
                  .setBase(TextItemBase.newBuilder()
                      .setSelfRef("#/texts/1")
                      .setLabel(DocItemLabel.DOC_ITEM_LABEL_PARAGRAPH)
                      .setText("a paragraph"))))
          .build();
      InProcessEnrich.Collected result = enrich.run(List.of(
          InProcessEnrich.options(EnrichOptions.newBuilder().setDoFormulaEnrichment(true).build()),
          // The formula's crop, sent under a ref the document does not have,
          // and a crop for a paragraph, which is not an enrichable item.
          crop("#/texts/7"),
          crop("#/texts/1"),
          completeChunk(document)));

      assertThat(result.error()).isNull();
      assertThat(result.events().get(0).getStarted().getUnmatchedImages())
          .as("crops that reached no picture, code or formula item")
          .isEqualTo(2);
      assertThat(annotationFor(result, "#/texts/0").getInput())
          .as("the formula got no image, and its annotation says so")
          .isEqualTo(AnnotationInput.ANNOTATION_INPUT_TEXT_ONLY);
    }
  }

  @Test
  void cropForAnItemWhoseJobIsOff_isNotUnmatched() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      Document document = Document.newBuilder()
          .setName("unused")
          .addPictures(InProcessEnrich.picture(""))
          .addTexts(formula("", "x2"))
          .build();
      InProcessEnrich.Collected result = enrich.run(List.of(
          InProcessEnrich.options(
              EnrichOptions.newBuilder().setDoPictureDescription(true).build()),
          // Refs by position, for items that declare none.
          crop("#/pictures/0"),
          crop("#/texts/0"),
          completeChunk(document)));

      assertThat(result.error()).isNull();
      assertThat(result.events().get(0).getStarted().getUnmatchedImages()).isZero();
    }
  }

  @Test
  void codeWhoseImageIsANonDataUri_isSkippedNotSentAsText() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      Document document = Document.newBuilder()
          .setName("remote-image")
          .addTexts(BaseTextItem.newBuilder()
              .setCode(CodeItem.newBuilder()
                  .setSelfRef("#/texts/0")
                  .setLabel(DocItemLabel.DOC_ITEM_LABEL_CODE)
                  .setText("print( 1 )")
                  .setImage(ImageRef.newBuilder()
                      .setMimetype("image/png")
                      .setUri("https://example.com/code.png"))))
          .build();
      InProcessEnrich.Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoCodeEnrichment(true)
          .setDocument(document)
          .build());

      assertThat(result.error()).isNull();
      assertThat(vlm.calls()).as("the item names an image this server cannot fetch").isZero();
      assertThat(result.annotations()).isEmpty();
      assertThat(result.skips()).singleElement().satisfies(skip -> {
        assertThat(skip.getSelfRef()).isEqualTo("#/texts/0");
        assertThat(skip.getReason()).isEqualTo(SkipReason.SKIP_REASON_UNFETCHABLE_URI);
      });
      assertThat(result.complete().getSkipped()).isEqualTo(1);
    }
  }
}
