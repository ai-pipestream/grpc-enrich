package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.document.v1.BaseTextItem;
import ai.pipestream.document.v1.CodeItem;
import ai.pipestream.document.v1.DocItemLabel;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.FormulaItem;
import ai.pipestream.document.v1.TextItemBase;
import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.v1.EnrichComplete;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.ItemSkipped;
import ai.pipestream.enrich.v1.SkipReason;
import org.junit.jupiter.api.Test;

/**
 * A model that answers with no text has not enriched anything. An endpoint
 * that replies {@code "content": ""} (a reasoning model that spent its
 * budget thinking, an immediate end of sequence, a reply that is only the
 * sentinels post-processing strips) must be a skip, never an annotation with
 * empty text counted as succeeded, and with return_document it must never
 * blank the text the item already had.
 */
class EmptyReplyTest {

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

  private static Document codeAndFormula() {
    return Document.newBuilder()
        .setName("code-and-formula")
        .addTexts(code("#/texts/0", "print( 'hi' )"))
        .addTexts(formula("#/texts/1", "x2+y2=z2"))
        .build();
  }

  private static void assertNoTextSkip(ItemSkipped skip) {
    assertThat(skip.getReason()).isEqualTo(SkipReason.SKIP_REASON_VLM_ERROR);
    assertThat(skip.getDetail()).isEqualTo("model returned no text");
  }

  @Test
  void emptyDescription_isSkipped_notAnEmptyAnnotation() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.responder = body -> "";
      InProcessEnrich.Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setReturnDocument(true)
          .setDocument(InProcessEnrich.pictures(1))
          .build());

      assertThat(result.error()).isNull();
      assertThat(result.annotations()).isEmpty();
      assertThat(result.skips()).singleElement().satisfies(skip -> {
        assertThat(skip.getSelfRef()).isEqualTo("#/pictures/0");
        assertNoTextSkip(skip);
      });
      EnrichComplete complete = result.complete();
      assertThat(complete.getSucceeded()).isZero();
      assertThat(complete.getSkipped()).isEqualTo(1);
      assertThat(complete.getFailed()).isZero();
      assertThat(complete.getDocument().getPictures(0).getAnnotationsCount()).isZero();
    }
  }

  @Test
  void whitespaceOnlyDescription_isSkipped() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.responder = body -> " \n\t ";
      InProcessEnrich.Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDocument(InProcessEnrich.pictures(1))
          .build());

      assertThat(result.annotations()).isEmpty();
      assertThat(result.skips()).singleElement().satisfies(EmptyReplyTest::assertNoTextSkip);
    }
  }

  @Test
  void emptyCodeAndFormula_areSkipped_andNeverBlankTheItemText() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.responder = body -> "";
      InProcessEnrich.Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoCodeEnrichment(true)
          .setDoFormulaEnrichment(true)
          .setReturnDocument(true)
          .setDocument(codeAndFormula())
          .build());

      assertThat(result.error()).isNull();
      assertThat(result.annotations()).isEmpty();
      assertThat(result.skips()).hasSize(2).allSatisfy(EmptyReplyTest::assertNoTextSkip);
      assertThat(result.skips()).extracting(ItemSkipped::getSelfRef)
          .containsExactlyInAnyOrder("#/texts/0", "#/texts/1");
      EnrichComplete complete = result.complete();
      assertThat(complete.getSucceeded()).isZero();
      assertThat(complete.getSkipped()).isEqualTo(2);
      // The text layer the items came with is still there.
      assertThat(complete.getDocument().getTexts(0).getCode().getText())
          .isEqualTo("print( 'hi' )");
      assertThat(complete.getDocument().getTexts(1).getFormula().getBase().getText())
          .isEqualTo("x2+y2=z2");
    }
  }

  @Test
  void replyThatIsOnlyStrippedSentinels_isSkipped() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      // Post-processing removes these, leaving nothing.
      vlm.responder = body -> "<end_of_utterance>";
      InProcessEnrich.Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoCodeEnrichment(true)
          .setDoFormulaEnrichment(true)
          .setReturnDocument(true)
          .setDocument(codeAndFormula())
          .build());

      assertThat(result.annotations()).isEmpty();
      assertThat(result.skips()).hasSize(2).allSatisfy(EmptyReplyTest::assertNoTextSkip);
      assertThat(result.complete().getDocument().getTexts(1).getFormula().getBase().getText())
          .isEqualTo("x2+y2=z2");
    }
  }

  @Test
  void codeReplyThatIsOnlyALanguageTag_isSkipped() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.responder = body -> "<_Python_>";
      InProcessEnrich.Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoCodeEnrichment(true)
          .setReturnDocument(true)
          .setDocument(codeAndFormula())
          .build());

      assertThat(result.annotations()).isEmpty();
      assertThat(result.skips()).singleElement().satisfies(EmptyReplyTest::assertNoTextSkip);
      assertThat(result.complete().getDocument().getTexts(0).getCode().getText())
          .isEqualTo("print( 'hi' )");
    }
  }

  @Test
  void oneEmptyReply_doesNotTakeTheOthersWithIt() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      // The code call gets nothing back; the formula call gets a formula.
      vlm.responder = body -> body.contains("x2+y2=z2") ? "x^2+y^2=z^2" : "";
      InProcessEnrich.Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoCodeEnrichment(true)
          .setDoFormulaEnrichment(true)
          .setReturnDocument(true)
          .setDocument(codeAndFormula())
          .build());

      assertThat(result.annotations()).singleElement().satisfies(annotation -> {
        assertThat(annotation.getSelfRef()).isEqualTo("#/texts/1");
        assertThat(annotation.getFormula().getText()).isEqualTo("x^2+y^2=z^2");
      });
      assertThat(result.skips()).singleElement().satisfies(skip -> {
        assertThat(skip.getSelfRef()).isEqualTo("#/texts/0");
        assertNoTextSkip(skip);
      });
      EnrichComplete complete = result.complete();
      assertThat(complete.getSucceeded()).isEqualTo(1);
      assertThat(complete.getSkipped()).isEqualTo(1);
      assertThat(complete.getDocument().getTexts(0).getCode().getText())
          .isEqualTo("print( 'hi' )");
      assertThat(complete.getDocument().getTexts(1).getFormula().getBase().getText())
          .isEqualTo("x^2+y^2=z^2");
    }
  }
}
