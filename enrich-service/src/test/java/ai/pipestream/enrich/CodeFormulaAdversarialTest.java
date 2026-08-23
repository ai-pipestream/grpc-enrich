package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.document.v1.CodeLanguageLabel;
import ai.pipestream.enrich.engine.CodeFormulaPostProcessor;
import ai.pipestream.enrich.engine.CodeFormulaPostProcessor.CodeResult;
import org.junit.jupiter.api.Test;

/**
 * Adversarial code/formula post-processing: sentinel placement, language
 * tokens in odd positions, unicode. Several tests pin behavior that looks
 * surprising (sentinels stripped from legitimate code) — those are
 * deliberate, not bugs.
 */
class CodeFormulaAdversarialTest {

  @Test
  void sentinelInsideLegitimateCode_isStripped() {
    // The sentinel list applies unconditionally; code that literally
    // prints the sentinel loses it. Surprising but deliberate — pinned as-is.
    assertThat(CodeFormulaPostProcessor.clean("print(\"</code>\")")).isEqualTo("print(\"\")");
  }

  @Test
  void multipleEndOfUtteranceMarkers_truncateAtFirst() {
    assertThat(CodeFormulaPostProcessor.clean(
        "a<end_of_utterance>b<end_of_utterance>c")).isEqualTo("a");
  }

  @Test
  void unterminatedEndOfUtterance_stillTruncates() {
    assertThat(CodeFormulaPostProcessor.clean("code<end_of_utterance")).isEqualTo("code");
  }

  @Test
  void languageTokenNotAtStart_notParsed() {
    CodeResult result = CodeFormulaPostProcessor.processCode("x = 1 <_Python_> y = 2");
    assertThat(result.language()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN);
    assertThat(result.languageRaw()).isEqualTo("");
    assertThat(result.text()).isEqualTo("x = 1 <_Python_> y = 2");
  }

  @Test
  void languageTokenWithNoBody() {
    CodeResult result = CodeFormulaPostProcessor.processCode("<_Python_>");
    assertThat(result.language()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_PYTHON);
    assertThat(result.languageRaw()).isEqualTo("Python");
    assertThat(result.text()).isEqualTo("");
  }

  @Test
  void languageTokenImmediatelyFollowedByCode() {
    // The token regex allows zero whitespace between token and body.
    CodeResult result = CodeFormulaPostProcessor.processCode("<_Python_>print(1)");
    assertThat(result.language()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_PYTHON);
    assertThat(result.text()).isEqualTo("print(1)");
  }

  @Test
  void emptyLanguageToken_notParsed() {
    CodeResult result = CodeFormulaPostProcessor.processCode("<__>print(1)");
    assertThat(result.language()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN);
    assertThat(result.text()).isEqualTo("<__>print(1)");
  }

  @Test
  void languageTokenWithSymbols_cPlusPlus() {
    CodeResult result = CodeFormulaPostProcessor.processCode("<_C++_>int main() {}");
    assertThat(result.language()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_C_PLUS_PLUS);
  }

  @Test
  void unicodeCode_survives() {
    String code = "print('héllo 😀 漢字')";
    CodeResult result = CodeFormulaPostProcessor.processCode("<_Python_>\n" + code);
    assertThat(result.text()).isEqualTo(code);
  }

  @Test
  void veryLongOutput_processes() {
    String code = "x = 1\n".repeat(100_000);
    CodeResult result = CodeFormulaPostProcessor.processCode("<_Python_>\n" + code);
    assertThat(result.text()).isEqualTo(code.stripLeading());
  }

  @Test
  void emptyResponse_staysEmpty() {
    assertThat(CodeFormulaPostProcessor.clean("")).isEqualTo("");
    CodeResult result = CodeFormulaPostProcessor.processCode("");
    assertThat(result.text()).isEqualTo("");
    assertThat(result.language()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN);
  }

  @Test
  void onlySentinels_cleansToEmpty() {
    assertThat(CodeFormulaPostProcessor.clean("</code></formula>"
        + "<loc_0><loc_0><loc_500><loc_500><end_of_utterance>")).isEqualTo("");
  }

  @Test
  void formulaSentinelStrippedButNotLanguageToken() {
    // Formulas never get a language token: a leading <_..._> stays in text.
    assertThat(CodeFormulaPostProcessor.processFormula("<_Latex_> x^2</formula>"))
        .isEqualTo("<_Latex_> x^2");
  }

  @Test
  void locSentinelVariant_notStrippedUnlessExact() {
    // Only the exact loc sentinel string is removed.
    assertThat(CodeFormulaPostProcessor.clean("<loc_1><loc_2><loc_3><loc_4>"))
        .isEqualTo("<loc_1><loc_2><loc_3><loc_4>");
  }
}
