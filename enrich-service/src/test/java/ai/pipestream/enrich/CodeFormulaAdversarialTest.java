package ai.pipestream.enrich;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.pipestream.document.v1.CodeLanguageLabel;
import ai.pipestream.enrich.engine.CodeFormulaPostProcessor;
import ai.pipestream.enrich.engine.CodeFormulaPostProcessor.CodeResult;
import org.junit.jupiter.api.Test;

/**
 * Adversarial code/formula post-processing: sentinel placement, language
 * tokens in odd positions, unicode. Several tests pin Docling-parity behavior
 * that looks surprising (sentinels stripped from legitimate code) — those are
 * deliberate, not bugs.
 */
class CodeFormulaAdversarialTest {

  @Test
  void sentinelInsideLegitimateCode_isStripped_doclingParity() {
    // Docling's to_remove list applies unconditionally; code that literally
    // prints the sentinel loses it. Surprising but parity — pinned as-is.
    assertEquals("print(\"\")",
        CodeFormulaPostProcessor.clean("print(\"</code>\")"));
  }

  @Test
  void multipleEndOfUtteranceMarkers_truncateAtFirst() {
    assertEquals("a", CodeFormulaPostProcessor.clean(
        "a<end_of_utterance>b<end_of_utterance>c"));
  }

  @Test
  void unterminatedEndOfUtterance_stillTruncates() {
    assertEquals("code", CodeFormulaPostProcessor.clean("code<end_of_utterance"));
  }

  @Test
  void languageTokenNotAtStart_notParsed() {
    CodeResult result = CodeFormulaPostProcessor.processCode("x = 1 <_Python_> y = 2");
    assertEquals(CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN, result.language());
    assertEquals("", result.languageRaw());
    assertEquals("x = 1 <_Python_> y = 2", result.text());
  }

  @Test
  void languageTokenWithNoBody() {
    CodeResult result = CodeFormulaPostProcessor.processCode("<_Python_>");
    assertEquals(CodeLanguageLabel.CODE_LANGUAGE_LABEL_PYTHON, result.language());
    assertEquals("Python", result.languageRaw());
    assertEquals("", result.text());
  }

  @Test
  void languageTokenImmediatelyFollowedByCode() {
    // Docling's regex allows zero whitespace between token and body.
    CodeResult result = CodeFormulaPostProcessor.processCode("<_Python_>print(1)");
    assertEquals(CodeLanguageLabel.CODE_LANGUAGE_LABEL_PYTHON, result.language());
    assertEquals("print(1)", result.text());
  }

  @Test
  void emptyLanguageToken_notParsed() {
    CodeResult result = CodeFormulaPostProcessor.processCode("<__>print(1)");
    assertEquals(CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN, result.language());
    assertEquals("<__>print(1)", result.text());
  }

  @Test
  void languageTokenWithSymbols_cPlusPlus() {
    CodeResult result = CodeFormulaPostProcessor.processCode("<_C++_>int main() {}");
    assertEquals(CodeLanguageLabel.CODE_LANGUAGE_LABEL_C_PLUS_PLUS, result.language());
  }

  @Test
  void unicodeCode_survives() {
    String code = "print('héllo 😀 漢字')";
    CodeResult result = CodeFormulaPostProcessor.processCode("<_Python_>\n" + code);
    assertEquals(code, result.text());
  }

  @Test
  void veryLongOutput_processes() {
    String code = "x = 1\n".repeat(100_000);
    CodeResult result = CodeFormulaPostProcessor.processCode("<_Python_>\n" + code);
    assertEquals(code.stripLeading(), result.text());
  }

  @Test
  void emptyResponse_staysEmpty() {
    assertEquals("", CodeFormulaPostProcessor.clean(""));
    CodeResult result = CodeFormulaPostProcessor.processCode("");
    assertEquals("", result.text());
    assertEquals(CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN, result.language());
  }

  @Test
  void onlySentinels_cleansToEmpty() {
    assertEquals("", CodeFormulaPostProcessor.clean("</code></formula>"
        + "<loc_0><loc_0><loc_500><loc_500><end_of_utterance>"));
  }

  @Test
  void formulaSentinelStrippedButNotLanguageToken() {
    // Formulas never get a language token: a leading <_..._> stays in text.
    assertEquals("<_Latex_> x^2",
        CodeFormulaPostProcessor.processFormula("<_Latex_> x^2</formula>"));
  }

  @Test
  void locSentinelVariant_notStrippedUnlessExact() {
    // Only Docling's exact loc sentinel string is removed.
    assertEquals("<loc_1><loc_2><loc_3><loc_4>",
        CodeFormulaPostProcessor.clean("<loc_1><loc_2><loc_3><loc_4>"));
  }
}
