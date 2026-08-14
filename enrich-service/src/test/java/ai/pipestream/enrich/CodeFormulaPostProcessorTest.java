package ai.pipestream.enrich;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.pipestream.document.v1.CodeLanguageLabel;
import ai.pipestream.enrich.engine.CodeFormulaPostProcessor;
import ai.pipestream.enrich.engine.CodeFormulaPostProcessor.CodeResult;
import org.junit.jupiter.api.Test;

/** Code/formula VLM output post-processing, mirroring Docling's CodeFormulaVlmModel. */
class CodeFormulaPostProcessorTest {

  @Test
  void truncatesAtEndOfUtterance() {
    assertEquals("x^2", CodeFormulaPostProcessor.clean("x^2<end_of_utterance>garbage"));
    // The tokenizer may decode the marker without its closing angle bracket.
    assertEquals("x^2", CodeFormulaPostProcessor.clean("x^2<end_of_utterance"));
  }

  @Test
  void stripsClosingTagsAndLocSentinel() {
    assertEquals("int x = 1;",
        CodeFormulaPostProcessor.clean("int x = 1;</code>"));
    assertEquals("x^2",
        CodeFormulaPostProcessor.clean("<loc_0><loc_0><loc_500><loc_500>x^2</formula>"));
  }

  @Test
  void lstripsAfterStripping() {
    assertEquals("x^2", CodeFormulaPostProcessor.clean("   x^2"));
    assertEquals("x^2",
        CodeFormulaPostProcessor.clean("  <loc_0><loc_0><loc_500><loc_500>x^2</formula>"));
  }

  @Test
  void languageTokenParsed() {
    CodeResult result = CodeFormulaPostProcessor.processCode("<_Python_>print('hi')");
    assertEquals("print('hi')", result.text());
    assertEquals(CodeLanguageLabel.CODE_LANGUAGE_LABEL_PYTHON, result.language());
    assertEquals("Python", result.languageRaw());
  }

  @Test
  void languageTokenWithWhitespaceAndNewlines() {
    // The regex's \s* greedily consumes all whitespace after the token.
    CodeResult result =
        CodeFormulaPostProcessor.processCode("<_Java_>\n  int x = 1;\n");
    assertEquals("int x = 1;\n", result.text());
    assertEquals(CodeLanguageLabel.CODE_LANGUAGE_LABEL_JAVA, result.language());
  }

  @Test
  void unknownLanguageFallsBackToUnknown() {
    CodeResult result = CodeFormulaPostProcessor.processCode("<_Brainfuck_>+++[>+++<-]");
    assertEquals("+++[>+++<-]", result.text());
    assertEquals(CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN, result.language());
    assertEquals("Brainfuck", result.languageRaw(), "raw string is preserved");
  }

  @Test
  void exactCaseMatchLikeDocling() {
    // Docling matches CodeLanguageLabel(value) exactly: lowercase "python"
    // is not the enum value "Python" and falls back to UNKNOWN.
    CodeResult result = CodeFormulaPostProcessor.processCode("<_python_>pass");
    assertEquals(CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN, result.language());
    assertEquals("python", result.languageRaw());
  }

  @Test
  void noLanguageToken() {
    CodeResult result = CodeFormulaPostProcessor.processCode("print('hi')");
    assertEquals("print('hi')", result.text());
    assertEquals(CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN, result.language());
    assertEquals("", result.languageRaw());
  }

  @Test
  void fullPipelineOrder() {
    // Docling order: clean (truncate, strip, lstrip) first, then the
    // language token is read off the cleaned text.
    CodeResult result = CodeFormulaPostProcessor.processCode(
        "  <_C++_>int main() {}</code><end_of_utterance>tail");
    assertEquals("int main() {}", result.text());
    assertEquals(CodeLanguageLabel.CODE_LANGUAGE_LABEL_C_PLUS_PLUS, result.language());
    assertEquals("C++", result.languageRaw());
  }

  @Test
  void formulaGetsSameCleaningNoLanguage() {
    assertEquals("\\frac{a}{b}",
        CodeFormulaPostProcessor.processFormula(
            "<loc_0><loc_0><loc_500><loc_500>\\frac{a}{b}</formula><end_of_utterance>"));
  }
}
