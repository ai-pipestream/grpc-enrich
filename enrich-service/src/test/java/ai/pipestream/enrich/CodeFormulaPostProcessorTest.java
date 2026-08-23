package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.document.v1.CodeLanguageLabel;
import ai.pipestream.enrich.engine.CodeFormulaPostProcessor;
import ai.pipestream.enrich.engine.CodeFormulaPostProcessor.CodeResult;
import org.junit.jupiter.api.Test;

/** Code/formula VLM output post-processing. */
class CodeFormulaPostProcessorTest {

  @Test
  void truncatesAtEndOfUtterance() {
    assertThat(CodeFormulaPostProcessor.clean("x^2<end_of_utterance>garbage")).isEqualTo("x^2");
    // The tokenizer may decode the marker without its closing angle bracket.
    assertThat(CodeFormulaPostProcessor.clean("x^2<end_of_utterance")).isEqualTo("x^2");
  }

  @Test
  void stripsClosingTagsAndLocSentinel() {
    assertThat(CodeFormulaPostProcessor.clean("int x = 1;</code>")).isEqualTo("int x = 1;");
    assertThat(CodeFormulaPostProcessor.clean("<loc_0><loc_0><loc_500><loc_500>x^2</formula>"))
        .isEqualTo("x^2");
  }

  @Test
  void lstripsAfterStripping() {
    assertThat(CodeFormulaPostProcessor.clean("   x^2")).isEqualTo("x^2");
    assertThat(CodeFormulaPostProcessor.clean("  <loc_0><loc_0><loc_500><loc_500>x^2</formula>"))
        .isEqualTo("x^2");
  }

  @Test
  void languageTokenParsed() {
    CodeResult result = CodeFormulaPostProcessor.processCode("<_Python_>print('hi')");
    assertThat(result.text()).isEqualTo("print('hi')");
    assertThat(result.language()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_PYTHON);
    assertThat(result.languageRaw()).isEqualTo("Python");
  }

  @Test
  void languageTokenWithWhitespaceAndNewlines() {
    // The regex's \s* greedily consumes all whitespace after the token.
    CodeResult result =
        CodeFormulaPostProcessor.processCode("<_Java_>\n  int x = 1;\n");
    assertThat(result.text()).isEqualTo("int x = 1;\n");
    assertThat(result.language()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_JAVA);
  }

  @Test
  void unknownLanguageFallsBackToUnknown() {
    CodeResult result = CodeFormulaPostProcessor.processCode("<_Brainfuck_>+++[>+++<-]");
    assertThat(result.text()).isEqualTo("+++[>+++<-]");
    assertThat(result.language()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN);
    assertThat(result.languageRaw()).as("raw string is preserved").isEqualTo("Brainfuck");
  }

  @Test
  void exactCaseLanguageMatch() {
    // Language matching is exact-case: lowercase "python"
    // is not the enum value "Python" and falls back to UNKNOWN.
    CodeResult result = CodeFormulaPostProcessor.processCode("<_python_>pass");
    assertThat(result.language()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN);
    assertThat(result.languageRaw()).isEqualTo("python");
  }

  @Test
  void noLanguageToken() {
    CodeResult result = CodeFormulaPostProcessor.processCode("print('hi')");
    assertThat(result.text()).isEqualTo("print('hi')");
    assertThat(result.language()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN);
    assertThat(result.languageRaw()).isEqualTo("");
  }

  @Test
  void fullPipelineOrder() {
    // Pipeline order: clean (truncate, strip, lstrip) first, then the
    // language token is read off the cleaned text.
    CodeResult result = CodeFormulaPostProcessor.processCode(
        "  <_C++_>int main() {}</code><end_of_utterance>tail");
    assertThat(result.text()).isEqualTo("int main() {}");
    assertThat(result.language()).isEqualTo(CodeLanguageLabel.CODE_LANGUAGE_LABEL_C_PLUS_PLUS);
    assertThat(result.languageRaw()).isEqualTo("C++");
  }

  @Test
  void formulaGetsSameCleaningNoLanguage() {
    assertThat(CodeFormulaPostProcessor.processFormula(
            "<loc_0><loc_0><loc_500><loc_500>\\frac{a}{b}</formula><end_of_utterance>"))
        .isEqualTo("\\frac{a}{b}");
  }
}
