package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.enrich.engine.ChartCodeExtractor;
import ai.pipestream.enrich.engine.ChartCsvParser;
import org.junit.jupiter.api.Test;

/**
 * Docling's chart reply post-processing (granite_vision.py
 * _extract_csv_to_dataframe and _extract_python_code), case by case.
 */
class ChartReplyExtractionTest {

  @Test
  void csv_fencedBlockWins_overSurroundingProse() {
    assertThat(ChartCsvParser.extractCsv("Table:\n```csv\na,b\n1,2\n```\ntrailing words"))
        .isEqualTo("a,b\n1,2");
  }

  @Test
  void csv_bareReplyIsStripped() {
    assertThat(ChartCsvParser.extractCsv("\n  a,b\n1,2  \n")).isEqualTo("a,b\n1,2");
  }

  @Test
  void csv_unclosedOrUnlabelledFencesAreRemoved() {
    assertThat(ChartCsvParser.extractCsv("```\na,b\n1,2\n```")).isEqualTo("a,b\n1,2");
    assertThat(ChartCsvParser.extractCsv("```csv a,b\n1,2")).isEqualTo("a,b\n1,2");
    assertThat(ChartCsvParser.extractCsv("a,b\n1,2\n````")).isEqualTo("a,b\n1,2");
  }

  @Test
  void code_firstFencedPythonBlock_stripped() {
    assertThat(ChartCodeExtractor.extractPython(
            "x\n```python\n  import a\nb()\n```\n```python\nsecond\n```"))
        .isEqualTo("import a\nb()");
  }

  @Test
  void code_noPythonFence_isNull() {
    assertThat(ChartCodeExtractor.extractPython("import a")).isNull();
    assertThat(ChartCodeExtractor.extractPython("```\nimport a\n```")).isNull();
    assertThat(ChartCodeExtractor.extractPython("```python\nimport a")).isNull();
  }
}
