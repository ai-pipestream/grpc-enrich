package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.document.v1.TableData;
import ai.pipestream.enrich.engine.ChartCsvParser;
import ai.pipestream.enrich.vlm.VlmClient.VlmException;
import org.junit.jupiter.api.Test;

/**
 * Adversarial chart-CSV cases: quoting, line endings, blank lines, degenerate
 * shapes. Most of these should pass as-is; a failure here is a parser bug.
 */
class ChartCsvParserAdversarialTest {

  @Test
  void crlfLineEndings() throws Exception {
    TableData table = ChartCsvParser.parse("year,sales\r\n2023,10\r\n2024,12\r\n");
    assertThat(table.getNumRows()).isEqualTo(3);
    assertThat(table.getNumCols()).isEqualTo(2);
    assertThat(table.getTableCells(4).getText()).isEqualTo("2024");
  }

  @Test
  void loneCarriageReturnLineEndings() throws Exception {
    TableData table = ChartCsvParser.parse("a,b\r1,2");
    assertThat(table.getNumRows()).isEqualTo(2);
    assertThat(table.getTableCells(2).getText()).isEqualTo("1");
  }

  @Test
  void quotedFieldWithEmbeddedNewlineAndCrlf() throws Exception {
    TableData table = ChartCsvParser.parse("\"a\nb\",c\r\n1,2");
    assertThat(table.getNumRows()).isEqualTo(2);
    assertThat(table.getTableCells(0).getText()).isEqualTo("a\nb");
  }

  @Test
  void quotedFieldWithDoubledQuotesAndCommas() throws Exception {
    TableData table = ChartCsvParser.parse("\"say \"\"hi\"\"\",x\n1,2");
    assertThat(table.getTableCells(0).getText()).isEqualTo("say \"hi\"");
  }

  @Test
  void trailingEmptyLinesDropped() throws Exception {
    TableData table = ChartCsvParser.parse("a,b\n1,2\n\n\n");
    assertThat(table.getNumRows()).isEqualTo(2);
  }

  @Test
  void blankLineInMiddleDropped() throws Exception {
    TableData table = ChartCsvParser.parse("a,b\n\n1,2");
    assertThat(table.getNumRows()).isEqualTo(2);
    assertThat(table.getTableCells(2).getText()).isEqualTo("1");
  }

  @Test
  void singleRowAllNumeric_noHeader() throws Exception {
    TableData table = ChartCsvParser.parse("1,2,3");
    assertThat(table.getNumRows()).isEqualTo(1);
    assertThat(table.getNumCols()).isEqualTo(3);
    for (int i = 0; i < 3; i++) {
      assertThat(table.getTableCells(i).getColumnHeader()).isFalse();
      assertThat(table.getTableCells(i).getRowHeader()).isFalse();
    }
  }

  @Test
  void singleColumn() throws Exception {
    TableData table = ChartCsvParser.parse("name\nalpha\nbeta");
    assertThat(table.getNumRows()).isEqualTo(3);
    assertThat(table.getNumCols()).isEqualTo(1);
    assertThat(table.getTableCells(0).getColumnHeader()).isTrue();
    assertThat(table.getTableCells(2).getText()).isEqualTo("beta");
  }

  @Test
  void cellsWithSpaces_numericDetectionTrimsTextPreserved() throws Exception {
    TableData table = ChartCsvParser.parse("h1,h2\n 1 , x ");
    // " 1 " parses as numeric -> not a row header; " x " is a row header.
    assertThat(table.getTableCells(2).getRowHeader()).isFalse();
    assertThat(table.getTableCells(3).getRowHeader()).isTrue();
    // Cell text is preserved verbatim, spaces and all.
    assertThat(table.getTableCells(2).getText()).isEqualTo(" 1 ");
  }

  @Test
  void firstRowAllEmptyStrings_isHeaderRow() throws Exception {
    // Header iff ALL first-row values are non-numeric; empty
    // strings are non-numeric, so this degenerate row is a header.
    TableData table = ChartCsvParser.parse(",\n1,2");
    assertThat(table.getTableCells(0).getColumnHeader()).isTrue();
    assertThat(table.getTableCells(0).getText()).isEqualTo("");
  }

  @Test
  void emptyCellVsMissingCell() throws Exception {
    TableData table = ChartCsvParser.parse("a,b,c\n1,,3");
    assertThat(table.getTableCells(4).getText()).isEqualTo("");
    // Empty data cell counts as non-numeric (pandas NaN parity) -> row header.
    assertThat(table.getTableCells(4).getRowHeader()).isTrue();
    assertThatThrownBy(() -> ChartCsvParser.parse("a,b,c\n1,2")).isInstanceOf(VlmException.class);
  }

  @Test
  void onlyNewline_rejected() {
    assertThatThrownBy(() -> ChartCsvParser.parse("\n")).isInstanceOf(VlmException.class);
  }

  @Test
  void onlyBlankLines_rejected() {
    assertThatThrownBy(() -> ChartCsvParser.parse("\n\n\n")).isInstanceOf(VlmException.class);
  }

  @Test
  void unterminatedQuote_lenientSingleRow() throws Exception {
    TableData table = ChartCsvParser.parse("\"a,b");
    assertThat(table.getNumRows()).isEqualTo(1);
    assertThat(table.getTableCells(0).getText()).isEqualTo("a,b");
  }

  @Test
  void veryWideCsv() throws Exception {
    StringBuilder csv = new StringBuilder();
    for (int c = 0; c < 2000; c++) {
      csv.append(c == 0 ? "" : ",").append("h").append(c);
    }
    csv.append('\n');
    for (int c = 0; c < 2000; c++) {
      csv.append(c == 0 ? "" : ",").append(c);
    }
    TableData table = ChartCsvParser.parse(csv.toString());
    assertThat(table.getNumCols()).isEqualTo(2000);
    assertThat(table.getNumRows()).isEqualTo(2);
  }

  @Test
  void manyRows() throws Exception {
    StringBuilder csv = new StringBuilder("a,b\n");
    for (int r = 0; r < 10_000; r++) {
      csv.append(r).append(',').append(r * 2).append('\n');
    }
    TableData table = ChartCsvParser.parse(csv.toString());
    assertThat(table.getNumRows()).isEqualTo(10_001);
  }

  @Test
  void allNumericColumns_dataRowsNotRowHeaders() throws Exception {
    TableData table = ChartCsvParser.parse("x,y\n1,2\n3,4");
    assertThat(table.getTableCells(2).getText()).isEqualTo("1");
    assertThat(table.getTableCells(2).getRowHeader()).isFalse();
  }

  @Test
  void nanAndInfinityStrings_parseAsNumeric() throws Exception {
    // Double.parseDouble accepts these; pandas would too.
    TableData table = ChartCsvParser.parse("x\nNaN\nInfinity");
    assertThat(table.getTableCells(1).getRowHeader()).isFalse();
    assertThat(table.getTableCells(2).getRowHeader()).isFalse();
  }
}
