package ai.pipestream.enrich;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    assertEquals(3, table.getNumRows());
    assertEquals(2, table.getNumCols());
    assertEquals("2024", table.getTableCells(4).getText());
  }

  @Test
  void loneCarriageReturnLineEndings() throws Exception {
    TableData table = ChartCsvParser.parse("a,b\r1,2");
    assertEquals(2, table.getNumRows());
    assertEquals("1", table.getTableCells(2).getText());
  }

  @Test
  void quotedFieldWithEmbeddedNewlineAndCrlf() throws Exception {
    TableData table = ChartCsvParser.parse("\"a\nb\",c\r\n1,2");
    assertEquals(2, table.getNumRows());
    assertEquals("a\nb", table.getTableCells(0).getText());
  }

  @Test
  void quotedFieldWithDoubledQuotesAndCommas() throws Exception {
    TableData table = ChartCsvParser.parse("\"say \"\"hi\"\"\",x\n1,2");
    assertEquals("say \"hi\"", table.getTableCells(0).getText());
  }

  @Test
  void trailingEmptyLinesDropped() throws Exception {
    TableData table = ChartCsvParser.parse("a,b\n1,2\n\n\n");
    assertEquals(2, table.getNumRows());
  }

  @Test
  void blankLineInMiddleDropped() throws Exception {
    TableData table = ChartCsvParser.parse("a,b\n\n1,2");
    assertEquals(2, table.getNumRows());
    assertEquals("1", table.getTableCells(2).getText());
  }

  @Test
  void singleRowAllNumeric_noHeader() throws Exception {
    TableData table = ChartCsvParser.parse("1,2,3");
    assertEquals(1, table.getNumRows());
    assertEquals(3, table.getNumCols());
    for (int i = 0; i < 3; i++) {
      assertFalse(table.getTableCells(i).getColumnHeader());
      assertFalse(table.getTableCells(i).getRowHeader());
    }
  }

  @Test
  void singleColumn() throws Exception {
    TableData table = ChartCsvParser.parse("name\nalpha\nbeta");
    assertEquals(3, table.getNumRows());
    assertEquals(1, table.getNumCols());
    assertTrue(table.getTableCells(0).getColumnHeader());
    assertEquals("beta", table.getTableCells(2).getText());
  }

  @Test
  void cellsWithSpaces_numericDetectionTrimsTextPreserved() throws Exception {
    TableData table = ChartCsvParser.parse("h1,h2\n 1 , x ");
    // " 1 " parses as numeric -> not a row header; " x " is a row header.
    assertFalse(table.getTableCells(2).getRowHeader());
    assertTrue(table.getTableCells(3).getRowHeader());
    // Cell text is preserved verbatim, spaces and all.
    assertEquals(" 1 ", table.getTableCells(2).getText());
  }

  @Test
  void firstRowAllEmptyStrings_isHeaderRow() throws Exception {
    // Docling parity: header iff ALL first-row values are non-numeric; empty
    // strings are non-numeric, so this degenerate row is a header.
    TableData table = ChartCsvParser.parse(",\n1,2");
    assertTrue(table.getTableCells(0).getColumnHeader());
    assertEquals("", table.getTableCells(0).getText());
  }

  @Test
  void emptyCellVsMissingCell() throws Exception {
    TableData table = ChartCsvParser.parse("a,b,c\n1,,3");
    assertEquals("", table.getTableCells(4).getText());
    // Empty data cell counts as non-numeric (pandas NaN parity) -> row header.
    assertTrue(table.getTableCells(4).getRowHeader());
    assertThrows(VlmException.class, () -> ChartCsvParser.parse("a,b,c\n1,2"));
  }

  @Test
  void onlyNewline_rejected() {
    assertThrows(VlmException.class, () -> ChartCsvParser.parse("\n"));
  }

  @Test
  void onlyBlankLines_rejected() {
    assertThrows(VlmException.class, () -> ChartCsvParser.parse("\n\n\n"));
  }

  @Test
  void unterminatedQuote_lenientSingleRow() throws Exception {
    TableData table = ChartCsvParser.parse("\"a,b");
    assertEquals(1, table.getNumRows());
    assertEquals("a,b", table.getTableCells(0).getText());
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
    assertEquals(2000, table.getNumCols());
    assertEquals(2, table.getNumRows());
  }

  @Test
  void manyRows() throws Exception {
    StringBuilder csv = new StringBuilder("a,b\n");
    for (int r = 0; r < 10_000; r++) {
      csv.append(r).append(',').append(r * 2).append('\n');
    }
    TableData table = ChartCsvParser.parse(csv.toString());
    assertEquals(10_001, table.getNumRows());
  }

  @Test
  void allNumericColumns_dataRowsNotRowHeaders() throws Exception {
    TableData table = ChartCsvParser.parse("x,y\n1,2\n3,4");
    assertEquals("1", table.getTableCells(2).getText());
    assertFalse(table.getTableCells(2).getRowHeader());
  }

  @Test
  void nanAndInfinityStrings_parseAsNumeric() throws Exception {
    // Double.parseDouble accepts these; pandas would too.
    TableData table = ChartCsvParser.parse("x\nNaN\nInfinity");
    assertFalse(table.getTableCells(1).getRowHeader());
    assertFalse(table.getTableCells(2).getRowHeader());
  }
}
