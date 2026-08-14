package ai.pipestream.enrich;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.pipestream.document.v1.TableData;
import ai.pipestream.enrich.engine.ChartCsvParser;
import ai.pipestream.enrich.vlm.VlmClient.VlmException;
import org.junit.jupiter.api.Test;

/** Chart CSV → typed TableData: the wire representation is typed cells. */
class ChartCsvParserTest {

  @Test
  void twoByThreeTable() throws Exception {
    TableData table = ChartCsvParser.parse("a,b,c\n1,2,3");
    assertEquals(2, table.getNumRows());
    assertEquals(3, table.getNumCols());
    assertEquals(6, table.getTableCellsCount());
    assertTrue(table.getTableCells(0).getColumnHeader());
    assertEquals("a", table.getTableCells(0).getText());
    assertEquals("3", table.getTableCells(5).getText());
    assertEquals(1, table.getTableCells(5).getStartRowOffsetIdx());
    assertEquals(2, table.getTableCells(5).getStartColOffsetIdx());
    assertEquals(2, table.getGridCount());
    assertEquals(3, table.getGrid(1).getCellsCount());
  }

  @Test
  void quotedFieldsWithCommasAndQuotes() throws Exception {
    TableData table = ChartCsvParser.parse("label,value\n\"a, b\",\"say \"\"hi\"\"\"");
    assertEquals("a, b", table.getTableCells(2).getText());
    assertEquals("say \"hi\"", table.getTableCells(3).getText());
  }

  @Test
  void allNumericFirstRow_noHeaderRow() throws Exception {
    // Docling: the first row is a header only when ALL its values are
    // non-numeric, so an all-numeric table has no header cells at all.
    TableData table = ChartCsvParser.parse("2023,10\n2024,12");
    assertEquals(2, table.getNumRows());
    for (int i = 0; i < table.getTableCellsCount(); i++) {
      assertFalse(table.getTableCells(i).getColumnHeader(), "cell " + i);
      assertFalse(table.getTableCells(i).getRowHeader(), "cell " + i);
    }
  }

  @Test
  void mixedFirstRow_noHeaderRow() throws Exception {
    // One numeric value in the first row is enough to demote it to data.
    TableData table = ChartCsvParser.parse("year,2023\nsales,10");
    assertFalse(table.getTableCells(0).getColumnHeader());
    assertTrue(table.getTableCells(0).getRowHeader(), "non-numeric data cell");
    assertFalse(table.getTableCells(1).getRowHeader(), "numeric data cell");
  }

  @Test
  void nonNumericDataCellsAreRowHeaders() throws Exception {
    TableData table = ChartCsvParser.parse("region,sales\nnorth,10\nsouth,20");
    assertTrue(table.getTableCells(0).getColumnHeader());
    assertTrue(table.getTableCells(1).getColumnHeader());
    assertTrue(table.getTableCells(2).getRowHeader(), "north");
    assertFalse(table.getTableCells(2).getColumnHeader());
    assertFalse(table.getTableCells(3).getRowHeader(), "10");
    assertTrue(table.getTableCells(4).getRowHeader(), "south");
    assertFalse(table.getTableCells(5).getRowHeader(), "20");
  }

  @Test
  void emptyCellIsNonNumericRowHeader() throws Exception {
    // Docling treats NaN/empty as non-numeric: text "" and row_header=true.
    TableData table = ChartCsvParser.parse("a,b\n,5");
    assertEquals("", table.getTableCells(2).getText());
    assertTrue(table.getTableCells(2).getRowHeader());
    assertFalse(table.getTableCells(3).getRowHeader());
  }

  @Test
  void emptyInputRejected() {
    assertThrows(VlmException.class, () -> ChartCsvParser.parse(""));
  }

  @Test
  void raggedRowsRejected() {
    assertThrows(VlmException.class, () -> ChartCsvParser.parse("a,b,c\n1,2"));
  }
}
