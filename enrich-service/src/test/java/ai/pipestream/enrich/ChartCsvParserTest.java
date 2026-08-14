package ai.pipestream.enrich;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
  void emptyInputRejected() {
    assertThrows(VlmException.class, () -> ChartCsvParser.parse(""));
  }

  @Test
  void raggedRowsRejected() {
    assertThrows(VlmException.class, () -> ChartCsvParser.parse("a,b,c\n1,2"));
  }
}
