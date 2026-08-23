package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.document.v1.TableData;
import ai.pipestream.enrich.engine.ChartCsvParser;
import ai.pipestream.enrich.vlm.VlmClient.VlmException;
import org.junit.jupiter.api.Test;

/** Chart CSV → typed TableData: the wire representation is typed cells. */
class ChartCsvParserTest {

  @Test
  void twoByThreeTable() throws Exception {
    TableData table = ChartCsvParser.parse("a,b,c\n1,2,3");
    assertThat(table.getNumRows()).isEqualTo(2);
    assertThat(table.getNumCols()).isEqualTo(3);
    assertThat(table.getTableCellsCount()).isEqualTo(6);
    assertThat(table.getTableCells(0).getColumnHeader()).isTrue();
    assertThat(table.getTableCells(0).getText()).isEqualTo("a");
    assertThat(table.getTableCells(5).getText()).isEqualTo("3");
    assertThat(table.getTableCells(5).getStartRowOffsetIdx()).isEqualTo(1);
    assertThat(table.getTableCells(5).getStartColOffsetIdx()).isEqualTo(2);
    assertThat(table.getGridCount()).isEqualTo(2);
    assertThat(table.getGrid(1).getCellsCount()).isEqualTo(3);
  }

  @Test
  void quotedFieldsWithCommasAndQuotes() throws Exception {
    TableData table = ChartCsvParser.parse("label,value\n\"a, b\",\"say \"\"hi\"\"\"");
    assertThat(table.getTableCells(2).getText()).isEqualTo("a, b");
    assertThat(table.getTableCells(3).getText()).isEqualTo("say \"hi\"");
  }

  @Test
  void allNumericFirstRow_noHeaderRow() throws Exception {
    // The first row is a header only when ALL its values are
    // non-numeric, so an all-numeric table has no header cells at all.
    TableData table = ChartCsvParser.parse("2023,10\n2024,12");
    assertThat(table.getNumRows()).isEqualTo(2);
    for (int i = 0; i < table.getTableCellsCount(); i++) {
      assertThat(table.getTableCells(i).getColumnHeader()).as("cell " + i).isFalse();
      assertThat(table.getTableCells(i).getRowHeader()).as("cell " + i).isFalse();
    }
  }

  @Test
  void mixedFirstRow_noHeaderRow() throws Exception {
    // One numeric value in the first row is enough to demote it to data.
    TableData table = ChartCsvParser.parse("year,2023\nsales,10");
    assertThat(table.getTableCells(0).getColumnHeader()).isFalse();
    assertThat(table.getTableCells(0).getRowHeader()).as("non-numeric data cell").isTrue();
    assertThat(table.getTableCells(1).getRowHeader()).as("numeric data cell").isFalse();
  }

  @Test
  void nonNumericDataCellsAreRowHeaders() throws Exception {
    TableData table = ChartCsvParser.parse("region,sales\nnorth,10\nsouth,20");
    assertThat(table.getTableCells(0).getColumnHeader()).isTrue();
    assertThat(table.getTableCells(1).getColumnHeader()).isTrue();
    assertThat(table.getTableCells(2).getRowHeader()).as("north").isTrue();
    assertThat(table.getTableCells(2).getColumnHeader()).isFalse();
    assertThat(table.getTableCells(3).getRowHeader()).as("10").isFalse();
    assertThat(table.getTableCells(4).getRowHeader()).as("south").isTrue();
    assertThat(table.getTableCells(5).getRowHeader()).as("20").isFalse();
  }

  @Test
  void emptyCellIsNonNumericRowHeader() throws Exception {
    // Empty cells count as non-numeric: text "" and row_header=true.
    TableData table = ChartCsvParser.parse("a,b\n,5");
    assertThat(table.getTableCells(2).getText()).isEqualTo("");
    assertThat(table.getTableCells(2).getRowHeader()).isTrue();
    assertThat(table.getTableCells(3).getRowHeader()).isFalse();
  }

  @Test
  void emptyInputRejected() {
    assertThatThrownBy(() -> ChartCsvParser.parse("")).isInstanceOf(VlmException.class);
  }

  @Test
  void raggedRowsRejected() {
    assertThatThrownBy(() -> ChartCsvParser.parse("a,b,c\n1,2")).isInstanceOf(VlmException.class);
  }
}
