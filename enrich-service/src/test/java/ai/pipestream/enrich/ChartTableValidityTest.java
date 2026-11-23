package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.document.v1.DocItemLabel;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.PictureItem;
import ai.pipestream.document.v1.TableData;
import ai.pipestream.enrich.engine.ChartCsvParser;
import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.v1.ChartOutput;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.SkipReason;
import ai.pipestream.enrich.vlm.VlmClient.VlmException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A chart table is only a table when the reply holds one: at least two rows
 * and at least one number. A model that answers the chart prompt with prose
 * (a refusal, a description) must come back as a skip, never as a one-row
 * "table" counted as a success.
 */
class ChartTableValidityTest {

  @ParameterizedTest
  @ValueSource(strings = {
      "I'm sorry, I cannot read this chart.",
      "The image shows a bar chart.",
      "Sorry, I can't help with that.\nPlease provide a clearer image.",
      "This chart compares revenue, cost, and profit across regions.\n"
          + "Revenue is highest, cost is lower, and profit is lowest.",
  })
  void proseIsNotATable(String reply) {
    assertThatThrownBy(() -> ChartCsvParser.parse(ChartCsvParser.extractCsv(reply)))
        .isInstanceOf(VlmException.class);
  }

  @Test
  void oneRowIsNotATable() {
    // One row has either no header or no data; a chart has both.
    assertThatThrownBy(() -> ChartCsvParser.parse("year,sales"))
        .isInstanceOf(VlmException.class)
        .hasMessageContaining("row");
    assertThatThrownBy(() -> ChartCsvParser.parse("1,2,3")).isInstanceOf(VlmException.class);
  }

  @Test
  void rowsWithoutAnyNumberAreNotAChartTable() {
    assertThatThrownBy(() -> ChartCsvParser.parse("name\nalpha\nbeta"))
        .isInstanceOf(VlmException.class)
        .hasMessageContaining("numeric");
  }

  @Test
  void unterminatedQuoteIsAParseFailure() {
    // The open quote would swallow the rest of the reply into one cell.
    assertThatThrownBy(() -> ChartCsvParser.parse("year,sales\n2023,\"10\n2024,12"))
        .isInstanceOf(VlmException.class)
        .hasMessageContaining("quote");
    assertThatThrownBy(() -> ChartCsvParser.parse("\"a,b")).isInstanceOf(VlmException.class);
  }

  @Test
  void percentAndCurrencyCellsAreNumbers() throws Exception {
    // The labels a chart actually prints. They are data, not row headers, and
    // a table made only of them is still a table.
    TableData table = ChartCsvParser.parse(
        "region,share,revenue,change\nnorth,45%,\"$1,200\",-3.5%\nsouth,55 %,€ 980,+2%");
    assertThat(table.getNumRows()).isEqualTo(3);
    assertThat(table.getNumCols()).isEqualTo(4);
    // Row 1: north | 45% | $1,200 | -3.5%
    assertThat(table.getTableCells(4).getRowHeader()).as("north").isTrue();
    assertThat(table.getTableCells(5).getRowHeader()).as("45%").isFalse();
    assertThat(table.getTableCells(6).getText()).isEqualTo("$1,200");
    assertThat(table.getTableCells(6).getRowHeader()).as("$1,200").isFalse();
    assertThat(table.getTableCells(7).getRowHeader()).as("-3.5%").isFalse();
    // Row 2: south | 55 % | € 980 | +2%
    assertThat(table.getTableCells(9).getRowHeader()).as("55 %").isFalse();
    assertThat(table.getTableCells(10).getRowHeader()).as("€ 980").isFalse();
    assertThat(table.getTableCells(11).getRowHeader()).as("+2%").isFalse();
    // Cell text is never rewritten.
    assertThat(table.getTableCells(9).getText()).isEqualTo("55 %");
  }

  @Test
  void wordsAroundDigitsAreNotNumbers() throws Exception {
    // Tolerance stops at currency, percent, and thousands separators.
    TableData table = ChartCsvParser.parse("label,value\nQ1 2023,10\n5 apples,20\n%,30\n$,40");
    assertThat(table.getTableCells(2).getRowHeader()).as("Q1 2023").isTrue();
    assertThat(table.getTableCells(4).getRowHeader()).as("5 apples").isTrue();
    assertThat(table.getTableCells(6).getRowHeader()).as("%").isTrue();
    assertThat(table.getTableCells(8).getRowHeader()).as("$").isTrue();
  }

  @Test
  void refusalSentence_isAVlmErrorSkipNotAOneRowTable() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.responder = body -> "I'm sorry, I cannot read this chart.";
      PictureItem chart = InProcessEnrich.picture("#/pictures/0").toBuilder()
          .setLabel(DocItemLabel.DOC_ITEM_LABEL_CHART)
          .build();
      InProcessEnrich.Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoChartExtraction(true)
          .setReturnDocument(true)
          .setDocument(Document.newBuilder().setName("charts").addPictures(chart))
          .build());

      assertThat(result.error()).isNull();
      assertThat(result.annotations()).as("a refusal is not a chart table").isEmpty();
      assertThat(result.skips()).singleElement().satisfies(skip -> {
        assertThat(skip.getSelfRef()).isEqualTo("#/pictures/0");
        assertThat(skip.getReason()).isEqualTo(SkipReason.SKIP_REASON_VLM_ERROR);
        assertThat(skip.getChartOutput()).isEqualTo(ChartOutput.CHART_OUTPUT_CSV);
        assertThat(skip.getDetail()).isNotEmpty();
      });
      assertThat(result.complete().getSucceeded()).isZero();
      assertThat(result.complete().getSkipped()).isEqualTo(1);
      assertThat(result.complete().getDocument().getPictures(0).getAnnotationsList())
          .as("no tabular_chart annotation is written into the document")
          .isEmpty();
    }
  }
}
