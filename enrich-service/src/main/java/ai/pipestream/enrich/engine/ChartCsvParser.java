package ai.pipestream.enrich.engine;

import ai.pipestream.document.v1.TableCell;
import ai.pipestream.document.v1.TableData;
import ai.pipestream.document.v1.TableRow;
import ai.pipestream.enrich.vlm.VlmClient.VlmException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns the CSV a chart2csv-style VLM returns into typed TableData cells.
 * The chart table is typed cells on the wire; the raw CSV only rides along in
 * the annotation for debugging.
 *
 * <p>Typing rules: the first row is a column-header row only when ALL its
 * values are non-numeric; any non-numeric data cell is marked
 * row_header=true; spans are 1x1 and no bbox is set.
 */
public final class ChartCsvParser {

  // Docling's _extract_csv_to_dataframe: the body of a fenced csv block when
  // there is one; otherwise the reply with any leading ```/```csv fence and
  // trailing ``` fence removed.
  private static final Pattern CSV_FENCE =
      Pattern.compile("```csv\\s*\\n(.*?)\\n```", Pattern.DOTALL);
  private static final Pattern LEADING_FENCE = Pattern.compile("^`{3,}(?:csv)?\\s*");
  private static final Pattern TRAILING_FENCE = Pattern.compile("`{3,}\\s*$");

  private ChartCsvParser() {}

  /**
   * The CSV inside a chart2csv reply, the way Docling reads it: the first
   * fenced {@code ```csv} block's body (stripped) when there is one,
   * otherwise the whole reply stripped of a leading {@code ```} or
   * {@code ```csv} fence and a trailing {@code ```} fence. A bare CSV reply
   * comes back unchanged apart from surrounding whitespace.
   */
  public static String extractCsv(String reply) {
    Matcher fenced = CSV_FENCE.matcher(reply);
    if (fenced.find()) {
      return fenced.group(1).strip();
    }
    String csv = LEADING_FENCE.matcher(reply.strip()).replaceFirst("");
    return TRAILING_FENCE.matcher(csv).replaceFirst("").strip();
  }

  /**
   * Parses CSV text (RFC 4180-ish: quoted fields, embedded commas and quotes)
   * into a TableData, inferring the header row from numeric content.
   *
   * @throws VlmException when the text is empty or the rows are ragged
   */
  public static TableData parse(String csv) throws VlmException {
    List<List<String>> rows = parseRows(csv);
    if (rows.isEmpty()) {
      throw new VlmException("chart model returned no CSV rows");
    }
    int numCols = rows.get(0).size();
    if (numCols == 0) {
      throw new VlmException("chart model returned an empty first row");
    }
    for (List<String> row : rows) {
      if (row.size() != numCols) {
        throw new VlmException("chart model returned ragged CSV rows");
      }
    }
    boolean firstRowIsHeader = rows.get(0).stream().allMatch(value -> !isNumeric(value));
    TableData.Builder table = TableData.newBuilder()
        .setNumRows(rows.size())
        .setNumCols(numCols);
    for (int r = 0; r < rows.size(); r++) {
      TableRow.Builder gridRow = TableRow.newBuilder();
      boolean headerRow = firstRowIsHeader && r == 0;
      for (int c = 0; c < numCols; c++) {
        String value = rows.get(r).get(c);
        TableCell cell = TableCell.newBuilder()
            .setText(value)
            .setRowSpan(1)
            .setColSpan(1)
            .setStartRowOffsetIdx(r)
            .setEndRowOffsetIdx(r + 1)
            .setStartColOffsetIdx(c)
            .setEndColOffsetIdx(c + 1)
            .setColumnHeader(headerRow)
            .setRowHeader(!headerRow && !isNumeric(value))
            .build();
        table.addTableCells(cell);
        gridRow.addCells(cell);
      }
      table.addGrid(gridRow);
    }
    return table.build();
  }

  /** A value is numeric when it parses as a double; empty cells count as
   * non-numeric. */
  private static boolean isNumeric(String value) {
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return false;
    }
    try {
      Double.parseDouble(trimmed);
      return true;
    } catch (NumberFormatException notNumeric) {
      return false;
    }
  }

  private static List<List<String>> parseRows(String csv) {
    List<List<String>> rows = new ArrayList<>();
    List<String> row = new ArrayList<>();
    StringBuilder field = new StringBuilder();
    boolean inQuotes = false;
    boolean hasContent = false;
    for (int i = 0; i < csv.length(); i++) {
      char c = csv.charAt(i);
      if (inQuotes) {
        if (c == '"') {
          if (i + 1 < csv.length() && csv.charAt(i + 1) == '"') {
            field.append('"');
            i++;
          } else {
            inQuotes = false;
          }
        } else {
          field.append(c);
        }
      } else if (c == '"') {
        inQuotes = true;
        hasContent = true;
      } else if (c == ',') {
        row.add(field.toString());
        field.setLength(0);
        hasContent = true;
      } else if (c == '\n' || c == '\r') {
        if (c == '\r' && i + 1 < csv.length() && csv.charAt(i + 1) == '\n') {
          i++;
        }
        row.add(field.toString());
        field.setLength(0);
        if (hasContent || row.size() > 1 || !row.get(0).isEmpty()) {
          rows.add(row);
        }
        row = new ArrayList<>();
        hasContent = false;
      } else {
        field.append(c);
        hasContent = true;
      }
    }
    if (hasContent || !row.isEmpty()) {
      row.add(field.toString());
      rows.add(row);
    }
    return rows;
  }
}
