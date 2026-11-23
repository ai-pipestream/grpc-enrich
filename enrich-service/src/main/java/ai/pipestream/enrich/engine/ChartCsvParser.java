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
 * row_header=true; spans are 1x1 and no bbox is set. A value is numeric the
 * way a chart prints numbers: a plain number, or one with a currency symbol,
 * a percent sign, or thousands separators ({@code $1,200}, {@code 45%}).
 *
 * <p>What is not a chart table is refused, never typed: a reply with fewer
 * than two rows, with no numeric cell at all, with ragged rows, or with a
 * quote left open. A model that answers the chart prompt with prose ("I'm
 * sorry, I cannot read this chart.") lands there, so the caller gets an
 * ItemSkipped rather than a one-row "table".
 */
public final class ChartCsvParser {

  // Docling's _extract_csv_to_dataframe: the body of a fenced csv block when
  // there is one; otherwise the reply with any leading ```/```csv fence and
  // trailing ``` fence removed.
  private static final Pattern CSV_FENCE =
      Pattern.compile("```csv\\s*\\n(.*?)\\n```", Pattern.DOTALL);
  private static final Pattern LEADING_FENCE = Pattern.compile("^`{3,}(?:csv)?\\s*");
  private static final Pattern TRAILING_FENCE = Pattern.compile("`{3,}\\s*$");

  /** A plain decimal, with or without thousands separators: 1200, 1,200,
   * 1,200.50, .5, 3e6. */
  private static final Pattern PLAIN_NUMBER = Pattern.compile(
      "(?:\\d{1,3}(?:,\\d{3})+|\\d+)?(?:\\.\\d*)?(?:[eE][+-]?\\d+)?");

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
   * @throws VlmException when the text is not a chart table: it is empty,
   *     has a single row, has no numeric cell, has ragged rows, or leaves a
   *     quote open
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
    if (rows.size() < 2) {
      // One row is a header with no data or data with no header; prose with
      // a comma in it parses as exactly this.
      throw new VlmException("chart model returned a single row, not a table with data rows");
    }
    if (rows.stream().noneMatch(row -> row.stream().anyMatch(ChartCsvParser::isNumeric))) {
      throw new VlmException("chart model returned a table with no numeric value in any cell");
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

  /** A value is numeric when it parses as a double, or when it is a number
   * the way a chart prints one: an optional sign, a currency symbol before
   * or after, a trailing percent sign, thousands separators. Empty cells
   * count as non-numeric, and so does any value with other words in it. */
  private static boolean isNumeric(String value) {
    String trimmed = value.strip();
    if (trimmed.isEmpty()) {
      return false;
    }
    try {
      Double.parseDouble(trimmed);
      return true;
    } catch (NumberFormatException notADouble) {
      return isPrintedNumber(trimmed);
    }
  }

  /** {@code -3.5%}, {@code $1,200}, {@code 980 €}: a sign, then a plain
   * number with at most a currency symbol on either side and a percent sign
   * after it. */
  private static boolean isPrintedNumber(String value) {
    int start = 0;
    int end = value.length();
    if (start < end && (value.charAt(start) == '+' || value.charAt(start) == '-')) {
      start++;
    }
    if (start < end && Character.getType(value.charAt(start)) == Character.CURRENCY_SYMBOL) {
      start++;
    } else if (end > start
        && Character.getType(value.charAt(end - 1)) == Character.CURRENCY_SYMBOL) {
      end--;
    }
    while (start < end && Character.isWhitespace(value.charAt(start))) {
      start++;
    }
    while (end > start && Character.isWhitespace(value.charAt(end - 1))) {
      end--;
    }
    if (end > start && value.charAt(end - 1) == '%') {
      end--;
      while (end > start && Character.isWhitespace(value.charAt(end - 1))) {
        end--;
      }
    }
    String number = value.substring(start, end);
    return number.chars().anyMatch(Character::isDigit)
        && PLAIN_NUMBER.matcher(number).matches();
  }

  private static List<List<String>> parseRows(String csv) throws VlmException {
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
    if (inQuotes) {
      // Everything after the open quote was read into one cell, row breaks
      // included; the rows that follow it are gone.
      throw new VlmException("chart model returned CSV with a quote that is never closed");
    }
    if (hasContent || !row.isEmpty()) {
      row.add(field.toString());
      rows.add(row);
    }
    return rows;
  }
}
