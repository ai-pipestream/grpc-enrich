package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.enrich.vlm.Json;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Adversarial tests for the hand-rolled JSON codec on the VLM wire. The
 * parser's contract is {@code IllegalArgumentException} on malformed input;
 * anything else (IndexOutOfBounds, StackOverflow) escapes the error handling
 * in OpenAiCompatVlmClient and EnrichmentEngine and is a bug.
 */
class JsonAdversarialTest {

  // -------------------------------------------------------------------------
  // Truncated input must be IllegalArgumentException, never an index error
  // -------------------------------------------------------------------------

  @Test
  void truncatedObject_isIllegalArgument() {
    assertThatThrownBy(() -> Json.parse("{\"a\":1")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void truncatedObjectAfterKey_isIllegalArgument() {
    assertThatThrownBy(() -> Json.parse("{\"a\"")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void truncatedObjectAfterComma_isIllegalArgument() {
    assertThatThrownBy(() -> Json.parse("{\"a\":1,")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void truncatedArray_isIllegalArgument() {
    assertThatThrownBy(() -> Json.parse("[1,2")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void truncatedChatCompletionsBody_isIllegalArgument() {
    // The realistic shape: the VLM connection drops mid-body.
    assertThatThrownBy(
        () -> Json.parse("{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"half"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void emptyInput_isIllegalArgument() {
    assertThatThrownBy(() -> Json.parse("")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void whitespaceOnly_isIllegalArgument() {
    assertThatThrownBy(() -> Json.parse("   \n\t ")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void truncatedUnicodeEscape_isIllegalArgument() {
    assertThatThrownBy(() -> Json.parse("\"\\u12\"")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void truncatedEscape_isIllegalArgument() {
    assertThatThrownBy(() -> Json.parse("\"abc\\")).isInstanceOf(IllegalArgumentException.class);
  }

  // -------------------------------------------------------------------------
  // Deep nesting must not kill the thread with a StackOverflowError
  // -------------------------------------------------------------------------

  @Test
  void deeplyNestedArray_isRejectedNotStackOverflow() {
    String deep = "[".repeat(200_000) + "]".repeat(200_000);
    assertThatThrownBy(() -> Json.parse(deep)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void deeplyNestedUnclosedArray_isRejectedNotStackOverflow() {
    String deep = "[".repeat(200_000);
    assertThatThrownBy(() -> Json.parse(deep)).isInstanceOf(IllegalArgumentException.class);
  }

  // -------------------------------------------------------------------------
  // Escapes, unicode, surrogate pairs
  // -------------------------------------------------------------------------

  @Test
  void surrogatePairEscape_decodesToOneCodePoint() {
    Object parsed = Json.parse("{\"s\":\"\\uD83D\\uDE00\"}");
    assertThat(Json.asString(Json.asObject(parsed).get("s"))).isEqualTo("😀");
  }

  @Test
  void nestedEscapes_roundTrip() {
    String original = "quote \" backslash \\ newline \n tab \t cr \r";
    Object parsed = Json.parse("{\"s\":" + Json.quote(original) + "}");
    assertThat(Json.asString(Json.asObject(parsed).get("s"))).isEqualTo(original);
  }

  @Test
  void unicodeRoundTrip_throughQuoteAndParse() {
    String original = "héllo wörld — emoji 😀 and CJK 漢字";
    Object parsed = Json.parse("{\"s\":" + Json.quote(original) + "}");
    assertThat(Json.asString(Json.asObject(parsed).get("s"))).isEqualTo(original);
  }

  @Test
  void controlCharacters_escapedByQuote() {
    String original = "\u0007bell\u000Cformfeed";
    String quoted = Json.quote(original);
    assertThat(quoted).contains("\\u0007");
    assertThat(Json.asString(Json.parse(quoted))).isEqualTo(original);
  }

  @Test
  void veryLongString_parses() {
    String longText = "x".repeat(1_000_000) + "😀";
    Object parsed = Json.parse(Json.quote(longText));
    assertThat(Json.asString(parsed)).isEqualTo(longText);
  }

  // -------------------------------------------------------------------------
  // Numbers, nulls, duplicates, odd-but-valid shapes
  // -------------------------------------------------------------------------

  @Test
  void bareNumberTokens_parse() {
    assertThat(Json.asArray(Json.parse("[1, -2.5, 1e3]"))).isEqualTo(List.of(1.0, -2.5, 1e3));
  }

  @Test
  void numberAsString_staysString() {
    Map<String, Object> parsed = Json.asObject(Json.parse("{\"n\":\"42\",\"m\":42}"));
    assertThat(parsed.get("n")).isEqualTo("42");
    assertThat(parsed.get("m")).isEqualTo(42.0);
  }

  @Test
  void nullValues_parse() {
    Map<String, Object> parsed = Json.asObject(Json.parse("{\"a\":null,\"b\":[null]}"));
    assertThat(parsed.get("a")).isNull();
    List<Object> array = Json.asArray(parsed.get("b"));
    assertThat(array.size()).isEqualTo(1);
    assertThat(array.get(0)).isNull();
  }

  @Test
  void duplicateKeys_lastWins() {
    assertThat(Json.asObject(Json.parse("{\"a\":1,\"a\":2}")).get("a")).isEqualTo(2.0);
  }

  @Test
  void arrayWhereObjectExpected_asObjectThrows() {
    assertThatThrownBy(() -> Json.asObject(Json.parse("[{\"choices\":[]}]")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void htmlishContent_roundTrips() {
    String html = "<p class=\"x\">a & b < c > d</p>";
    Object parsed = Json.parse("{\"s\":" + Json.quote(html) + "}");
    assertThat(Json.asString(Json.asObject(parsed).get("s"))).isEqualTo(html);
  }

  @Test
  void loneSurrogateEscape_parsesLeniently() {
    // Valid JSON (the escape is legal) but an invalid Unicode string. The
    // parser keeps it; proto serialization later substitutes '?' for the
    // unpaired surrogate rather than throwing. Lenient end to end, no crash.
    String parsed = Json.asString(Json.parse("\"a\\ud800b\""));
    assertThat(parsed.length()).isEqualTo(3);
  }

  @Test
  void trailingGarbage_rejected() {
    assertThatThrownBy(() -> Json.parse("{} extra")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Json.parse("{\"a\":1} {\"b\":2}"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void badLiteral_rejected() {
    assertThatThrownBy(() -> Json.parse("tru")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Json.parse("nulL")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Json.parse("NaN")).isInstanceOf(IllegalArgumentException.class);
  }
}
