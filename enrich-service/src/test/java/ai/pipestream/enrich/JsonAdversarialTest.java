package ai.pipestream.enrich;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\":1"));
  }

  @Test
  void truncatedObjectAfterKey_isIllegalArgument() {
    assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\""));
  }

  @Test
  void truncatedObjectAfterComma_isIllegalArgument() {
    assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\":1,"));
  }

  @Test
  void truncatedArray_isIllegalArgument() {
    assertThrows(IllegalArgumentException.class, () -> Json.parse("[1,2"));
  }

  @Test
  void truncatedChatCompletionsBody_isIllegalArgument() {
    // The realistic shape: the VLM connection drops mid-body.
    assertThrows(IllegalArgumentException.class,
        () -> Json.parse("{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"half"));
  }

  @Test
  void emptyInput_isIllegalArgument() {
    assertThrows(IllegalArgumentException.class, () -> Json.parse(""));
  }

  @Test
  void whitespaceOnly_isIllegalArgument() {
    assertThrows(IllegalArgumentException.class, () -> Json.parse("   \n\t "));
  }

  @Test
  void truncatedUnicodeEscape_isIllegalArgument() {
    assertThrows(IllegalArgumentException.class, () -> Json.parse("\"\\u12\""));
  }

  @Test
  void truncatedEscape_isIllegalArgument() {
    assertThrows(IllegalArgumentException.class, () -> Json.parse("\"abc\\"));
  }

  // -------------------------------------------------------------------------
  // Deep nesting must not kill the thread with a StackOverflowError
  // -------------------------------------------------------------------------

  @Test
  void deeplyNestedArray_isRejectedNotStackOverflow() {
    String deep = "[".repeat(200_000) + "]".repeat(200_000);
    assertThrows(IllegalArgumentException.class, () -> Json.parse(deep));
  }

  @Test
  void deeplyNestedUnclosedArray_isRejectedNotStackOverflow() {
    String deep = "[".repeat(200_000);
    assertThrows(IllegalArgumentException.class, () -> Json.parse(deep));
  }

  // -------------------------------------------------------------------------
  // Escapes, unicode, surrogate pairs
  // -------------------------------------------------------------------------

  @Test
  void surrogatePairEscape_decodesToOneCodePoint() {
    Object parsed = Json.parse("{\"s\":\"\\uD83D\\uDE00\"}");
    assertEquals("😀", Json.asString(Json.asObject(parsed).get("s")));
  }

  @Test
  void nestedEscapes_roundTrip() {
    String original = "quote \" backslash \\ newline \n tab \t cr \r";
    Object parsed = Json.parse("{\"s\":" + Json.quote(original) + "}");
    assertEquals(original, Json.asString(Json.asObject(parsed).get("s")));
  }

  @Test
  void unicodeRoundTrip_throughQuoteAndParse() {
    String original = "héllo wörld — emoji 😀 and CJK 漢字";
    Object parsed = Json.parse("{\"s\":" + Json.quote(original) + "}");
    assertEquals(original, Json.asString(Json.asObject(parsed).get("s")));
  }

  @Test
  void controlCharacters_escapedByQuote() {
    String original = "\u0007bell\u000Cformfeed";
    String quoted = Json.quote(original);
    assertTrue(quoted.contains("\\u0007"));
    assertEquals(original, Json.asString(Json.parse(quoted)));
  }

  @Test
  void veryLongString_parses() {
    String longText = "x".repeat(1_000_000) + "😀";
    Object parsed = Json.parse(Json.quote(longText));
    assertEquals(longText, Json.asString(parsed));
  }

  // -------------------------------------------------------------------------
  // Numbers, nulls, duplicates, odd-but-valid shapes
  // -------------------------------------------------------------------------

  @Test
  void bareNumberTokens_parse() {
    assertEquals(List.of(1.0, -2.5, 1e3), Json.asArray(Json.parse("[1, -2.5, 1e3]")));
  }

  @Test
  void numberAsString_staysString() {
    Map<String, Object> parsed = Json.asObject(Json.parse("{\"n\":\"42\",\"m\":42}"));
    assertEquals("42", parsed.get("n"));
    assertEquals(42.0, parsed.get("m"));
  }

  @Test
  void nullValues_parse() {
    Map<String, Object> parsed = Json.asObject(Json.parse("{\"a\":null,\"b\":[null]}"));
    assertNull(parsed.get("a"));
    List<Object> array = Json.asArray(parsed.get("b"));
    assertEquals(1, array.size());
    assertNull(array.get(0));
  }

  @Test
  void duplicateKeys_lastWins() {
    assertEquals(2.0, Json.asObject(Json.parse("{\"a\":1,\"a\":2}")).get("a"));
  }

  @Test
  void arrayWhereObjectExpected_asObjectThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> Json.asObject(Json.parse("[{\"choices\":[]}]")));
  }

  @Test
  void htmlishContent_roundTrips() {
    String html = "<p class=\"x\">a & b < c > d</p>";
    Object parsed = Json.parse("{\"s\":" + Json.quote(html) + "}");
    assertEquals(html, Json.asString(Json.asObject(parsed).get("s")));
  }

  @Test
  void loneSurrogateEscape_parsesLeniently() {
    // Valid JSON (the escape is legal) but an invalid Unicode string. The
    // parser keeps it; proto serialization later substitutes '?' for the
    // unpaired surrogate rather than throwing. Lenient end to end, no crash.
    String parsed = Json.asString(Json.parse("\"a\\ud800b\""));
    assertEquals(3, parsed.length());
  }

  @Test
  void trailingGarbage_rejected() {
    assertThrows(IllegalArgumentException.class, () -> Json.parse("{} extra"));
    assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\":1} {\"b\":2}"));
  }

  @Test
  void badLiteral_rejected() {
    assertThrows(IllegalArgumentException.class, () -> Json.parse("tru"));
    assertThrows(IllegalArgumentException.class, () -> Json.parse("nulL"));
    assertThrows(IllegalArgumentException.class, () -> Json.parse("NaN"));
  }
}
