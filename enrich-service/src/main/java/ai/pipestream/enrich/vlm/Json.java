package ai.pipestream.enrich.vlm;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal JSON writer and parser for the VLM HTTP wire. The VLM endpoint is
 * the one place this service speaks JSON (OpenAI-compatible chat
 * completions); typed document data never round-trips through this class.
 */
public final class Json {

  private Json() {}

  /** Serializes a String for embedding in a JSON document, with quotes. */
  public static String quote(String value) {
    StringBuilder out = new StringBuilder(value.length() + 2);
    out.append('"');
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"' -> out.append("\\\"");
        case '\\' -> out.append("\\\\");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        default -> {
          if (c < 0x20) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
        }
      }
    }
    out.append('"');
    return out.toString();
  }

  /**
   * Parses a JSON document into Maps, Lists, Strings, Doubles, Booleans, and
   * null. Only as much JSON as the chat-completions wire needs.
   */
  public static Object parse(String text) throws IllegalArgumentException {
    Parser parser = new Parser(text);
    Object value = parser.readValue();
    parser.skipWhitespace();
    if (!parser.atEnd()) {
      throw new IllegalArgumentException("trailing content after JSON value");
    }
    return value;
  }

  /** Casts a parsed value to an object map or throws. */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> asObject(Object value) {
    if (!(value instanceof Map)) {
      throw new IllegalArgumentException("expected a JSON object");
    }
    return (Map<String, Object>) value;
  }

  /** Casts a parsed value to an array or throws. */
  @SuppressWarnings("unchecked")
  public static List<Object> asArray(Object value) {
    if (!(value instanceof List)) {
      throw new IllegalArgumentException("expected a JSON array");
    }
    return (List<Object>) value;
  }

  /** Casts a parsed value to a string or throws. */
  public static String asString(Object value) {
    if (!(value instanceof String)) {
      throw new IllegalArgumentException("expected a JSON string");
    }
    return (String) value;
  }

  private static final class Parser {
    /** Nesting cap: a hostile or broken endpoint must not be able to kill the
     * calling thread with a StackOverflowError. Far beyond anything the
     * chat-completions wire legitimately carries. */
    private static final int MAX_DEPTH = 512;

    private final String text;
    private int pos;
    private int depth;

    Parser(String text) {
      this.text = text;
    }

    boolean atEnd() {
      return pos >= text.length();
    }

    void skipWhitespace() {
      while (!atEnd() && Character.isWhitespace(text.charAt(pos))) {
        pos++;
      }
    }

    Object readValue() {
      skipWhitespace();
      if (atEnd()) {
        throw new IllegalArgumentException("unexpected end of JSON input");
      }
      char c = text.charAt(pos);
      return switch (c) {
        case '{' -> readObject();
        case '[' -> readArray();
        case '"' -> readString();
        case 't' -> readLiteral("true", Boolean.TRUE);
        case 'f' -> readLiteral("false", Boolean.FALSE);
        case 'n' -> readLiteral("null", null);
        default -> readNumber();
      };
    }

    private Object readLiteral(String literal, Object value) {
      if (!text.startsWith(literal, pos)) {
        throw new IllegalArgumentException("invalid JSON literal at offset " + pos);
      }
      pos += literal.length();
      return value;
    }

    private Map<String, Object> readObject() {
      enterContainer();
      pos++; // '{'
      Map<String, Object> object = new LinkedHashMap<>();
      skipWhitespace();
      if (!atEnd() && text.charAt(pos) == '}') {
        pos++;
        depth--;
        return object;
      }
      while (true) {
        skipWhitespace();
        String key = readString();
        skipWhitespace();
        expect(':');
        object.put(key, readValue());
        skipWhitespace();
        if (atEnd()) {
          throw new IllegalArgumentException("unterminated JSON object");
        }
        char c = text.charAt(pos);
        if (c == ',') {
          pos++;
        } else if (c == '}') {
          pos++;
          depth--;
          return object;
        } else {
          throw new IllegalArgumentException("expected ',' or '}' at offset " + pos);
        }
      }
    }

    private List<Object> readArray() {
      enterContainer();
      pos++; // '['
      List<Object> array = new ArrayList<>();
      skipWhitespace();
      if (!atEnd() && text.charAt(pos) == ']') {
        pos++;
        depth--;
        return array;
      }
      while (true) {
        array.add(readValue());
        skipWhitespace();
        if (atEnd()) {
          throw new IllegalArgumentException("unterminated JSON array");
        }
        char c = text.charAt(pos);
        if (c == ',') {
          pos++;
        } else if (c == ']') {
          pos++;
          depth--;
          return array;
        } else {
          throw new IllegalArgumentException("expected ',' or ']' at offset " + pos);
        }
      }
    }

    private void enterContainer() {
      if (++depth > MAX_DEPTH) {
        throw new IllegalArgumentException("JSON nesting deeper than " + MAX_DEPTH);
      }
    }

    private String readString() {
      expect('"');
      StringBuilder out = new StringBuilder();
      while (true) {
        if (atEnd()) {
          throw new IllegalArgumentException("unterminated JSON string");
        }
        char c = text.charAt(pos++);
        if (c == '"') {
          return out.toString();
        }
        if (c == '\\') {
          if (atEnd()) {
            throw new IllegalArgumentException("unterminated JSON escape");
          }
          char escape = text.charAt(pos++);
          switch (escape) {
            case '"' -> out.append('"');
            case '\\' -> out.append('\\');
            case '/' -> out.append('/');
            case 'b' -> out.append('\b');
            case 'f' -> out.append('\f');
            case 'n' -> out.append('\n');
            case 'r' -> out.append('\r');
            case 't' -> out.append('\t');
            case 'u' -> {
              if (pos + 4 > text.length()) {
                throw new IllegalArgumentException("truncated \\u escape");
              }
              out.append((char) Integer.parseInt(text.substring(pos, pos + 4), 16));
              pos += 4;
            }
            default -> throw new IllegalArgumentException("bad JSON escape: \\" + escape);
          }
        } else {
          out.append(c);
        }
      }
    }

    private Object readNumber() {
      int start = pos;
      while (!atEnd() && "-+0123456789.eE".indexOf(text.charAt(pos)) >= 0) {
        pos++;
      }
      if (start == pos) {
        throw new IllegalArgumentException("invalid JSON value at offset " + pos);
      }
      return Double.parseDouble(text.substring(start, pos));
    }

    private void expect(char c) {
      if (atEnd() || text.charAt(pos) != c) {
        throw new IllegalArgumentException("expected '" + c + "' at offset " + pos);
      }
      pos++;
    }
  }
}
