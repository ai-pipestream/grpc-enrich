package ai.pipestream.enrich.engine;

import ai.pipestream.document.v1.CodeLanguageLabel;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Output post-processing for code/formula VLM responses:
 * truncate at {@code <end_of_utterance>}, strip the
 * closing-tag and location sentinels, lstrip, then (code only) parse a
 * leading {@code <_language_>} token into a CodeLanguageLabel with an UNKNOWN
 * fallback. Applied to every code/formula response, whether the prompt was
 * the bare image prompt or the text-only fallback.
 */
public final class CodeFormulaPostProcessor {

  /** The result of post-processing a code response. */
  public record CodeResult(String text, CodeLanguageLabel language, String languageRaw) {}

  /** The leading {@code ^<_([^_>]+)_>\s*(.*)} language token. */
  private static final Pattern LANGUAGE_TOKEN =
      Pattern.compile("^<_([^_>]+)_>\\s*(.*)", Pattern.DOTALL);

  /** Model-output sentinels stripped from every response. */
  private static final List<String> SENTINELS =
      List.of("</code>", "</formula>", "<loc_0><loc_0><loc_500><loc_500>");

  /** CodeLanguageLabel value strings (the document schema's spellings) to
   * our proto enum. Matching is exact-case; anything else falls back to
   * UNKNOWN with the raw string preserved. */
  private static final Map<String, CodeLanguageLabel> LANGUAGES = Map.ofEntries(
      Map.entry("Ada", CodeLanguageLabel.CODE_LANGUAGE_LABEL_ADA),
      Map.entry("Awk", CodeLanguageLabel.CODE_LANGUAGE_LABEL_AWK),
      Map.entry("Bash", CodeLanguageLabel.CODE_LANGUAGE_LABEL_BASH),
      Map.entry("bc", CodeLanguageLabel.CODE_LANGUAGE_LABEL_BC),
      Map.entry("C", CodeLanguageLabel.CODE_LANGUAGE_LABEL_C),
      Map.entry("C#", CodeLanguageLabel.CODE_LANGUAGE_LABEL_C_SHARP),
      Map.entry("C++", CodeLanguageLabel.CODE_LANGUAGE_LABEL_C_PLUS_PLUS),
      Map.entry("CMake", CodeLanguageLabel.CODE_LANGUAGE_LABEL_CMAKE),
      Map.entry("COBOL", CodeLanguageLabel.CODE_LANGUAGE_LABEL_COBOL),
      Map.entry("CSS", CodeLanguageLabel.CODE_LANGUAGE_LABEL_CSS),
      Map.entry("Ceylon", CodeLanguageLabel.CODE_LANGUAGE_LABEL_CEYLON),
      Map.entry("Clojure", CodeLanguageLabel.CODE_LANGUAGE_LABEL_CLOJURE),
      Map.entry("Crystal", CodeLanguageLabel.CODE_LANGUAGE_LABEL_CRYSTAL),
      Map.entry("Cuda", CodeLanguageLabel.CODE_LANGUAGE_LABEL_CUDA),
      Map.entry("Cython", CodeLanguageLabel.CODE_LANGUAGE_LABEL_CYTHON),
      Map.entry("D", CodeLanguageLabel.CODE_LANGUAGE_LABEL_D),
      Map.entry("Dart", CodeLanguageLabel.CODE_LANGUAGE_LABEL_DART),
      Map.entry("dc", CodeLanguageLabel.CODE_LANGUAGE_LABEL_DC),
      Map.entry("Dockerfile", CodeLanguageLabel.CODE_LANGUAGE_LABEL_DOCKERFILE),
      Map.entry("DocLang", CodeLanguageLabel.CODE_LANGUAGE_LABEL_DOCLANG),
      Map.entry("Elixir", CodeLanguageLabel.CODE_LANGUAGE_LABEL_ELIXIR),
      Map.entry("Erlang", CodeLanguageLabel.CODE_LANGUAGE_LABEL_ERLANG),
      Map.entry("FORTRAN", CodeLanguageLabel.CODE_LANGUAGE_LABEL_FORTRAN),
      Map.entry("Forth", CodeLanguageLabel.CODE_LANGUAGE_LABEL_FORTH),
      Map.entry("Go", CodeLanguageLabel.CODE_LANGUAGE_LABEL_GO),
      Map.entry("HTML", CodeLanguageLabel.CODE_LANGUAGE_LABEL_HTML),
      Map.entry("Haskell", CodeLanguageLabel.CODE_LANGUAGE_LABEL_HASKELL),
      Map.entry("Haxe", CodeLanguageLabel.CODE_LANGUAGE_LABEL_HAXE),
      Map.entry("Java", CodeLanguageLabel.CODE_LANGUAGE_LABEL_JAVA),
      Map.entry("JavaScript", CodeLanguageLabel.CODE_LANGUAGE_LABEL_JAVASCRIPT),
      Map.entry("JSON", CodeLanguageLabel.CODE_LANGUAGE_LABEL_JSON),
      Map.entry("Julia", CodeLanguageLabel.CODE_LANGUAGE_LABEL_JULIA),
      Map.entry("Kotlin", CodeLanguageLabel.CODE_LANGUAGE_LABEL_KOTLIN),
      Map.entry("Latex", CodeLanguageLabel.CODE_LANGUAGE_LABEL_LATEX),
      Map.entry("Lisp", CodeLanguageLabel.CODE_LANGUAGE_LABEL_LISP),
      Map.entry("Lua", CodeLanguageLabel.CODE_LANGUAGE_LABEL_LUA),
      Map.entry("Matlab", CodeLanguageLabel.CODE_LANGUAGE_LABEL_MATLAB),
      Map.entry("MoonScript", CodeLanguageLabel.CODE_LANGUAGE_LABEL_MOONSCRIPT),
      Map.entry("Nim", CodeLanguageLabel.CODE_LANGUAGE_LABEL_NIM),
      Map.entry("OCaml", CodeLanguageLabel.CODE_LANGUAGE_LABEL_OCAML),
      Map.entry("ObjectiveC", CodeLanguageLabel.CODE_LANGUAGE_LABEL_OBJECTIVEC),
      Map.entry("Octave", CodeLanguageLabel.CODE_LANGUAGE_LABEL_OCTAVE),
      Map.entry("PHP", CodeLanguageLabel.CODE_LANGUAGE_LABEL_PHP),
      Map.entry("Pascal", CodeLanguageLabel.CODE_LANGUAGE_LABEL_PASCAL),
      Map.entry("Perl", CodeLanguageLabel.CODE_LANGUAGE_LABEL_PERL),
      Map.entry("Prolog", CodeLanguageLabel.CODE_LANGUAGE_LABEL_PROLOG),
      Map.entry("Python", CodeLanguageLabel.CODE_LANGUAGE_LABEL_PYTHON),
      Map.entry("Racket", CodeLanguageLabel.CODE_LANGUAGE_LABEL_RACKET),
      Map.entry("Ruby", CodeLanguageLabel.CODE_LANGUAGE_LABEL_RUBY),
      Map.entry("Rust", CodeLanguageLabel.CODE_LANGUAGE_LABEL_RUST),
      Map.entry("SML", CodeLanguageLabel.CODE_LANGUAGE_LABEL_SML),
      Map.entry("SQL", CodeLanguageLabel.CODE_LANGUAGE_LABEL_SQL),
      Map.entry("Scala", CodeLanguageLabel.CODE_LANGUAGE_LABEL_SCALA),
      Map.entry("Scheme", CodeLanguageLabel.CODE_LANGUAGE_LABEL_SCHEME),
      Map.entry("Swift", CodeLanguageLabel.CODE_LANGUAGE_LABEL_SWIFT),
      Map.entry("Tikz", CodeLanguageLabel.CODE_LANGUAGE_LABEL_TIKZ),
      Map.entry("TypeScript", CodeLanguageLabel.CODE_LANGUAGE_LABEL_TYPESCRIPT),
      Map.entry("unknown", CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN),
      Map.entry("VisualBasic", CodeLanguageLabel.CODE_LANGUAGE_LABEL_VISUALBASIC),
      Map.entry("XML", CodeLanguageLabel.CODE_LANGUAGE_LABEL_XML),
      Map.entry("YAML", CodeLanguageLabel.CODE_LANGUAGE_LABEL_YAML));

  private CodeFormulaPostProcessor() {}

  /** Cleans a raw response: truncate at the end-of-utterance marker (with or
   * without the closing angle bracket), remove the closing-tag and location
   * sentinels, then lstrip. */
  public static String clean(String raw) {
    String text = raw;
    int idx = text.indexOf("<end_of_utterance>");
    if (idx == -1) {
      idx = text.indexOf("<end_of_utterance");
    }
    if (idx != -1) {
      text = text.substring(0, idx);
    }
    for (String sentinel : SENTINELS) {
      text = text.replace(sentinel, "");
    }
    return text.stripLeading();
  }

  /** Post-processes a code response: clean, then split off a leading
   * {@code <_language_>} token. Language is UNKNOWN when no token is present
   * or the token names a language the enum does not know; languageRaw carries
   * the token string whenever one was present. */
  public static CodeResult processCode(String raw) {
    String cleaned = clean(raw);
    Matcher token = LANGUAGE_TOKEN.matcher(cleaned);
    if (token.matches()) {
      String languageRaw = token.group(1);
      return new CodeResult(token.group(2), toLabel(languageRaw), languageRaw);
    }
    return new CodeResult(cleaned, CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN, "");
  }

  /** Post-processes a formula response: clean only, no language token. */
  public static String processFormula(String raw) {
    return clean(raw);
  }

  private static CodeLanguageLabel toLabel(String languageRaw) {
    return LANGUAGES.getOrDefault(languageRaw, CodeLanguageLabel.CODE_LANGUAGE_LABEL_UNKNOWN);
  }
}
