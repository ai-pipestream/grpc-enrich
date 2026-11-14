package ai.pipestream.enrich.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The formula text kept from a VLM reply.
 *
 * <p>A reply to the CodeFormula token goes through
 * {@link CodeFormulaPostProcessor#processFormula(String)} exactly as before,
 * so Docling parity is unchanged. A reply to one of the LaTeX prompts is
 * cleaned further, since a general VLM wraps its answer: code fences, a
 * {@code \documentclass ... \begin{document}} shell, and sometimes prose
 * around a display block. The LaTeX kept is the longest display block when
 * there is one; otherwise the longest inline block of a reply that opens as
 * prose; otherwise the reply itself.
 */
public final class FormulaReply {

  private static final Pattern FENCE = Pattern.compile("```(?:latex|tex)?");
  private static final Pattern SHELL = Pattern.compile(
      "\\\\documentclass.*?\\\\begin\\{document\\}|\\\\end\\{document\\}|\\\\usepackage(?:\\[[^\\]]*\\])?\\{[^}]*\\}",
      Pattern.DOTALL);
  private static final Pattern DISPLAY = Pattern.compile(
      "\\\\\\[(.*?)\\\\\\]|\\$\\$(.*?)\\$\\$|\\\\begin\\{(?:equation|align|gather)\\*?\\}(.*?)"
          + "\\\\end\\{(?:equation|align|gather)\\*?\\}",
      Pattern.DOTALL);
  private static final Pattern INLINE =
      Pattern.compile("\\\\\\((.*?)\\\\\\)|(?<!\\\\)\\$(.+?)(?<!\\\\)\\$", Pattern.DOTALL);
  private static final Pattern PROSE = Pattern.compile("^\\s*(The|This|It|Here|In|To)\\b");

  private FormulaReply() {}

  /** The formula text for a reply {@code content} to {@code prompt}. */
  public static String text(String prompt, String content) {
    String base = CodeFormulaPostProcessor.processFormula(content);
    return FormulaPrompts.CODEFORMULA_TOKEN.equals(prompt) ? base : latex(base);
  }

  /** The LaTeX inside a general VLM's reply. */
  static String latex(String reply) {
    String text = SHELL.matcher(FENCE.matcher(reply == null ? "" : reply).replaceAll("")).replaceAll(" ");
    String display = longest(DISPLAY.matcher(text));
    if (!display.isEmpty()) {
      return display.strip();
    }
    if (PROSE.matcher(text).find()) {
      String inline = longest(INLINE.matcher(text));
      if (!inline.isEmpty()) {
        return inline.strip();
      }
    }
    return text.strip();
  }

  private static String longest(Matcher matcher) {
    List<String> blocks = new ArrayList<>();
    while (matcher.find()) {
      for (int group = 1; group <= matcher.groupCount(); group++) {
        if (matcher.group(group) != null && !matcher.group(group).isBlank()) {
          blocks.add(matcher.group(group));
        }
      }
    }
    return blocks.stream().reduce("", (a, b) -> b.length() > a.length() ? b : a);
  }
}
