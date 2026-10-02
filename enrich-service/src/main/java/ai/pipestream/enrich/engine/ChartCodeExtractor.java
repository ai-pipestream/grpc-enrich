package ai.pipestream.enrich.engine;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads the code out of a chart2code reply the way Docling does
 * ({@code _extract_python_code} in the granite_vision chart stage): the body
 * of the first fenced {@code ```python} block, stripped. A reply without one
 * yields nothing; Docling then leaves meta.code unset, and this service
 * reports the item as skipped instead of emitting empty code.
 */
public final class ChartCodeExtractor {

  private static final Pattern PYTHON_FENCE =
      Pattern.compile("```python\\s*\\n(.*?)\\n```", Pattern.DOTALL);

  private ChartCodeExtractor() {}

  /** The fenced python block's body, stripped; null when the reply has none. */
  public static String extractPython(String reply) {
    Matcher fenced = PYTHON_FENCE.matcher(reply);
    return fenced.find() ? fenced.group(1).strip() : null;
  }
}
