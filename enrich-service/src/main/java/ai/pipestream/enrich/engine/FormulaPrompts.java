package ai.pipestream.enrich.engine;

import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.FormulaPrompt;
import java.util.List;
import java.util.Locale;

/**
 * The prompt a formula image is sent with.
 *
 * <p>The CodeFormula models answer the bare {@code <formula>} token with
 * LaTeX; a general VLM does not know the token and answers it with prose. So
 * an unset {@link EnrichOptions#getFormulaPrompt()} keeps the token for a
 * CodeFormula model (or when the model is unknown) and asks a general VLM for
 * the LaTeX, with the item's PDF text layer as a hint for its symbols and
 * subscripts. On 168 formulas checked against arXiv sources the hinted prompt
 * cut the median token distance from 0.43 to 0.07 with Qwen2.5-VL-7B.
 */
public final class FormulaPrompts {

  /** Docling's CodeFormula prompt: the bare token. */
  public static final String CODEFORMULA_TOKEN = "<formula>";

  static final String LATEX = "Convert this formula image to LaTeX. Output only the LaTeX.";

  static final String TEXT_HINT = "\n\nThe PDF text layer of this formula, in reading order (structure is"
      + " lost, and symbols may be out of order):\n%s\n\nUse it for the exact symbols, letters, subscripts"
      + " and digits. Trust the image for the structure: fractions, sums, scripts, brackets and alignment.";

  /** Name fragments of the models trained on the token prompt, compared without case or punctuation. */
  private static final List<String> CODEFORMULA_MODELS =
      List.of("codeformula", "granitedocling", "smoldocling");

  private FormulaPrompts() {}

  /**
   * {@code options} with the formula prompt style decided: as given when set;
   * otherwise the hinted LaTeX prompt when the model is a general VLM, and
   * unchanged (the token) when it is a CodeFormula model or unknown. The
   * model is the request's own when it names a {@code vlm_endpoint}, else
   * {@code servedModel}, what the operator's endpoint serves (ENRICH_VLM_MODEL).
   */
  public static EnrichOptions resolve(EnrichOptions options, String servedModel) {
    if (options.getFormulaPrompt() != FormulaPrompt.FORMULA_PROMPT_UNSPECIFIED) {
      return options;
    }
    String model = options.getVlmEndpoint().isEmpty() ? servedModel : requestModel(options);
    if (model == null || model.isBlank() || isCodeFormulaModel(model)) {
      return options;
    }
    return options.toBuilder().setFormulaPrompt(FormulaPrompt.FORMULA_PROMPT_LATEX_WITH_TEXT_HINT).build();
  }

  /** The prompt for one formula image whose text layer reads {@code text}. */
  public static String imagePrompt(EnrichOptions options, String text) {
    return switch (options.getFormulaPrompt()) {
      case FORMULA_PROMPT_LATEX -> LATEX;
      case FORMULA_PROMPT_LATEX_WITH_TEXT_HINT ->
          text == null || text.isBlank() ? LATEX : LATEX + TEXT_HINT.formatted(text.strip());
      default -> CODEFORMULA_TOKEN;
    };
  }

  /** Whether {@code model} names a model trained on the token prompt. */
  static boolean isCodeFormulaModel(String model) {
    String squashed = model.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
    return CODEFORMULA_MODELS.stream().anyMatch(squashed::contains);
  }

  private static String requestModel(EnrichOptions options) {
    return switch (options.getCodeFormulaPreset()) {
      case CODE_FORMULA_PRESET_CODE_FORMULA_V2 -> "code-formula-v2";
      case CODE_FORMULA_PRESET_GRANITE_DOCLING -> "granite-docling";
      default -> options.getCodeFormulaPresetRaw();
    };
  }
}
