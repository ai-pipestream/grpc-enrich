package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.enrich.engine.FormulaPrompts;
import ai.pipestream.enrich.engine.FormulaReply;
import ai.pipestream.enrich.v1.CodeFormulaPreset;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.FormulaPrompt;
import org.junit.jupiter.api.Test;

/** Formula prompt choice by model, the prompt text, and the reply kept. */
class FormulaPromptsTest {

  private static final EnrichOptions DEFAULTS = EnrichOptions.newBuilder()
      .setDoFormulaEnrichment(true)
      .setCodeFormulaPreset(CodeFormulaPreset.CODE_FORMULA_PRESET_CODE_FORMULA_V2)
      .build();

  @Test
  void unknownOrCodeFormulaModelKeepsTheToken() {
    assertThat(FormulaPrompts.resolve(DEFAULTS, "")).isEqualTo(DEFAULTS);
    assertThat(FormulaPrompts.resolve(DEFAULTS, "ds4sd/CodeFormulaV2")).isEqualTo(DEFAULTS);
    assertThat(FormulaPrompts.resolve(DEFAULTS, "ibm-granite/granite-docling-258M")).isEqualTo(DEFAULTS);
    assertThat(FormulaPrompts.imagePrompt(DEFAULTS, "x = 1")).isEqualTo(FormulaPrompts.CODEFORMULA_TOKEN);
  }

  @Test
  void generalVlmBehindTheOperatorEndpointGetsTheHintedLatexPrompt() {
    EnrichOptions resolved = FormulaPrompts.resolve(DEFAULTS, "Qwen2.5-VL-7B-Instruct");
    assertThat(resolved.getFormulaPrompt()).isEqualTo(FormulaPrompt.FORMULA_PROMPT_LATEX_WITH_TEXT_HINT);
    String prompt = FormulaPrompts.imagePrompt(resolved, " ℒd = ℒr + 𝛽𝑐ℒc , (9) ");
    assertThat(prompt).startsWith("Convert this formula image to LaTeX. Output only the LaTeX.")
        .contains("\nℒd = ℒr + 𝛽𝑐ℒc , (9)\n")
        .contains("Trust the image for the structure");
  }

  @Test
  void aRequestEndpointIsJudgedByTheRequestModel() {
    EnrichOptions ownEndpoint = DEFAULTS.toBuilder().setVlmEndpoint("http://other:8080").build();
    // Preset CodeFormula V2 on the caller's endpoint: the token, whatever the operator serves.
    assertThat(FormulaPrompts.resolve(ownEndpoint, "Qwen2.5-VL-7B-Instruct")).isEqualTo(ownEndpoint);
    EnrichOptions rawVlm = ownEndpoint.toBuilder()
        .setCodeFormulaPreset(CodeFormulaPreset.CODE_FORMULA_PRESET_UNSPECIFIED)
        .setCodeFormulaPresetRaw("qwen2.5-vl")
        .build();
    assertThat(FormulaPrompts.resolve(rawVlm, "").getFormulaPrompt())
        .isEqualTo(FormulaPrompt.FORMULA_PROMPT_LATEX_WITH_TEXT_HINT);
  }

  @Test
  void anExplicitStyleWins() {
    EnrichOptions pinned = DEFAULTS.toBuilder()
        .setFormulaPrompt(FormulaPrompt.FORMULA_PROMPT_CODEFORMULA_TOKEN).build();
    assertThat(FormulaPrompts.resolve(pinned, "Qwen2.5-VL-7B-Instruct")).isEqualTo(pinned);
    EnrichOptions bare = DEFAULTS.toBuilder().setFormulaPrompt(FormulaPrompt.FORMULA_PROMPT_LATEX).build();
    assertThat(FormulaPrompts.imagePrompt(bare, "x")).isEqualTo(
        "Convert this formula image to LaTeX. Output only the LaTeX.");
  }

  @Test
  void hintedPromptWithoutTextFallsBackToTheBareLatexPrompt() {
    EnrichOptions hinted = DEFAULTS.toBuilder()
        .setFormulaPrompt(FormulaPrompt.FORMULA_PROMPT_LATEX_WITH_TEXT_HINT).build();
    assertThat(FormulaPrompts.imagePrompt(hinted, "  ")).isEqualTo(
        "Convert this formula image to LaTeX. Output only the LaTeX.");
  }

  @Test
  void tokenRepliesAreProcessedAsBefore() {
    assertThat(FormulaReply.text(FormulaPrompts.CODEFORMULA_TOKEN, "  \\[ x^2 \\]</formula>"))
        .isEqualTo("\\[ x^2 \\]");
  }

  @Test
  void latexRepliesLoseFencesShellsAndProse() {
    String prompt = "Convert this formula image to LaTeX. Output only the LaTeX.";
    assertThat(FormulaReply.text(prompt, "```latex\nE = mc^2\n```")).isEqualTo("E = mc^2");
    assertThat(FormulaReply.text(prompt,
        "\\documentclass{article}\n\\usepackage{amsmath}\n\\begin{document}\n\\[ a = b \\]\n\\end{document}"))
        .isEqualTo("a = b");
    assertThat(FormulaReply.text(prompt,
        "The formula you've provided is:\n\\[ \\mathcal{L}_d = \\mathcal{L}_r \\]\nwhere \\( d \\) is ..."))
        .isEqualTo("\\mathcal{L}_d = \\mathcal{L}_r");
    assertThat(FormulaReply.text(prompt, "The value is \\( x_{t} + 1 \\) here.")).isEqualTo("x_{t} + 1");
    assertThat(FormulaReply.text(prompt, "  \\hat{y} = f_\\theta(X, e_T),  ")).isEqualTo("\\hat{y} = f_\\theta(X, e_T),");
  }
}
