package ai.pipestream.enrich.engine;

import ai.pipestream.document.v1.CodeItem;
import ai.pipestream.document.v1.DocItemLabel;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.FormulaItem;
import ai.pipestream.document.v1.ImageRef;
import ai.pipestream.document.v1.PageItem;
import ai.pipestream.document.v1.PictureClassificationData;
import ai.pipestream.document.v1.PictureItem;
import ai.pipestream.document.v1.ProvenanceItem;
import ai.pipestream.document.v1.Size;
import ai.pipestream.enrich.v1.ChartExtractionOptions;
import ai.pipestream.enrich.v1.ChartOutput;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.ItemImage;
import ai.pipestream.enrich.v1.ItemSkipped;
import ai.pipestream.enrich.v1.SkipReason;
import ai.pipestream.enrich.vlm.VlmClient.VlmImage;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Walks a Document for enrichable items: pictures above an area threshold
 * for description, chart-classified pictures for chart extraction, and text
 * items already labelled code or formula. This service never re-runs layout:
 * labels from gRParse are the authority.
 *
 * <p>Selection rules: the area threshold defaults to 0.05 of the page area,
 * prompts are fixed per preset (SmolVLM / Granite Vision / chart2csv /
 * CodeFormula), and a picture is a chart when its top figure-class
 * prediction is one of the supported chart types (bar_chart, pie_chart,
 * line_chart) or the picture carries the label DOC_ITEM_LABEL_CHART.
 */
public final class ItemSelector {

  /** The enrichment job a work item belongs to. */
  public enum Kind {
    DESCRIPTION,
    CHART,
    CODE,
    FORMULA
  }

  /** One item selected for a VLM call. A chart picture yields one work item
   * per enabled chart output, each with its own prompt; {@code chartOutput}
   * names which (UNSPECIFIED for every other kind). {@code endpoint} is the
   * per-item endpoint override, or null for the request's endpoint.
   * {@code image} is null for a text-only call; a crop stays raw bytes here
   * and is encoded only when its own call is made. */
  public record WorkItem(
      String selfRef,
      Kind kind,
      String model,
      String prompt,
      VlmImage image,
      String text,
      int maxTokens,
      int pictureIndex,
      int textIndex,
      ChartOutput chartOutput,
      String endpoint) {

    WorkItem(String selfRef, Kind kind, String model, String prompt, VlmImage image,
        String text, int maxTokens, int pictureIndex, int textIndex) {
      this(selfRef, kind, model, prompt, image, text, maxTokens, pictureIndex, textIndex,
          ChartOutput.CHART_OUTPUT_UNSPECIFIED, null);
    }
  }

  /** The result of walking the document: work to run plus items skipped at
   * selection time (before any VLM call). */
  public record Selection(List<WorkItem> work, List<ItemSkipped> skips) {
    /** Items of {@code kind} selected. Charts count pictures, not calls: a
     * chart with three enabled outputs is one chart extraction. */
    public long count(Kind kind) {
      if (kind == Kind.CHART) {
        return work.stream()
            .filter(item -> item.kind == Kind.CHART)
            .mapToInt(WorkItem::pictureIndex)
            .distinct()
            .count();
      }
      return work.stream().filter(item -> item.kind == kind).count();
    }
  }

  /** Default picture-description area threshold (fraction of the page area). */
  static final double DEFAULT_AREA_THRESHOLD = 0.05;

  /** Generation caps: description 200, code/formula 2048. Chart gets 4096
   * because a wide table does not fit in the code/formula budget. */
  static final int MAX_TOKENS_DESCRIPTION = 200;
  static final int MAX_TOKENS_CODE_FORMULA = 2048;
  static final int MAX_TOKENS_CHART = 4096;

  /** Chart classes that trigger chart extraction (top prediction, lowercased,
   * exact match). */
  private static final Set<String> SUPPORTED_CHART_TYPES =
      Set.of("bar_chart", "pie_chart", "line_chart");

  // Fixed per-job prompts. Wire-visible model inputs; never reworded.
  private static final String DESCRIBE_PROMPT_SMOLVLM =
      "Describe this image in a few sentences.";
  private static final String DESCRIBE_PROMPT_GRANITE_VISION =
      "What is shown in this image?";
  /** The original chart prompt, used only when EnrichOptions.chart_extraction
   * is absent, so a v1 caller keeps getting exactly what it got. */
  private static final String CHART_PROMPT =
      "Convert the information in this chart into a data table in CSV format.";

  // The Docling chart stage's prompts (docling granite_vision.py): the
  // Granite Vision special tokens, and the natural-language equivalents
  // Docling substitutes when use_natural_language_prompts is set. Verbatim.
  static final String CHART2CSV_TOKEN = "<chart2csv>";
  static final String CHART2SUMMARY_TOKEN = "<chart2summary>";
  static final String CHART2CODE_TOKEN = "<chart2code>";
  static final String CHART2CSV_NATURAL =
      "Convert the information in this chart into a data table in CSV format "
          + "with a header row and numeric values.";
  static final String CHART2SUMMARY_NATURAL = "Describe this chart in a few sentences.";
  static final String CHART2CODE_NATURAL =
      "Write Python code using matplotlib that recreates this chart. "
          + "Return only a fenced ```python code block.";
  private static final String CODE_IMAGE_PROMPT = "<code>";
  private static final String FORMULA_IMAGE_PROMPT = "<formula>";

  // Text-only fallbacks for code/formula items with no image crop, so
  // enrichment still works when the caller cannot supply crops.
  private static final String CODE_TEXT_PROMPT =
      "Transcribe and normalize the following code block. Reply with the code only.\n\n";
  private static final String FORMULA_TEXT_PROMPT =
      "Transcribe the following formula to LaTeX. Reply with the formula only.\n\n";

  private ItemSelector() {}

  /** Selects the enrichable items on {@code document} under {@code options}. */
  public static Selection select(
      Document document, Map<String, ItemImage> crops, EnrichOptions options) {
    List<WorkItem> work = new ArrayList<>();
    List<ItemSkipped> skips = new ArrayList<>();

    for (int i = 0; i < document.getPicturesCount(); i++) {
      PictureItem picture = document.getPictures(i);
      String selfRef = selfRef(picture.getSelfRef(), "#/pictures/", i);
      boolean chartJob = options.getDoChartExtraction() && isChart(picture);
      boolean describeJob =
          !chartJob && options.getDoPictureDescription() && passesArea(picture, document, options);
      boolean areaSkipped =
          !chartJob
              && options.getDoPictureDescription()
              && !passesArea(picture, document, options);
      if (!chartJob && !describeJob) {
        if (areaSkipped) {
          skips.add(skip(selfRef, SkipReason.SKIP_REASON_BELOW_AREA_THRESHOLD,
              "picture area is below the configured threshold"));
        }
        continue;
      }
      VlmImage image = image(picture.getImage(), selfRef, crops);
      if (image == null) {
        skips.add(imageSkip(picture, selfRef));
        continue;
      }
      if (chartJob) {
        addChartWork(work, selfRef, image, i, options);
      } else {
        work.add(new WorkItem(selfRef, Kind.DESCRIPTION, descriptionModel(options),
            describePrompt(options), image, null, MAX_TOKENS_DESCRIPTION, i, -1));
      }
    }

    for (int i = 0; i < document.getTextsCount(); i++) {
      var baseText = document.getTexts(i);
      if (options.getDoCodeEnrichment() && baseText.hasCode()) {
        CodeItem code = baseText.getCode();
        String selfRef = selfRef(code.getSelfRef(), "#/texts/", i);
        VlmImage image = code.hasImage() ? image(code.getImage(), selfRef, crops)
            : crop(selfRef, crops);
        if (image != null) {
          work.add(new WorkItem(selfRef, Kind.CODE, codeFormulaModel(options), CODE_IMAGE_PROMPT,
              image, code.getText(), MAX_TOKENS_CODE_FORMULA, -1, i));
        } else {
          work.add(new WorkItem(selfRef, Kind.CODE, codeFormulaModel(options),
              CODE_TEXT_PROMPT + code.getText(), null, code.getText(), MAX_TOKENS_CODE_FORMULA,
              -1, i));
        }
      } else if (options.getDoFormulaEnrichment() && baseText.hasFormula()) {
        FormulaItem formula = baseText.getFormula();
        String selfRef = selfRef(formula.getBase().getSelfRef(), "#/texts/", i);
        VlmImage image = crop(selfRef, crops);
        if (image != null) {
          work.add(new WorkItem(selfRef, Kind.FORMULA, codeFormulaModel(options),
              FORMULA_IMAGE_PROMPT, image, formula.getBase().getText(), MAX_TOKENS_CODE_FORMULA,
              -1, i));
        } else {
          work.add(new WorkItem(selfRef, Kind.FORMULA, codeFormulaModel(options),
              FORMULA_TEXT_PROMPT + formula.getBase().getText(), null, formula.getBase().getText(),
              MAX_TOKENS_CODE_FORMULA, -1, i));
        }
      }
    }

    return new Selection(work, skips);
  }

  /** One work item per enabled chart output, in Docling's order (csv,
   * summary, code). Without ChartExtractionOptions: the original single CSV
   * call with its original prompt. */
  private static void addChartWork(
      List<WorkItem> work, String selfRef, VlmImage image, int pictureIndex,
      EnrichOptions options) {
    if (!options.hasChartExtraction()) {
      work.add(new WorkItem(selfRef, Kind.CHART, chartModel(options), CHART_PROMPT, image,
          null, MAX_TOKENS_CHART, pictureIndex, -1, ChartOutput.CHART_OUTPUT_CSV, null));
      return;
    }
    ChartExtractionOptions chart = options.getChartExtraction();
    String model = chart.getModel().isEmpty() ? chartModel(options) : chart.getModel();
    String endpoint = chart.getVlmEndpoint().isEmpty() ? null : chart.getVlmEndpoint();
    boolean natural = chart.getNaturalLanguagePrompts();
    for (ChartOutput output : enabledChartOutputs(chart)) {
      work.add(new WorkItem(selfRef, Kind.CHART, model, chartPrompt(output, natural), image,
          null, MAX_TOKENS_CHART, pictureIndex, -1, output, endpoint));
    }
  }

  /** The chart outputs {@code chart} enables, in Docling's order. csv is on
   * unless explicitly set false. */
  public static List<ChartOutput> enabledChartOutputs(ChartExtractionOptions chart) {
    List<ChartOutput> outputs = new ArrayList<>(3);
    if (!chart.hasCsv() || chart.getCsv()) {
      outputs.add(ChartOutput.CHART_OUTPUT_CSV);
    }
    if (chart.getSummary()) {
      outputs.add(ChartOutput.CHART_OUTPUT_SUMMARY);
    }
    if (chart.getCode()) {
      outputs.add(ChartOutput.CHART_OUTPUT_CODE);
    }
    return outputs;
  }

  /** The wire prompt for one chart output: the special token, or Docling's
   * natural-language equivalent. */
  static String chartPrompt(ChartOutput output, boolean natural) {
    return switch (output) {
      case CHART_OUTPUT_SUMMARY -> natural ? CHART2SUMMARY_NATURAL : CHART2SUMMARY_TOKEN;
      case CHART_OUTPUT_CODE -> natural ? CHART2CODE_NATURAL : CHART2CODE_TOKEN;
      default -> natural ? CHART2CSV_NATURAL : CHART2CSV_TOKEN;
    };
  }

  private static String selfRef(String declared, String prefix, int index) {
    return declared.isEmpty() ? prefix + index : declared;
  }

  /** A picture is a chart when its label says so or its top figure-class
   * prediction is one of the supported chart types; there is no layout
   * re-run here. */
  private static boolean isChart(PictureItem picture) {
    if (picture.getLabel() == DocItemLabel.DOC_ITEM_LABEL_CHART) {
      return true;
    }
    for (var annotation : picture.getAnnotationsList()) {
      if (annotation.hasClassification()) {
        PictureClassificationData classification = annotation.getClassification();
        if (classification.getPredictedClassesCount() > 0
            && SUPPORTED_CHART_TYPES.contains(classification
                .getPredictedClasses(0)
                .getClassName()
                .toLowerCase(Locale.ROOT))) {
          return true;
        }
      }
    }
    return false;
  }

  /** Area ratio of the picture's first provenance box to its page. Pictures
   * without provenance or a known page size count as full-page (ratio 1).
   * An unset (zero) or NaN threshold means the default 0.05; a negative
   * value disables the threshold entirely. */
  private static boolean passesArea(
      PictureItem picture, Document document, EnrichOptions options) {
    double threshold = options.getPictureDescriptionAreaThreshold();
    if (threshold < 0.0) {
      return true;
    }
    if (threshold == 0.0 || Double.isNaN(threshold)) {
      // NaN would make every comparison false and silently skip everything.
      threshold = DEFAULT_AREA_THRESHOLD;
    }
    if (picture.getProvCount() == 0) {
      return true;
    }
    ProvenanceItem prov = picture.getProv(0);
    PageItem page = document.getPagesMap().get(prov.getPageNo());
    if (page == null) {
      return true;
    }
    Size pageSize = page.getSize();
    double pageArea = pageSize.getWidth() * pageSize.getHeight();
    if (pageArea <= 0.0) {
      return true;
    }
    var bbox = prov.getBbox();
    double area = Math.abs(bbox.getR() - bbox.getL()) * Math.abs(bbox.getB() - bbox.getT());
    return area / pageArea >= threshold;
  }

  /** Resolves image bytes for an item: an ItemImage crop wins, then an inline
   * data URI on the item's ImageRef. Anything else is null (skipped). */
  private static VlmImage image(
      ImageRef imageRef, String selfRef, Map<String, ItemImage> crops) {
    VlmImage crop = crop(selfRef, crops);
    if (crop != null) {
      return crop;
    }
    if (imageRef != null && imageRef.getUri().startsWith("data:")) {
      return new VlmImage.DataUri(imageRef.getUri());
    }
    return null;
  }

  /** The ItemImage crop for a self_ref, or null when absent. The bytes are
   * referenced, not copied or encoded: base64 happens per call, so a large
   * document's crops are never all held encoded at once. */
  private static VlmImage crop(String selfRef, Map<String, ItemImage> crops) {
    ItemImage crop = crops.get(selfRef);
    if (crop == null) {
      return null;
    }
    return new VlmImage.Bytes(crop.getMimetype(), crop.getData());
  }

  private static ItemSkipped imageSkip(PictureItem picture, String selfRef) {
    if (picture.hasImage() && !picture.getImage().getUri().isEmpty()) {
      return skip(selfRef, SkipReason.SKIP_REASON_UNFETCHABLE_URI,
          "image is a non-data URI and outbound fetching is disabled: "
              + picture.getImage().getUri());
    }
    return skip(selfRef, SkipReason.SKIP_REASON_NO_IMAGE,
        "picture has no image bytes and no ItemImage crop was supplied");
  }

  private static ItemSkipped skip(String selfRef, SkipReason reason, String detail) {
    return ItemSkipped.newBuilder()
        .setSelfRef(selfRef)
        .setReason(reason)
        .setDetail(detail)
        .build();
  }

  /** The per-preset description prompt; the SmolVLM prompt is the default
   * for any other (raw) model. */
  private static String describePrompt(EnrichOptions options) {
    return switch (options.getPictureDescriptionPreset()) {
      case PICTURE_DESCRIPTION_PRESET_GRANITE_VISION -> DESCRIBE_PROMPT_GRANITE_VISION;
      default -> DESCRIBE_PROMPT_SMOLVLM;
    };
  }

  private static String descriptionModel(EnrichOptions options) {
    return switch (options.getPictureDescriptionPreset()) {
      case PICTURE_DESCRIPTION_PRESET_SMOLVLM -> "smolvlm";
      case PICTURE_DESCRIPTION_PRESET_GRANITE_VISION -> "granite-vision";
      default -> options.getPictureDescriptionPresetRaw();
    };
  }

  private static String chartModel(EnrichOptions options) {
    return switch (options.getChartPreset()) {
      case CHART_PRESET_GRANITE_VISION_CHART2CSV -> "granite-vision-chart2csv";
      default -> options.getChartPresetRaw();
    };
  }

  private static String codeFormulaModel(EnrichOptions options) {
    return switch (options.getCodeFormulaPreset()) {
      case CODE_FORMULA_PRESET_CODE_FORMULA_V2 -> "code-formula-v2";
      default -> options.getCodeFormulaPresetRaw();
    };
  }
}
