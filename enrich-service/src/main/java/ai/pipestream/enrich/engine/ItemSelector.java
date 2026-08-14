package ai.pipestream.enrich.engine;

import ai.pipestream.document.v1.CodeItem;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.FormulaItem;
import ai.pipestream.document.v1.PageItem;
import ai.pipestream.document.v1.PictureClassificationData;
import ai.pipestream.document.v1.PictureItem;
import ai.pipestream.document.v1.ProvenanceItem;
import ai.pipestream.document.v1.Size;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.ItemImage;
import ai.pipestream.enrich.v1.ItemSkipped;
import ai.pipestream.enrich.v1.SkipReason;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Walks a Document for enrichable items, mirroring Docling's enrichment flags:
 * pictures above an area threshold for description, chart-classified pictures
 * for chart extraction, and text items already labelled code or formula. This
 * service never re-runs layout: labels from gRParse are the authority.
 */
public final class ItemSelector {

  /** The enrichment job a work item belongs to. */
  public enum Kind {
    DESCRIPTION,
    CHART,
    CODE,
    FORMULA
  }

  /** One item selected for a VLM call. */
  public record WorkItem(
      String selfRef,
      Kind kind,
      String model,
      String prompt,
      String imageDataUri,
      String text,
      int pictureIndex,
      int textIndex) {}

  /** The result of walking the document: work to run plus items skipped at
   * selection time (before any VLM call). */
  public record Selection(List<WorkItem> work, List<ItemSkipped> skips) {
    public long count(Kind kind) {
      return work.stream().filter(item -> item.kind == kind).count();
    }
  }

  private static final String DESCRIBE_PROMPT =
      "Describe this picture in detail.";
  private static final String CHART_PROMPT =
      "Extract the chart data from this image as a CSV table, with a header row.";
  private static final String CODE_PROMPT =
      "Transcribe and normalize the following code block. Reply with the code only.\n\n";
  private static final String FORMULA_PROMPT =
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
      String image = imageDataUri(picture, selfRef, crops);
      if (image == null) {
        skips.add(imageSkip(picture, selfRef));
        continue;
      }
      if (chartJob) {
        work.add(new WorkItem(selfRef, Kind.CHART, chartModel(options), CHART_PROMPT, image,
            null, i, -1));
      } else {
        work.add(new WorkItem(selfRef, Kind.DESCRIPTION, descriptionModel(options),
            DESCRIBE_PROMPT, image, null, i, -1));
      }
    }

    for (int i = 0; i < document.getTextsCount(); i++) {
      var baseText = document.getTexts(i);
      if (options.getDoCodeEnrichment() && baseText.hasCode()) {
        CodeItem code = baseText.getCode();
        String selfRef = selfRef(code.getSelfRef(), "#/texts/", i);
        work.add(new WorkItem(selfRef, Kind.CODE, codeFormulaModel(options),
            CODE_PROMPT + code.getText(), null, code.getText(), -1, i));
      } else if (options.getDoFormulaEnrichment() && baseText.hasFormula()) {
        FormulaItem formula = baseText.getFormula();
        String selfRef = selfRef(formula.getBase().getSelfRef(), "#/texts/", i);
        work.add(new WorkItem(selfRef, Kind.FORMULA, codeFormulaModel(options),
            FORMULA_PROMPT + formula.getBase().getText(), null, formula.getBase().getText(),
            -1, i));
      }
    }

    return new Selection(work, skips);
  }

  private static String selfRef(String declared, String prefix, int index) {
    return declared.isEmpty() ? prefix + index : declared;
  }

  /** A picture is a chart when its label says so or its top figure-class
   * prediction is a chart class; there is no layout re-run here. */
  private static boolean isChart(PictureItem picture) {
    if (picture.getLabel() == ai.pipestream.document.v1.DocItemLabel.DOC_ITEM_LABEL_CHART) {
      return true;
    }
    for (var annotation : picture.getAnnotationsList()) {
      if (annotation.hasClassification()) {
        PictureClassificationData classification = annotation.getClassification();
        if (classification.getPredictedClassesCount() > 0
            && classification
                .getPredictedClasses(0)
                .getClassName()
                .toLowerCase(java.util.Locale.ROOT)
                .contains("chart")) {
          return true;
        }
      }
    }
    return false;
  }

  /** Area ratio of the picture's first provenance box to its page. Pictures
   * without provenance or a known page size count as full-page (ratio 1). */
  private static boolean passesArea(
      PictureItem picture, Document document, EnrichOptions options) {
    double threshold = options.getPictureDescriptionAreaThreshold();
    if (threshold <= 0.0) {
      return true;
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

  /** Resolves image bytes for a picture: an ItemImage crop wins, then an
   * inline data URI on the item's ImageRef. Anything else is null (skipped). */
  private static String imageDataUri(
      PictureItem picture, String selfRef, Map<String, ItemImage> crops) {
    ItemImage crop = crops.get(selfRef);
    if (crop != null) {
      String mime = crop.getMimetype().isEmpty() ? "image/png" : crop.getMimetype();
      return "data:" + mime + ";base64," + Base64.getEncoder().encodeToString(
          crop.getData().toByteArray());
    }
    if (picture.hasImage() && picture.getImage().getUri().startsWith("data:")) {
      return picture.getImage().getUri();
    }
    return null;
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
