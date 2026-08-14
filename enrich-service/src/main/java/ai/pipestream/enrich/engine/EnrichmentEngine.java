package ai.pipestream.enrich.engine;

import ai.pipestream.document.v1.BaseTextItem;
import ai.pipestream.document.v1.CodeItem;
import ai.pipestream.document.v1.DescriptionAnnotation;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.PictureAnnotation;
import ai.pipestream.document.v1.PictureItem;
import ai.pipestream.document.v1.PictureTabularChartData;
import ai.pipestream.enrich.engine.ItemSelector.Kind;
import ai.pipestream.enrich.engine.ItemSelector.Selection;
import ai.pipestream.enrich.engine.ItemSelector.WorkItem;
import ai.pipestream.enrich.v1.EnrichComplete;
import ai.pipestream.enrich.v1.EnrichDocumentResponse;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.EnrichStarted;
import ai.pipestream.enrich.v1.ItemAnnotation;
import ai.pipestream.enrich.v1.ItemImage;
import ai.pipestream.enrich.v1.ItemSkipped;
import ai.pipestream.enrich.v1.SkipReason;
import ai.pipestream.enrich.vlm.VlmClient;
import ai.pipestream.enrich.vlm.VlmClient.VlmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Runs one document's enrichment: selects items, emits EnrichStarted, then
 * one ItemAnnotation or ItemSkipped per item as that item's VLM call returns,
 * then the EnrichComplete trailer. Per-item events are emitted the moment
 * they exist, never buffered into a batch; out-of-order across items is
 * legal. A failed VLM call skips its item and never fails the RPC.
 */
public final class EnrichmentEngine {

  private final VlmClient.Factory clientFactory;
  private final String defaultEndpoint;
  private final int defaultConcurrency;
  private final int maxConcurrency;
  private final Duration defaultTimeout;
  private final ExecutorService executor;

  public EnrichmentEngine(
      VlmClient.Factory clientFactory,
      String defaultEndpoint,
      int defaultConcurrency,
      int maxConcurrency,
      Duration defaultTimeout,
      ExecutorService executor) {
    this.clientFactory = clientFactory;
    this.defaultEndpoint = defaultEndpoint;
    this.defaultConcurrency = defaultConcurrency;
    this.maxConcurrency = maxConcurrency;
    this.defaultTimeout = defaultTimeout;
    this.executor = executor;
  }

  /**
   * Enriches {@code document}, pushing every event to {@code emit} as it is
   * produced. {@code emit} must be thread-safe: item events arrive from
   * worker threads. Returns after the EnrichComplete trailer has been
   * emitted.
   */
  public void enrich(
      Document document,
      Map<String, ItemImage> crops,
      EnrichOptions options,
      Consumer<EnrichDocumentResponse> emit) {
    Selection selection = ItemSelector.select(document, crops, options);
    emit.accept(event(EnrichStarted.newBuilder()
        .setPictureDescriptions((int) selection.count(Kind.DESCRIPTION))
        .setChartExtractions((int) selection.count(Kind.CHART))
        .setCodeEnrichments((int) selection.count(Kind.CODE))
        .setFormulaEnrichments((int) selection.count(Kind.FORMULA))
        .build()));

    AtomicInteger succeeded = new AtomicInteger();
    AtomicInteger skipped = new AtomicInteger();
    AtomicInteger failed = new AtomicInteger();
    ConcurrentLinkedQueue<WorkItem> patches = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<ItemAnnotation> annotations = new ConcurrentLinkedQueue<>();

    for (ItemSkipped preskip : selection.skips()) {
      skipped.incrementAndGet();
      emit.accept(EnrichDocumentResponse.newBuilder().setSkipped(preskip).build());
    }

    String endpoint = options.getVlmEndpoint().isEmpty()
        ? defaultEndpoint
        : options.getVlmEndpoint();
    int concurrency = options.getConcurrency() == 0
        ? defaultConcurrency
        : Math.min(options.getConcurrency(), maxConcurrency);
    Duration timeout = options.getTimeoutSeconds() == 0
        ? defaultTimeout
        : Duration.ofSeconds(options.getTimeoutSeconds());

    if (!selection.work().isEmpty() && endpoint.isEmpty()) {
      for (WorkItem item : selection.work()) {
        skipped.incrementAndGet();
        emit.accept(skippedEvent(item.selfRef(), SkipReason.SKIP_REASON_VLM_ERROR,
            "no VLM endpoint configured (set ENRICH_VLM_URL or EnrichOptions.vlm_endpoint)"));
      }
    } else {
      VlmClient client = clientFactory.create(endpoint);
      Semaphore slots = new Semaphore(Math.max(1, concurrency));
      CountDownLatch done = new CountDownLatch(selection.work().size());
      for (WorkItem item : selection.work()) {
        executor.execute(() -> {
          try {
            slots.acquire();
            try {
              runItem(client, item, timeout, emit, succeeded, skipped, failed, patches,
                  annotations);
            } finally {
              slots.release();
            }
          } catch (InterruptedException interrupt) {
            Thread.currentThread().interrupt();
            failed.incrementAndGet();
          } finally {
            done.countDown();
          }
        });
      }
      try {
        done.await();
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
      }
    }

    EnrichComplete.Builder complete = EnrichComplete.newBuilder()
        .setSucceeded(succeeded.get())
        .setSkipped(skipped.get())
        .setFailed(failed.get());
    if (options.getReturnDocument()) {
      complete.setDocument(applyPatches(document, selection.work(), annotations));
    }
    emit.accept(event(complete.build()));
  }

  private static void runItem(
      VlmClient client,
      WorkItem item,
      Duration timeout,
      Consumer<EnrichDocumentResponse> emit,
      AtomicInteger succeeded,
      AtomicInteger skipped,
      AtomicInteger failed,
      ConcurrentLinkedQueue<WorkItem> order,
      ConcurrentLinkedQueue<ItemAnnotation> annotations) {
    try {
      String content =
          client.complete(item.model(), item.prompt(), item.imageDataUri(), item.maxTokens(),
              timeout);
      ItemAnnotation.Builder annotation = ItemAnnotation.newBuilder()
          .setSelfRef(item.selfRef())
          .setModel(item.model());
      switch (item.kind()) {
        case DESCRIPTION -> annotation.getDescriptionBuilder().setText(content);
        case CHART -> annotation.getChartTableBuilder()
            .setTable(ChartCsvParser.parse(content))
            .setCsv(content);
        case CODE -> {
          CodeFormulaPostProcessor.CodeResult code =
              CodeFormulaPostProcessor.processCode(content);
          annotation.getCodeBuilder()
              .setText(code.text())
              .setLanguage(code.language())
              .setLanguageRaw(code.languageRaw());
        }
        case FORMULA -> annotation.getFormulaBuilder()
            .setText(CodeFormulaPostProcessor.processFormula(content));
      }
      ItemAnnotation built = annotation.build();
      succeeded.incrementAndGet();
      order.add(item);
      annotations.add(built);
      emit.accept(EnrichDocumentResponse.newBuilder().setAnnotation(built).build());
    } catch (VlmException vlm) {
      skipped.incrementAndGet();
      emit.accept(skippedEvent(item.selfRef(), SkipReason.SKIP_REASON_VLM_ERROR,
          vlm.getMessage()));
    } catch (RuntimeException unexpected) {
      failed.incrementAndGet();
      emit.accept(skippedEvent(item.selfRef(), SkipReason.SKIP_REASON_UNSPECIFIED,
          "unexpected failure enriching this item: " + unexpected));
    }
  }

  /** Applies the emitted annotations to a copy of the document. Additive
   * only: provenance boxes and orig text are never rewritten. */
  private static Document applyPatches(
      Document document, List<WorkItem> work, ConcurrentLinkedQueue<ItemAnnotation> annotations) {
    Map<String, ItemAnnotation> byRef = new java.util.HashMap<>();
    annotations.forEach(annotation -> byRef.put(annotation.getSelfRef(), annotation));
    Document.Builder patched = document.toBuilder();
    for (WorkItem item : work) {
      ItemAnnotation annotation = byRef.get(item.selfRef());
      if (annotation == null) {
        continue;
      }
      switch (item.kind()) {
        case DESCRIPTION -> {
          PictureItem picture = patched.getPictures(item.pictureIndex());
          patched.setPictures(item.pictureIndex(), picture.toBuilder()
              .addAnnotations(PictureAnnotation.newBuilder()
                  .setDescription(DescriptionAnnotation.newBuilder()
                      .setKind("description")
                      .setText(annotation.getDescription().getText())
                      .setProvenance(annotation.getModel())))
              .build());
        }
        case CHART -> {
          PictureItem picture = patched.getPictures(item.pictureIndex());
          patched.setPictures(item.pictureIndex(), picture.toBuilder()
              .addAnnotations(PictureAnnotation.newBuilder()
                  .setTabularChart(PictureTabularChartData.newBuilder()
                      .setKind("tabular_chart")
                      .setTitle(annotation.getChartTable().getTitle())
                      .setChartData(annotation.getChartTable().getTable())))
              .build());
        }
        case CODE -> {
          BaseTextItem text = patched.getTexts(item.textIndex());
          CodeItem.Builder code = text.getCode().toBuilder()
              .setText(annotation.getCode().getText())
              .setCodeLanguage(annotation.getCode().getLanguage());
          if (!annotation.getCode().getLanguageRaw().isEmpty()) {
            code.setCodeLanguageRaw(annotation.getCode().getLanguageRaw());
          }
          patched.setTexts(item.textIndex(), text.toBuilder().setCode(code).build());
        }
        case FORMULA -> {
          BaseTextItem text = patched.getTexts(item.textIndex());
          patched.setTexts(item.textIndex(), text.toBuilder()
              .setFormula(text.getFormula().toBuilder()
                  .setBase(text.getFormula().getBase().toBuilder()
                      .setText(annotation.getFormula().getText())))
              .build());
        }
      }
    }
    return patched.build();
  }

  private static EnrichDocumentResponse event(EnrichStarted started) {
    return EnrichDocumentResponse.newBuilder().setStarted(started).build();
  }

  private static EnrichDocumentResponse event(EnrichComplete complete) {
    return EnrichDocumentResponse.newBuilder().setComplete(complete).build();
  }

  private static EnrichDocumentResponse skippedEvent(
      String selfRef, SkipReason reason, String detail) {
    return EnrichDocumentResponse.newBuilder()
        .setSkipped(ItemSkipped.newBuilder()
            .setSelfRef(selfRef)
            .setReason(reason)
            .setDetail(detail == null ? "" : detail))
        .build();
  }
}
