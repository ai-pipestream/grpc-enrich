package ai.pipestream.enrich.engine;

import ai.pipestream.document.v1.BaseTextItem;
import ai.pipestream.document.v1.CodeItem;
import ai.pipestream.document.v1.CodeLanguageLabel;
import ai.pipestream.document.v1.CodeMetaField;
import ai.pipestream.document.v1.DescriptionMetaField;
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
import ai.pipestream.enrich.vlm.VlmClient.Header;
import ai.pipestream.enrich.vlm.VlmClient.VlmException;
import ai.pipestream.enrich.vlm.VlmClient.VlmRequest;
import ai.pipestream.enrich.vlm.VlmEndpoint;
import io.grpc.Status;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
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
 *
 * <p><b>Endpoints and credentials.</b> {@link EndpointPolicy} decides whether
 * a request may name its own endpoint, and the operator's key is attached
 * only to the operator's endpoint. A failure on a caller-named endpoint is
 * reported by its safe message, never with bytes that endpoint sent.
 */
public final class EnrichmentEngine {

  private final VlmClient.Factory clientFactory;
  private final EndpointPolicy endpoints;
  private final int defaultConcurrency;
  private final int maxConcurrency;
  private final Duration defaultTimeout;
  private final ExecutorService executor;

  /** An engine whose only endpoint is {@code defaultEndpoint}: no key, and
   * per-request endpoints refused. */
  public EnrichmentEngine(
      VlmClient.Factory clientFactory,
      String defaultEndpoint,
      int defaultConcurrency,
      int maxConcurrency,
      Duration defaultTimeout,
      ExecutorService executor) {
    this(clientFactory, EndpointPolicy.defaultOnly(defaultEndpoint), defaultConcurrency,
        maxConcurrency, defaultTimeout, executor);
  }

  /**
   * An engine that reaches the endpoints {@code endpoints} allows.
   *
   * @param endpoints which endpoints requests may reach, and the operator's key
   */
  public EnrichmentEngine(
      VlmClient.Factory clientFactory,
      EndpointPolicy endpoints,
      int defaultConcurrency,
      int maxConcurrency,
      Duration defaultTimeout,
      ExecutorService executor) {
    this.clientFactory = clientFactory;
    this.endpoints = endpoints;
    this.defaultConcurrency = defaultConcurrency;
    this.maxConcurrency = maxConcurrency;
    this.defaultTimeout = defaultTimeout;
    this.executor = executor;
  }

  /**
   * Checks {@code options} against this server's endpoint policy before any
   * work starts. Returns {@link Status#OK}, PERMISSION_DENIED for a
   * per-request endpoint the operator has not allowed, or INVALID_ARGUMENT
   * for one that is not a usable URL.
   */
  public Status validate(EnrichOptions options) {
    for (String endpoint : requestEndpoints(options)) {
      try {
        VlmEndpoint.completionsUri(endpoint);
      } catch (IllegalArgumentException unusable) {
        return Status.INVALID_ARGUMENT.withDescription(
            "per-request VLM endpoint is unusable: " + unusable.getMessage());
      }
      if (!endpoints.allowsRequestEndpoint(endpoint)) {
        return Status.PERMISSION_DENIED.withDescription(
            "this server does not accept a per-request VLM endpoint for "
                + VlmEndpoint.origin(endpoint) + " (vlm_endpoint or chart_extraction.vlm_endpoint);"
                + " its operator allows them with ENRICH_ALLOW_REQUEST_ENDPOINT or"
                + " ENRICH_VLM_ENDPOINT_ALLOWLIST");
      }
    }
    return Status.OK;
  }

  /** The non-empty endpoints a request names for its own calls. */
  private static List<String> requestEndpoints(EnrichOptions options) {
    List<String> requested = new ArrayList<>(2);
    if (!options.getVlmEndpoint().isEmpty()) {
      requested.add(options.getVlmEndpoint());
    }
    if (options.hasChartExtraction() && !options.getChartExtraction().getVlmEndpoint().isEmpty()) {
      requested.add(options.getChartExtraction().getVlmEndpoint());
    }
    return requested;
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
    ConcurrentLinkedQueue<EnrichedItem> enriched = new ConcurrentLinkedQueue<>();

    for (ItemSkipped preskip : selection.skips()) {
      skipped.incrementAndGet();
      emit.accept(EnrichDocumentResponse.newBuilder().setSkipped(preskip).build());
    }

    // Both fields are uint32 on the wire; Java surfaces values above 2^31 as
    // negative ints. A wrapped concurrency is above the cap, so clamp to it;
    // a wrapped timeout must be widened back to its unsigned value or the
    // negative Duration would fail every VLM call.
    int requestedConcurrency = options.getConcurrency();
    int concurrency = requestedConcurrency == 0
        ? defaultConcurrency
        : requestedConcurrency < 0
            ? maxConcurrency
            : Math.min(requestedConcurrency, maxConcurrency);
    Duration timeout = options.getTimeoutSeconds() == 0
        ? defaultTimeout
        : Duration.ofSeconds(Integer.toUnsignedLong(options.getTimeoutSeconds()));

    List<Header> operatorHeaders = endpoints.defaultApiKey().isEmpty()
        ? List.of()
        : List.of(new Header("Authorization", "Bearer " + endpoints.defaultApiKey()));

    // A work item may name its own endpoint (chart calls routed to a chart
    // model); every other item uses the request's, then the operator's. One
    // client per endpoint.
    List<Call> runnable = new ArrayList<>();
    Map<String, VlmClient> clients = new HashMap<>();
    for (WorkItem item : selection.work()) {
      String requested = item.endpoint() != null ? item.endpoint() : options.getVlmEndpoint();
      boolean callerChosen = !requested.isEmpty();
      String endpoint = callerChosen ? requested : endpoints.defaultEndpoint();
      String refusal = null;
      VlmClient client = null;
      if (endpoint.isEmpty()) {
        refusal = "no VLM endpoint configured (set ENRICH_VLM_URL or EnrichOptions.vlm_endpoint)";
      } else if (callerChosen && !endpoints.allowsRequestEndpoint(endpoint)) {
        refusal = "per-request VLM endpoints are not allowed on this server";
      } else {
        try {
          client = clients.computeIfAbsent(endpoint, clientFactory::create);
        } catch (IllegalArgumentException unusable) {
          refusal = "VLM endpoint is unusable: " + unusable.getMessage();
        }
      }
      if (refusal != null) {
        skipped.incrementAndGet();
        emit.accept(skippedEvent(item, SkipReason.SKIP_REASON_VLM_ERROR, refusal));
      } else {
        // Only the operator's endpoint gets the operator's key.
        runnable.add(new Call(item, client, callerChosen,
            callerChosen ? List.of() : operatorHeaders));
      }
    }
    if (!runnable.isEmpty()) {
      Semaphore slots = new Semaphore(Math.max(1, concurrency));
      CountDownLatch done = new CountDownLatch(runnable.size());
      for (Call call : runnable) {
        executor.execute(() -> {
          try {
            slots.acquire();
            try {
              runItem(call, timeout, emit, succeeded, skipped, failed, enriched);
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
      complete.setDocument(applyPatches(document, enriched));
    }
    emit.accept(event(complete.build()));
  }

  /** A work item bound to its endpoint's client and the headers that go
   * with it. {@code callerChosen} marks an endpoint the request named:
   * its failures are reported without anything it sent. */
  private record Call(WorkItem item, VlmClient client, boolean callerChosen,
      List<Header> headers) {}

  /** A work item paired with the annotation its own VLM call produced. The
   * pairing (not the self_ref) keys patch application, so two items sharing
   * a self_ref still each get their own annotation. */
  private record EnrichedItem(WorkItem item, ItemAnnotation annotation) {}

  private static void runItem(
      Call call,
      Duration timeout,
      Consumer<EnrichDocumentResponse> emit,
      AtomicInteger succeeded,
      AtomicInteger skipped,
      AtomicInteger failed,
      ConcurrentLinkedQueue<EnrichedItem> enriched) {
    WorkItem item = call.item();
    try {
      String content = call.client().complete(new VlmRequest(item.model(), item.prompt(),
          item.image(), item.maxTokens(), OptionalDouble.empty(), OptionalDouble.empty(),
          OptionalLong.empty(), call.headers(), timeout));
      ItemAnnotation.Builder annotation = ItemAnnotation.newBuilder()
          .setSelfRef(item.selfRef())
          .setModel(item.model());
      switch (item.kind()) {
        case DESCRIPTION -> annotation.getDescriptionBuilder().setText(content);
        case CHART -> chartAnnotation(item, content, annotation);
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
      enriched.add(new EnrichedItem(item, built));
      emit.accept(EnrichDocumentResponse.newBuilder().setAnnotation(built).build());
    } catch (VlmException vlm) {
      skipped.incrementAndGet();
      emit.accept(skippedEvent(item, SkipReason.SKIP_REASON_VLM_ERROR,
          call.callerChosen() ? vlm.safeMessage() : vlm.getMessage()));
    } catch (RuntimeException unexpected) {
      failed.incrementAndGet();
      emit.accept(skippedEvent(item, SkipReason.SKIP_REASON_UNSPECIFIED,
          "unexpected failure enriching this item: " + (call.callerChosen()
              ? unexpected.getClass().getSimpleName()
              : unexpected.toString())));
    }
  }

  /** Post-processes one chart output's reply the way Docling's
   * granite_vision_charts handler does: csv into typed cells (from a fenced
   * csv block when there is one), summary verbatim, code from the first
   * fenced python block. A reply that yields nothing usable is a
   * VlmException, so the output is skipped and the chart's other outputs
   * still land. */
  private static void chartAnnotation(
      WorkItem item, String content, ItemAnnotation.Builder annotation) throws VlmException {
    switch (item.chartOutput()) {
      case CHART_OUTPUT_SUMMARY -> {
        if (content.isBlank()) {
          throw new VlmException("chart model returned an empty summary");
        }
        annotation.getChartSummaryBuilder().setText(content);
      }
      case CHART_OUTPUT_CODE -> {
        String code = ChartCodeExtractor.extractPython(content);
        if (code == null || code.isEmpty()) {
          throw new VlmException("chart model reply carries no fenced python code block");
        }
        annotation.getChartCodeBuilder()
            .setText(code)
            .setLanguage(CodeLanguageLabel.CODE_LANGUAGE_LABEL_PYTHON);
      }
      default -> {
        String csv = ChartCsvParser.extractCsv(content);
        annotation.getChartTableBuilder()
            .setTable(ChartCsvParser.parse(csv))
            .setCsv(csv);
      }
    }
  }

  /** Applies the emitted annotations to a copy of the document. Additive
   * only: provenance boxes and orig text are never rewritten. Each patch
   * lands on the item whose VLM call produced it, keyed by document index —
   * not by self_ref, which pathological documents can duplicate. */
  private static Document applyPatches(
      Document document, ConcurrentLinkedQueue<EnrichedItem> enriched) {
    Document.Builder patched = document.toBuilder();
    for (EnrichedItem result : enriched) {
      WorkItem item = result.item();
      ItemAnnotation annotation = result.annotation();
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
          PictureItem.Builder updated = picture.toBuilder();
          switch (annotation.getAnnotationCase()) {
            case CHART_SUMMARY -> {
              updated.addAnnotations(PictureAnnotation.newBuilder()
                  .setDescription(DescriptionAnnotation.newBuilder()
                      .setKind("description")
                      .setText(annotation.getChartSummary().getText())
                      .setProvenance(annotation.getModel())));
              DescriptionMetaField.Builder description = updated.getMetaBuilder()
                  .getDescriptionBuilder()
                  .setText(annotation.getChartSummary().getText());
              if (!annotation.getModel().isEmpty()) {
                description.setCreatedBy(annotation.getModel());
              }
            }
            case CHART_CODE -> {
              CodeMetaField.Builder code = updated.getMetaBuilder()
                  .getCodeBuilder()
                  .setText(annotation.getChartCode().getText())
                  .setLanguage(annotation.getChartCode().getLanguage());
              if (!annotation.getModel().isEmpty()) {
                code.setCreatedBy(annotation.getModel());
              }
            }
            default -> updated.addAnnotations(PictureAnnotation.newBuilder()
                .setTabularChart(PictureTabularChartData.newBuilder()
                    .setKind("tabular_chart")
                    .setTitle(annotation.getChartTable().getTitle())
                    .setChartData(annotation.getChartTable().getTable())));
          }
          patched.setPictures(item.pictureIndex(), updated.build());
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
      WorkItem item, SkipReason reason, String detail) {
    return EnrichDocumentResponse.newBuilder()
        .setSkipped(ItemSkipped.newBuilder()
            .setSelfRef(item.selfRef())
            .setReason(reason)
            .setDetail(detail == null ? "" : detail)
            .setChartOutput(item.chartOutput()))
        .build();
  }
}
