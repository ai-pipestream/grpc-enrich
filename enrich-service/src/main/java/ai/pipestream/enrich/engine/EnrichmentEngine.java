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
import ai.pipestream.enrich.v1.VlmGenerationParams;
import ai.pipestream.enrich.v1.VlmHeader;
import ai.pipestream.enrich.vlm.VlmClient;
import ai.pipestream.enrich.vlm.VlmClient.Header;
import ai.pipestream.enrich.vlm.VlmClient.VlmException;
import ai.pipestream.enrich.vlm.VlmClient.VlmRequest;
import ai.pipestream.enrich.vlm.PublicAddress;
import ai.pipestream.enrich.vlm.VlmEndpoint;
import io.grpc.Status;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
 * <p><b>Concurrency.</b> {@code maxConcurrency} is a process-wide cap: one
 * fair semaphore shared by every request bounds the VLM calls in flight, so
 * N concurrent documents cannot put N times the cap on the VLM server (and
 * multiply it again with retries exactly when it pushes back). A request's
 * own {@code concurrency} is a sub-limit inside that cap.
 *
 * <p><b>Endpoints and credentials.</b> {@link EndpointPolicy} decides whether
 * a request may name its own endpoint, and the operator's key is attached
 * only to the operator's endpoint. A caller's {@code vlm_headers} go only to
 * the endpoint the caller named. A failure on a caller-named endpoint is
 * reported by its safe message, never with bytes that endpoint sent.
 *
 * <p><b>Cancellation.</b> {@link Cancellation#cancel()} interrupts every VLM
 * call the enrichment has in flight and stops the queued ones from starting,
 * so a client that went away stops costing VLM capacity.
 */
public final class EnrichmentEngine {

  /** Most vlm_headers one request may carry. */
  static final int MAX_HEADERS = 32;
  /** Longest vlm_headers name, in characters. */
  static final int MAX_HEADER_NAME_CHARS = 256;
  /** Longest vlm_headers value, in characters. */
  static final int MAX_HEADER_VALUE_CHARS = 8192;

  /**
   * Header names a caller may not set: the ones this client sets itself
   * (Content-Type, the framing headers), the target host, and the hop-by-hop
   * headers, which describe a connection rather than the request.
   */
  private static final Set<String> RESERVED_HEADERS = reservedHeaders();

  private static final System.Logger LOG = System.getLogger(EnrichmentEngine.class.getName());

  private final VlmClient.Factory clientFactory;
  private final EndpointPolicy endpoints;
  private final int defaultConcurrency;
  private final int maxConcurrency;
  private final Duration defaultTimeout;
  private final ExecutorService executor;
  /** Process-wide VLM call slots, shared by every enrichment. Fair, so a
   * request that queued first is served first. */
  private final Semaphore processSlots;
  /** Resolves a caller endpoint's host for the public-address check. */
  private final PublicAddress.Resolver resolver;

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
   * @param defaultConcurrency per-request VLM concurrency when the request
   *     names none
   * @param maxConcurrency the process-wide cap on in-flight VLM calls, which
   *     also bounds each request's concurrency
   * @param defaultTimeout per-call timeout when the request names none, and
   *     the ceiling on the one it names
   */
  public EnrichmentEngine(
      VlmClient.Factory clientFactory,
      EndpointPolicy endpoints,
      int defaultConcurrency,
      int maxConcurrency,
      Duration defaultTimeout,
      ExecutorService executor) {
    this(clientFactory, endpoints, defaultConcurrency, maxConcurrency, defaultTimeout, executor,
        PublicAddress.SYSTEM);
  }

  /**
   * {@link #EnrichmentEngine(VlmClient.Factory, EndpointPolicy, int, int,
   * Duration, ExecutorService)} with the resolver a caller endpoint's host
   * goes through (a test seam; the system resolver otherwise).
   */
  public EnrichmentEngine(
      VlmClient.Factory clientFactory,
      EndpointPolicy endpoints,
      int defaultConcurrency,
      int maxConcurrency,
      Duration defaultTimeout,
      ExecutorService executor,
      PublicAddress.Resolver resolver) {
    this.resolver = resolver;
    this.clientFactory = clientFactory;
    this.endpoints = endpoints;
    this.defaultConcurrency = defaultConcurrency;
    this.maxConcurrency = maxConcurrency;
    this.defaultTimeout = defaultTimeout;
    this.executor = executor;
    this.processSlots = new Semaphore(Math.max(1, maxConcurrency), true);
  }

  /**
   * Checks {@code options} against this server's endpoint policy and the
   * rules for headers and generation parameters, before any work starts.
   * Returns {@link Status#OK}, PERMISSION_DENIED for a per-request endpoint
   * the operator has not allowed, or INVALID_ARGUMENT. A description never
   * repeats a header value.
   */
  public Status validate(EnrichOptions options) {
    List<String> requested = requestEndpoints(options);
    for (String endpoint : requested) {
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
                + " ENRICH_VLM_ENDPOINT_ALLOWLIST, and the origin of ENRICH_VLM_URL is"
                + " always allowed");
      }
      // An IP literal is judged here; a host name is resolved, checked, and
      // pinned once when the calls start, so it is never resolved twice.
      if (endpoints.requiresPublicAddress(endpoint)) {
        InetAddress literal = PublicAddress.literal(URI.create(endpoint.strip()).getHost());
        if (literal != null && !PublicAddress.isPublic(literal)) {
          return Status.PERMISSION_DENIED.withDescription(
              "per-request VLM endpoint is a loopback, private, link-local, or other"
                  + " non-public address, which this server does not call for a caller");
        }
      }
    }
    if (options.getVlmHeadersCount() > 0) {
      if (requested.isEmpty()) {
        return Status.INVALID_ARGUMENT.withDescription(
            "vlm_headers are sent only to a per-request endpoint (vlm_endpoint or"
                + " chart_extraction.vlm_endpoint), and none is set");
      }
      Status headers = validateHeaders(options.getVlmHeadersList());
      if (!headers.isOk()) {
        return headers;
      }
    }
    return options.hasPictureDescriptionParams()
        ? validateParams(options.getPictureDescriptionParams())
        : Status.OK;
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

  private static Status validateHeaders(List<VlmHeader> headers) {
    if (headers.size() > MAX_HEADERS) {
      return Status.INVALID_ARGUMENT.withDescription(
          "vlm_headers carries " + headers.size() + " headers; at most " + MAX_HEADERS
              + " are allowed");
    }
    for (int i = 0; i < headers.size(); i++) {
      String name = headers.get(i).getName();
      String value = headers.get(i).getValue();
      if (name.isEmpty() || name.length() > MAX_HEADER_NAME_CHARS || !isToken(name)) {
        return Status.INVALID_ARGUMENT.withDescription(
            "vlm_headers[" + i + "] has an invalid header name");
      }
      if (RESERVED_HEADERS.contains(name)) {
        return Status.INVALID_ARGUMENT.withDescription(
            "vlm_headers[" + i + "] sets " + name + ", which this server does not let a caller"
                + " set");
      }
      if (value.length() > MAX_HEADER_VALUE_CHARS || !isHeaderValue(value)
          || !jdkAccepts(name, value)) {
        return Status.INVALID_ARGUMENT.withDescription(
            "vlm_headers[" + i + "] (" + name + ") has a value HTTP does not allow, or one"
                + " longer than " + MAX_HEADER_VALUE_CHARS + " characters");
      }
    }
    return Status.OK;
  }

  private static Status validateParams(VlmGenerationParams params) {
    if (params.hasMaxTokens() && params.getMaxTokens() == 0) {
      return Status.INVALID_ARGUMENT.withDescription(
          "picture_description_params.max_tokens must be positive");
    }
    if (params.hasTemperature()
        && !(Double.isFinite(params.getTemperature()) && params.getTemperature() >= 0.0)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "picture_description_params.temperature must be a finite, non-negative number");
    }
    if (params.hasTopP()
        && !(Double.isFinite(params.getTopP())
            && params.getTopP() >= 0.0 && params.getTopP() <= 1.0)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "picture_description_params.top_p must be between 0 and 1");
    }
    return Status.OK;
  }

  /** An RFC 9110 token: the characters an HTTP field name may use. */
  private static boolean isToken(String name) {
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      boolean alphanumeric = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
          || (c >= '0' && c <= '9');
      if (!alphanumeric && "!#$%&'*+-.^_`|~".indexOf(c) < 0) {
        return false;
      }
    }
    return true;
  }

  /** Visible ASCII, space, tab, and obs-text; never CR, LF, NUL, or DEL. */
  private static boolean isHeaderValue(String value) {
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c != '\t' && (c < 0x20 || c == 0x7f || c > 0xff)) {
        return false;
      }
    }
    return true;
  }

  /** Whether the JDK HttpClient takes this header. Checked up front because
   * the JDK's own refusal message repeats the value. */
  private static boolean jdkAccepts(String name, String value) {
    try {
      HttpRequest.newBuilder(URI.create("http://localhost/")).header(name, value);
      return true;
    } catch (IllegalArgumentException refused) {
      return false;
    }
  }

  private static Set<String> reservedHeaders() {
    Set<String> reserved = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    reserved.addAll(List.of("content-type", "content-length", "host", "expect", "connection",
        "keep-alive", "proxy-authenticate", "proxy-authorization", "proxy-connection", "te",
        "trailer", "transfer-encoding", "upgrade"));
    return reserved;
  }

  /**
   * Stops one enrichment. {@link #cancel()} marks it cancelled and interrupts
   * every VLM call it has in flight; calls not yet started never start, and
   * no trailer is emitted. Safe from any thread, any number of times, before
   * or after the enrichment starts.
   */
  public static final class Cancellation {
    private final Set<Future<?>> inFlight = ConcurrentHashMap.newKeySet();
    private volatile boolean cancelled;

    public void cancel() {
      cancelled = true;
      for (Future<?> call : inFlight) {
        call.cancel(true);
      }
    }

    public boolean isCancelled() {
      return cancelled;
    }

    /** Registers a call; one registered after cancel() is cancelled at once. */
    private void track(Future<?> call) {
      inFlight.add(call);
      if (cancelled) {
        call.cancel(true);
      }
    }

    private void forget(Future<?> call) {
      inFlight.remove(call);
    }
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
    enrich(document, crops, options, emit, new Cancellation());
  }

  /**
   * {@link #enrich(Document, Map, EnrichOptions, Consumer)} that stops when
   * {@code cancellation} is cancelled: in-flight VLM calls are interrupted,
   * queued ones never start, and it returns without a trailer.
   */
  public void enrich(
      Document document,
      Map<String, ItemImage> crops,
      EnrichOptions options,
      Consumer<EnrichDocumentResponse> emit,
      Cancellation cancellation) {
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

    Duration timeout = effectiveTimeout(options);
    List<Header> callerHeaders = options.getVlmHeadersList().stream()
        .map(header -> new Header(header.getName(), header.getValue()))
        .toList();
    List<Header> operatorHeaders = endpoints.defaultApiKey().isEmpty()
        ? List.of()
        : List.of(new Header("Authorization", "Bearer " + endpoints.defaultApiKey()));

    // One client per endpoint, closed once every call has finished.
    Map<String, VlmClient> clients = new HashMap<>();
    try {
      List<Call> runnable = bindCalls(selection.work(), options.getVlmEndpoint(), callerHeaders,
          operatorHeaders, clients, emit, skipped);
      runCalls(runnable, effectiveConcurrency(options), timeout, emit, cancellation,
          new Tally(succeeded, skipped, failed, enriched));
    } finally {
      clients.values().forEach(VlmClient::close);
    }
    if (cancellation.isCancelled()) {
      return;
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

  /** The request's concurrency, within the server's cap. The field is uint32
   * on the wire; Java surfaces values above 2^31 as negative ints, and a
   * wrapped value is above the cap, so it is clamped to it. */
  private int effectiveConcurrency(EnrichOptions options) {
    int requested = options.getConcurrency();
    if (requested == 0) {
      return defaultConcurrency;
    }
    return requested < 0 ? maxConcurrency : Math.min(requested, maxConcurrency);
  }

  /** The request's timeout: uint32 on the wire, so widened back to its
   * unsigned value. The server's own timeout is the ceiling: a caller can
   * shorten it, never lengthen it (it also bounds every Retry-After wait). */
  private Duration effectiveTimeout(EnrichOptions options) {
    Duration requested = Duration.ofSeconds(Integer.toUnsignedLong(options.getTimeoutSeconds()));
    return options.getTimeoutSeconds() == 0 || requested.compareTo(defaultTimeout) > 0
        ? defaultTimeout
        : requested;
  }

  /**
   * Binds each work item to its endpoint's client and headers; an item whose
   * endpoint is refused or unusable is skipped here, with its event. A work
   * item may name its own endpoint (chart calls routed to a chart model);
   * every other item uses the request's, then the operator's. Each endpoint
   * is bound once, so a host is resolved once.
   */
  private List<Call> bindCalls(
      List<WorkItem> work,
      String requestEndpoint,
      List<Header> callerHeaders,
      List<Header> operatorHeaders,
      Map<String, VlmClient> clients,
      Consumer<EnrichDocumentResponse> emit,
      AtomicInteger skipped) {
    List<Call> runnable = new ArrayList<>();
    Map<String, Refusal> refusals = new HashMap<>();
    for (WorkItem item : work) {
      String requested = item.endpoint() != null ? item.endpoint() : requestEndpoint;
      boolean callerChosen = !requested.isEmpty();
      String endpoint = callerChosen ? requested : endpoints.defaultEndpoint();
      Refusal refusal = refusals.computeIfAbsent(endpoint,
          unbound -> bind(unbound, callerChosen, clients));
      if (refusal != Refusal.NONE) {
        skipped.incrementAndGet();
        emit.accept(skippedEvent(item, refusal.reason(), refusal.detail()));
      } else {
        runnable.add(new Call(item, clients.get(endpoint), callerChosen,
            callerChosen ? callerHeaders : operatorHeaders));
      }
    }
    return runnable;
  }

  /**
   * Builds the client for {@code endpoint} into {@code clients}, or says why
   * its items are skipped. A caller endpoint the operator allowed only by
   * allowing any endpoint gets a client pinned to a checked public address.
   */
  private Refusal bind(String endpoint, boolean callerChosen, Map<String, VlmClient> clients) {
    if (endpoint.isEmpty()) {
      return new Refusal(SkipReason.SKIP_REASON_VLM_ERROR,
          "no VLM endpoint configured (set ENRICH_VLM_URL or EnrichOptions.vlm_endpoint)");
    }
    if (callerChosen && !endpoints.allowsRequestEndpoint(endpoint)) {
      return new Refusal(SkipReason.SKIP_REASON_ENDPOINT_REFUSED,
          "per-request VLM endpoints are not allowed on this server");
    }
    try {
      clients.put(endpoint, callerChosen && endpoints.requiresPublicAddress(endpoint)
          ? pinnedClient(endpoint)
          : clientFactory.create(endpoint));
      return Refusal.NONE;
    } catch (IllegalArgumentException unusable) {
      return new Refusal(SkipReason.SKIP_REASON_VLM_ERROR,
          "VLM endpoint is unusable: " + unusable.getMessage());
    } catch (PublicAddress.Unresolvable unknown) {
      return new Refusal(SkipReason.SKIP_REASON_VLM_ERROR, unknown.getMessage());
    } catch (PublicAddress.Refused notPublic) {
      return new Refusal(SkipReason.SKIP_REASON_ENDPOINT_REFUSED, notPublic.getMessage());
    }
  }

  /**
   * Runs {@code runnable} on the executor, at most {@code concurrency} at a
   * time for this request and within the process-wide slots, and waits for
   * every call. Returns early only through {@code cancellation}.
   */
  private void runCalls(
      List<Call> runnable,
      int concurrency,
      Duration timeout,
      Consumer<EnrichDocumentResponse> emit,
      Cancellation cancellation,
      Tally tally) {
    Semaphore requestSlots = new Semaphore(Math.max(1, concurrency));
    List<Future<?>> calls = new ArrayList<>(runnable.size());
    for (Call call : runnable) {
      Future<?> future = executor.submit(() -> {
        try {
          requestSlots.acquire();
          try {
            processSlots.acquire();
            try {
              if (!cancellation.isCancelled()) {
                runItem(call, timeout, emit, tally.succeeded(), tally.skipped(), tally.failed(),
                    tally.enriched());
              }
            } finally {
              processSlots.release();
            }
          } finally {
            requestSlots.release();
          }
        } catch (InterruptedException interrupt) {
          Thread.currentThread().interrupt();
          tally.failed().incrementAndGet();
        }
      });
      cancellation.track(future);
      calls.add(future);
    }
    for (int i = 0; i < calls.size(); i++) {
      Future<?> future = calls.get(i);
      try {
        future.get();
      } catch (CancellationException cancelled) {
        // Cancelled with the RPC: nothing is left to wait for, and no
        // trailer follows.
      } catch (ExecutionException escaped) {
        // An Error (out of memory encoding a crop, a stack overflow) got
        // past runItem's own handling. The item still gets its event and
        // its count, so the trailer adds up to EnrichStarted.
        unexpected(runnable.get(i), escaped.getCause(), emit, tally.failed());
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
        cancellation.cancel();
      }
      cancellation.forget(future);
    }
  }

  /**
   * A client for a caller endpoint that may only reach public addresses:
   * its host is resolved once, every address is checked, and the client is
   * pinned to the checked address so no later lookup decides where the
   * calls go.
   */
  private VlmClient pinnedClient(String endpoint) throws PublicAddress.Refused {
    String host = VlmEndpoint.completionsUri(endpoint).getHost();
    InetAddress address = PublicAddress.resolvePublic(host, resolver);
    return clientFactory.createPinned(endpoint, address);
  }

  /** Why an endpoint's items are skipped; {@link #NONE} when it is usable. */
  private record Refusal(SkipReason reason, String detail) {
    static final Refusal NONE = new Refusal(SkipReason.SKIP_REASON_UNSPECIFIED, "");
  }

  /** The per-request outcome counts and patches every call adds to. */
  private record Tally(AtomicInteger succeeded, AtomicInteger skipped, AtomicInteger failed,
      ConcurrentLinkedQueue<EnrichedItem> enriched) {}

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
      VlmGenerationParams sampling = item.sampling();
      String content = call.client().complete(new VlmRequest(
          item.model(),
          item.prompt(),
          item.image(),
          item.maxTokens(),
          sampling.hasTemperature()
              ? OptionalDouble.of(sampling.getTemperature()) : OptionalDouble.empty(),
          sampling.hasTopP() ? OptionalDouble.of(sampling.getTopP()) : OptionalDouble.empty(),
          sampling.hasSeed() ? OptionalLong.of(sampling.getSeed()) : OptionalLong.empty(),
          call.headers(),
          timeout));
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
      unexpected(call, unexpected, emit, failed);
    }
  }

  /** Counts, logs, and reports a failure no VLM error accounts for. The log
   * keeps the stack trace; the event names only the type for a caller-chosen
   * endpoint, whose bytes the message may carry. */
  private static void unexpected(
      Call call, Throwable failure, Consumer<EnrichDocumentResponse> emit, AtomicInteger failed) {
    failed.incrementAndGet();
    LOG.log(System.Logger.Level.ERROR,
        "unexpected failure enriching " + call.item().selfRef(), failure);
    emit.accept(skippedEvent(call.item(), SkipReason.SKIP_REASON_UNSPECIFIED,
        "unexpected failure enriching this item: " + (call.callerChosen()
            ? failure.getClass().getSimpleName()
            : failure.toString())));
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
