package ai.pipestream.enrich.engine;

import ai.pipestream.enrich.v1.EnrichDocumentResponse;
import ai.pipestream.enrich.v1.ItemSkipped;
import ai.pipestream.enrich.v1.SkipReason;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Process-wide counts of what enrichment did to the items it was given:
 * documents started and completed, annotations emitted, skips by reason, and
 * items that failed inside this service. They are counted from the events on
 * the wire, so the operator's view and the caller's view cannot disagree.
 *
 * <p>A document counts as completed only when its EnrichComplete trailer was
 * emitted. A VLM that is down shows up as {@code annotated=0} with every item
 * under {@code vlm_error}, rather than as documents "enriched".
 */
public final class EnrichMetrics {

  /** Most distinct VLM failure messages remembered for once-only logging. */
  public static final int MAX_REMEMBERED_FAILURES = 128;

  private final LongAdder documentsStarted = new LongAdder();
  private final LongAdder documentsCompleted = new LongAdder();
  private final LongAdder annotated = new LongAdder();
  private final LongAdder failed = new LongAdder();
  private final Map<SkipReason, LongAdder> skipped = new EnumMap<>(SkipReason.class);
  private final Set<String> seenFailures = ConcurrentHashMap.newKeySet();

  public EnrichMetrics() {
    for (SkipReason reason : SkipReason.values()) {
      if (isSkip(reason)) {
        skipped.put(reason, new LongAdder());
      }
    }
  }

  /** The reasons that are skips: every real reason but the internal fault,
   * which is counted under {@link #failed()} as the trailer counts it. */
  private static boolean isSkip(SkipReason reason) {
    return reason != SkipReason.SKIP_REASON_UNSPECIFIED
        && reason != SkipReason.SKIP_REASON_INTERNAL_ERROR
        && reason != SkipReason.UNRECOGNIZED;
  }

  /** Counts one event on its way to the caller. */
  void record(EnrichDocumentResponse event) {
    switch (event.getEventCase()) {
      case STARTED -> documentsStarted.increment();
      case COMPLETE -> documentsCompleted.increment();
      case ANNOTATION -> annotated.increment();
      case SKIPPED -> record(event.getSkipped());
      default -> { }
    }
  }

  private void record(ItemSkipped skip) {
    LongAdder counter = skipped.get(skip.getReason());
    if (counter == null) {
      failed.increment();
    } else {
      counter.increment();
    }
  }

  /**
   * Whether {@code failure} is a VLM failure message not seen recently, so
   * it is worth one log line. The memory is bounded: once
   * {@link #MAX_REMEMBERED_FAILURES} distinct messages are held it starts
   * over, which can only log a message again, never hide one.
   */
  public boolean firstSighting(String failure) {
    if (seenFailures.size() >= MAX_REMEMBERED_FAILURES) {
      seenFailures.clear();
    }
    return seenFailures.add(failure);
  }

  /** Distinct failure messages currently remembered. */
  public int rememberedFailures() {
    return seenFailures.size();
  }

  /** Documents whose enrichment started (EnrichStarted emitted). */
  public long documentsStarted() {
    return documentsStarted.sum();
  }

  /** Documents whose EnrichComplete trailer was emitted. */
  public long documentsCompleted() {
    return documentsCompleted.sum();
  }

  /** ItemAnnotation events emitted. */
  public long annotated() {
    return annotated.sum();
  }

  /** Items that failed inside this service (SKIP_REASON_INTERNAL_ERROR). */
  public long failed() {
    return failed.sum();
  }

  /** ItemSkipped events emitted with {@code reason}. */
  public long skipped(SkipReason reason) {
    LongAdder counter = skipped.get(reason);
    return counter == null ? 0 : counter.sum();
  }

  /** The counts as one log line fragment, every skip reason named. */
  public String line() {
    StringBuilder line = new StringBuilder()
        .append("docs{started=").append(documentsStarted())
        .append(",completed=").append(documentsCompleted())
        .append("} items{annotated=").append(annotated())
        .append(",failed=").append(failed())
        .append("} skipped{");
    boolean first = true;
    for (Map.Entry<SkipReason, LongAdder> entry : skipped.entrySet()) {
      if (!first) {
        line.append(',');
      }
      first = false;
      line.append(entry.getKey().name().substring("SKIP_REASON_".length())
              .toLowerCase(Locale.ROOT))
          .append('=').append(entry.getValue().sum());
    }
    return line.append('}').toString();
  }
}
