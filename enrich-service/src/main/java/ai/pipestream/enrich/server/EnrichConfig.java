package ai.pipestream.enrich.server;

import java.time.Duration;

/**
 * Server configuration resolved from the environment. All knobs are
 * validated on startup so a bad value fails fast instead of surfacing
 * mid-request.
 *
 * @param port gRPC listen port (ENRICH_PORT)
 * @param vlmUrl default VLM endpoint base URL, empty when unconfigured
 *     (ENRICH_VLM_URL)
 * @param maxDocumentBytes assembled-document byte cap
 *     (ENRICH_MAX_DOCUMENT_MIB)
 * @param maxConcurrentVlm cap on concurrent VLM calls per request
 *     (ENRICH_MAX_CONCURRENT_VLM; defaults to cores, min 2)
 * @param vlmTimeout per-VLM-call timeout (ENRICH_VLM_TIMEOUT_SECONDS)
 * @param metricsInterval metrics line interval; zero disables
 *     (ENRICH_METRICS_INTERVAL_SECONDS)
 * @param httpPort HTTP front-end listen port, or null when the listener is
 *     disabled (ENRICH_HTTP_PORT blank or "0")
 */
record EnrichConfig(
    int port,
    String vlmUrl,
    long maxDocumentBytes,
    int maxConcurrentVlm,
    Duration vlmTimeout,
    Duration metricsInterval,
    Integer httpPort) {

  /** Default port for the HTTP front end when ENRICH_HTTP_PORT is unset. */
  static final int DEFAULT_HTTP_PORT = 50068;

  /** Reads and validates every knob from the process environment. */
  static EnrichConfig fromEnv() {
    int cores = Runtime.getRuntime().availableProcessors();
    return new EnrichConfig(
        intFromEnv("ENRICH_PORT", 50056, 1, 65535),
        System.getenv().getOrDefault("ENRICH_VLM_URL", ""),
        intFromEnv("ENRICH_MAX_DOCUMENT_MIB", 70, 1, 4096) * 1024L * 1024L,
        intFromEnv("ENRICH_MAX_CONCURRENT_VLM", Math.max(2, cores), 1, 256),
        Duration.ofSeconds(intFromEnv("ENRICH_VLM_TIMEOUT_SECONDS", 300, 1, 86400)),
        Duration.ofSeconds(intFromEnv("ENRICH_METRICS_INTERVAL_SECONDS", 60, 0, 86400)),
        httpPortFromEnv());
  }

  /** Unset applies the default HTTP port; blank or "0" turns the listener off. */
  private static Integer httpPortFromEnv() {
    String configured = System.getenv("ENRICH_HTTP_PORT");
    if (configured == null) {
      return DEFAULT_HTTP_PORT;
    }
    if (configured.isBlank() || configured.strip().equals("0")) {
      return null;
    }
    return intFromEnv("ENRICH_HTTP_PORT", DEFAULT_HTTP_PORT, 1, 65535);
  }

  private static int intFromEnv(String name, int fallback, int min, int max) {
    String configured = System.getenv(name);
    if (configured == null || configured.isBlank()) {
      return fallback;
    }
    final int value;
    try {
      value = Integer.parseInt(configured.strip());
    } catch (NumberFormatException bad) {
      throw new IllegalArgumentException(name + " must be an integer, got: " + configured);
    }
    if (value < min || value > max) {
      throw new IllegalArgumentException(name + " must be in [" + min + ", " + max + "], got: "
          + value);
    }
    return value;
  }
}
