package ai.pipestream.enrich.server;

import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.vlm.VlmEndpoint;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Server configuration resolved from the environment. All knobs are
 * validated on startup so a bad value fails fast instead of surfacing
 * mid-request. A startup error never repeats the VLM URL or key: either
 * may carry a credential.
 *
 * @param port gRPC listen port (ENRICH_PORT)
 * @param vlmUrl default VLM endpoint, a base URL or a full endpoint URL;
 *     empty when unconfigured (ENRICH_VLM_URL)
 * @param vlmApiKey bearer token sent to {@code vlmUrl} only, never to a
 *     per-request endpoint; empty for none (ENRICH_VLM_API_KEY)
 * @param allowRequestEndpoint whether a request may name any http or https
 *     VLM endpoint of its own (ENRICH_ALLOW_REQUEST_ENDPOINT, default false)
 * @param vlmEndpointAllowlist origins a request may name even when
 *     {@code allowRequestEndpoint} is off (ENRICH_VLM_ENDPOINT_ALLOWLIST,
 *     comma-separated, for example {@code https://vlm.internal:8443})
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
    String vlmApiKey,
    boolean allowRequestEndpoint,
    Set<String> vlmEndpointAllowlist,
    long maxDocumentBytes,
    int maxConcurrentVlm,
    Duration vlmTimeout,
    Duration metricsInterval,
    Integer httpPort) {

  /** Default port for the HTTP front end when ENRICH_HTTP_PORT is unset. */
  static final int DEFAULT_HTTP_PORT = 50068;

  /** Reads and validates every knob from the process environment. */
  static EnrichConfig fromEnv() {
    return from(System.getenv());
  }

  /** Reads and validates every knob from {@code env}. */
  static EnrichConfig from(Map<String, String> env) {
    int cores = Runtime.getRuntime().availableProcessors();
    return new EnrichConfig(
        intFromEnv(env, "ENRICH_PORT", 50056, 1, 65535),
        vlmUrlFromEnv(env),
        apiKeyFromEnv(env),
        booleanFromEnv(env, "ENRICH_ALLOW_REQUEST_ENDPOINT"),
        allowlistFromEnv(env),
        intFromEnv(env, "ENRICH_MAX_DOCUMENT_MIB", 70, 1, 4096) * 1024L * 1024L,
        intFromEnv(env, "ENRICH_MAX_CONCURRENT_VLM", Math.max(2, cores), 1, 256),
        Duration.ofSeconds(intFromEnv(env, "ENRICH_VLM_TIMEOUT_SECONDS", 300, 1, 86400)),
        Duration.ofSeconds(intFromEnv(env, "ENRICH_METRICS_INTERVAL_SECONDS", 60, 0, 86400)),
        httpPortFromEnv(env));
  }

  /** The endpoint policy these knobs describe. */
  EndpointPolicy endpointPolicy() {
    return new EndpointPolicy(vlmUrl, vlmApiKey, allowRequestEndpoint, vlmEndpointAllowlist);
  }

  /** How per-request endpoints are treated, for the startup line. */
  String requestEndpointMode() {
    if (allowRequestEndpoint) {
      return "any";
    }
    return vlmEndpointAllowlist.isEmpty() ? "refused" : "allowlist " + vlmEndpointAllowlist;
  }

  /** Never prints the key, and only the VLM URL's origin. */
  @Override
  public String toString() {
    return "EnrichConfig[port=" + port
        + ", vlmUrl=" + VlmEndpoint.origin(vlmUrl)
        + ", vlmApiKey=" + (vlmApiKey.isEmpty() ? "unset" : "[REDACTED]")
        + ", allowRequestEndpoint=" + allowRequestEndpoint
        + ", vlmEndpointAllowlist=" + vlmEndpointAllowlist
        + ", maxDocumentBytes=" + maxDocumentBytes
        + ", maxConcurrentVlm=" + maxConcurrentVlm
        + ", vlmTimeout=" + vlmTimeout
        + ", metricsInterval=" + metricsInterval
        + ", httpPort=" + httpPort + "]";
  }

  private static String vlmUrlFromEnv(Map<String, String> env) {
    String url = env.getOrDefault("ENRICH_VLM_URL", "").strip();
    if (!url.isEmpty()) {
      try {
        VlmEndpoint.completionsUri(url);
      } catch (IllegalArgumentException unusable) {
        throw new IllegalArgumentException("ENRICH_VLM_URL is unusable: " + unusable.getMessage());
      }
    }
    return url;
  }

  private static String apiKeyFromEnv(Map<String, String> env) {
    String key = env.getOrDefault("ENRICH_VLM_API_KEY", "").strip();
    for (int i = 0; i < key.length(); i++) {
      char c = key.charAt(i);
      if (c < 0x21 || c > 0x7e) {
        throw new IllegalArgumentException(
            "ENRICH_VLM_API_KEY may only contain visible ASCII characters");
      }
    }
    return key;
  }

  private static boolean booleanFromEnv(Map<String, String> env, String name) {
    String configured = env.get(name);
    if (configured == null || configured.isBlank()) {
      return false;
    }
    return switch (configured.strip().toLowerCase(Locale.ROOT)) {
      case "true" -> true;
      case "false" -> false;
      default -> throw new IllegalArgumentException(
          name + " must be true or false, got: " + configured);
    };
  }

  /** Comma-separated origins, each normalized the way requests are matched. */
  private static Set<String> allowlistFromEnv(Map<String, String> env) {
    String configured = env.getOrDefault("ENRICH_VLM_ENDPOINT_ALLOWLIST", "");
    Set<String> origins = new LinkedHashSet<>();
    for (String entry : configured.split(",")) {
      String origin = entry.strip();
      if (origin.isEmpty()) {
        continue;
      }
      if (!isBareOrigin(origin) || VlmEndpoint.origin(origin).isEmpty()) {
        throw new IllegalArgumentException("ENRICH_VLM_ENDPOINT_ALLOWLIST entries must be"
            + " origins such as https://vlm.internal:8443, got: " + origin);
      }
      origins.add(VlmEndpoint.origin(origin));
    }
    return Set.copyOf(origins);
  }

  /** scheme://host[:port] with nothing else: no userinfo, path, or query. */
  private static boolean isBareOrigin(String origin) {
    try {
      URI uri = new URI(origin);
      String path = uri.getRawPath();
      return uri.getRawUserInfo() == null && uri.getRawQuery() == null
          && uri.getRawFragment() == null && (path == null || path.isEmpty() || path.equals("/"));
    } catch (URISyntaxException bad) {
      return false;
    }
  }

  /** Unset applies the default HTTP port; blank or "0" turns the listener off. */
  private static Integer httpPortFromEnv(Map<String, String> env) {
    String configured = env.get("ENRICH_HTTP_PORT");
    if (configured == null) {
      return DEFAULT_HTTP_PORT;
    }
    if (configured.isBlank() || configured.strip().equals("0")) {
      return null;
    }
    return intFromEnv(env, "ENRICH_HTTP_PORT", DEFAULT_HTTP_PORT, 1, 65535);
  }

  private static int intFromEnv(Map<String, String> env, String name, int fallback, int min,
      int max) {
    String configured = env.get(name);
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
