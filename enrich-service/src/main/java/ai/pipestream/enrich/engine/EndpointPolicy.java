package ai.pipestream.enrich.engine;

import ai.pipestream.enrich.vlm.VlmEndpoint;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Which VLM endpoints a request may reach, and which credentials go where.
 *
 * <p><b>Caller-chosen endpoints are refused by default.</b> A per-request
 * {@code vlm_endpoint} (gRParse fills it from the end user's Docling
 * {@code picture_description_api.url}) would otherwise let any caller make
 * this server POST to any host it can reach, cluster-internal services and
 * cloud metadata included. Docling gates the same feature behind
 * {@code enable_remote_services}; here the operator opts in with
 * {@code ENRICH_ALLOW_REQUEST_ENDPOINT} (any http or https URL) or names the
 * origins callers may use in {@code ENRICH_VLM_ENDPOINT_ALLOWLIST}.
 *
 * <p><b>The operator's key stays with the operator's endpoint.</b>
 * {@code defaultApiKey} is sent only on calls to {@code defaultEndpoint},
 * never to an endpoint a caller named, or allowing per-request endpoints
 * would let a caller collect the key by pointing the URL at its own server.
 *
 * @param defaultEndpoint the operator's endpoint (ENRICH_VLM_URL); empty when
 *     unconfigured
 * @param defaultApiKey bearer token for {@code defaultEndpoint}
 *     (ENRICH_VLM_API_KEY); empty for none
 * @param allowAnyRequestEndpoint whether callers may name any http or https
 *     endpoint (ENRICH_ALLOW_REQUEST_ENDPOINT)
 * @param allowedRequestOrigins origins callers may name even when
 *     {@code allowAnyRequestEndpoint} is off, normalized by
 *     {@link VlmEndpoint#origin(String)} (ENRICH_VLM_ENDPOINT_ALLOWLIST)
 */
public record EndpointPolicy(
    String defaultEndpoint,
    String defaultApiKey,
    boolean allowAnyRequestEndpoint,
    Set<String> allowedRequestOrigins) {

  public EndpointPolicy {
    defaultEndpoint = defaultEndpoint == null ? "" : defaultEndpoint;
    defaultApiKey = defaultApiKey == null ? "" : defaultApiKey;
    allowedRequestOrigins = allowedRequestOrigins.stream()
        .map(VlmEndpoint::origin)
        .filter(origin -> !origin.isEmpty())
        .collect(Collectors.toUnmodifiableSet());
  }

  /** Only the operator's endpoint, no key, no per-request endpoints. */
  public static EndpointPolicy defaultOnly(String defaultEndpoint) {
    return new EndpointPolicy(defaultEndpoint, "", false, Set.of());
  }

  /** Whether a caller may send this request's calls to {@code endpoint}. */
  public boolean allowsRequestEndpoint(String endpoint) {
    if (allowAnyRequestEndpoint) {
      return true;
    }
    String origin = VlmEndpoint.origin(endpoint);
    return !origin.isEmpty() && allowedRequestOrigins.contains(origin);
  }

  /** Never prints the key, and only the endpoint's origin. */
  @Override
  public String toString() {
    return "EndpointPolicy[defaultEndpoint=" + VlmEndpoint.origin(defaultEndpoint)
        + ", defaultApiKey=" + (defaultApiKey.isEmpty() ? "unset" : "[REDACTED]")
        + ", allowAnyRequestEndpoint=" + allowAnyRequestEndpoint
        + ", allowedRequestOrigins=" + allowedRequestOrigins + "]";
  }
}
