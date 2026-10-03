package ai.pipestream.enrich.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The endpoint-policy knobs: per-request endpoints are refused unless
 * ENRICH_ALLOW_REQUEST_ENDPOINT or ENRICH_VLM_ENDPOINT_ALLOWLIST says
 * otherwise, bad values fail at startup, and neither the key nor anything
 * past the VLM URL's origin is ever printed.
 */
class EnrichConfigTest {

  private static final String URL_WITH_SECRETS =
      "https://svc:url-secret@vlm.internal:8443/v1?api_key=query-secret";
  private static final String KEY = "sk-operator-sentinel-77";

  @Test
  void requestEndpoints_areRefusedByDefault() {
    EnrichConfig config = EnrichConfig.from(Map.of("ENRICH_VLM_URL", "http://vlm:8080"));

    assertThat(config.allowRequestEndpoint()).isFalse();
    assertThat(config.vlmEndpointAllowlist()).isEmpty();
    assertThat(config.endpointPolicy().allowsRequestEndpoint("http://anything:1")).isFalse();
    assertThat(config.requestEndpointMode()).isEqualTo("refused");
  }

  @Test
  void theVlmUrlOrigin_isAlwaysAllowed() {
    var policy = EnrichConfig.from(Map.of("ENRICH_VLM_URL", URL_WITH_SECRETS)).endpointPolicy();
    assertThat(policy.allowsRequestEndpoint("https://VLM.internal:8443/v1/chat/completions"))
        .isTrue();
    assertThat(policy.allowsRequestEndpoint("https://other:pw@vlm.internal:8443")).isTrue();
    assertThat(policy.allowsRequestEndpoint("http://vlm.internal:8443")).isFalse();
    assertThat(policy.allowsRequestEndpoint("https://vlm.internal")).isFalse();
    assertThat(policy.allowsRequestEndpoint("https://vlm.internal.evil:8443")).isFalse();
    assertThat(EnrichConfig.from(Map.of()).endpointPolicy().allowsRequestEndpoint(""))
      .as("no ENRICH_VLM_URL allows nothing").isFalse();
  }

  @Test
  void allowRequestEndpoint_acceptsOnlyTrueOrFalse() {
    assertThat(EnrichConfig.from(Map.of("ENRICH_ALLOW_REQUEST_ENDPOINT", "TRUE"))
        .allowRequestEndpoint()).isTrue();
    assertThat(EnrichConfig.from(Map.of("ENRICH_ALLOW_REQUEST_ENDPOINT", "false"))
        .allowRequestEndpoint()).isFalse();
    assertThatThrownBy(() -> EnrichConfig.from(Map.of("ENRICH_ALLOW_REQUEST_ENDPOINT", "yes")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void allowlist_isNormalizedToOrigins() {
    EnrichConfig config = EnrichConfig.from(Map.of("ENRICH_VLM_ENDPOINT_ALLOWLIST",
        " https://VLM.internal:443 , http://chart-model:8086/ ,"));

    assertThat(config.vlmEndpointAllowlist())
        .containsExactlyInAnyOrder("https://vlm.internal", "http://chart-model:8086");
    assertThat(config.endpointPolicy()
        .allowsRequestEndpoint("http://chart-model:8086/v1/chat/completions")).isTrue();
    assertThat(config.endpointPolicy().allowsRequestEndpoint("http://chart-model:8087")).isFalse();
  }

  @Test
  void allowlistEntriesThatAreNotOrigins_failAtStartup() {
    for (String entry : new String[] {"http://host/v1/chat/completions", "ftp://host",
        "http://user:pw@host", "http://host?x=1", "host:8080"}) {
      assertThatThrownBy(() -> EnrichConfig.from(Map.of("ENRICH_VLM_ENDPOINT_ALLOWLIST", entry)))
          .as(entry).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void unusableVlmUrl_failsAtStartupWithoutRepeatingIt() {
    String bad = "ftp://user:url-secret@vlm/x";
    assertThatThrownBy(() -> EnrichConfig.from(Map.of("ENRICH_VLM_URL", bad)))
        .isInstanceOf(IllegalArgumentException.class)
        .satisfies(error -> assertThat(error.getMessage()).doesNotContain("url-secret"));
  }

  @Test
  void apiKeyWithControlCharacters_failsAtStartupWithoutRepeatingIt() {
    assertThatThrownBy(() -> EnrichConfig.from(Map.of("ENRICH_VLM_API_KEY", KEY + "\nX: y")))
        .isInstanceOf(IllegalArgumentException.class)
        .satisfies(error -> assertThat(error.getMessage()).doesNotContain(KEY));
  }

  @Test
  void toStringAndStartupLine_neverPrintTheKeyOrUrlSecrets() {
    EnrichConfig config = EnrichConfig.from(Map.of(
        "ENRICH_VLM_URL", URL_WITH_SECRETS,
        "ENRICH_VLM_API_KEY", KEY));

    assertThat(config.vlmApiKey()).isEqualTo(KEY);
    for (String printed : new String[] {config.toString(), config.endpointPolicy().toString(),
        GrpcEnrichServer.startupLine(config)}) {
      assertThat(printed).contains("https://vlm.internal:8443")
          .doesNotContain(KEY).doesNotContain("url-secret").doesNotContain("query-secret");
    }
    assertThat(GrpcEnrichServer.startupLine(config))
        .contains("with API key").contains("per-request endpoints: refused");
  }
}
