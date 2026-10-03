package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.enrich.vlm.VlmEndpoint;
import org.junit.jupiter.api.Test;

/**
 * Where a configured VLM URL sends its requests: a base URL gets the
 * chat-completions path, a Docling-style full URL is used verbatim.
 */
class VlmEndpointTest {

  private static String resolved(String endpoint) {
    return VlmEndpoint.completionsUri(endpoint).toString();
  }

  @Test
  void baseUrls_getTheChatCompletionsPath() {
    assertThat(resolved("http://vlm:8080")).isEqualTo("http://vlm:8080/v1/chat/completions");
    assertThat(resolved("http://vlm:8080/")).isEqualTo("http://vlm:8080/v1/chat/completions");
    assertThat(resolved("http://proxy/models/llama"))
        .isEqualTo("http://proxy/models/llama/v1/chat/completions");
  }

  @Test
  void versionedBaseUrls_getOnlyChatCompletions() {
    assertThat(resolved("https://api.example.com/v1"))
        .isEqualTo("https://api.example.com/v1/chat/completions");
    assertThat(resolved("http://ovms:8000/v3/")).isEqualTo("http://ovms:8000/v3/chat/completions");
  }

  @Test
  void fullUrls_areUsedVerbatim() {
    // Docling's default, OVMS, and a proxy prefix in front of /v1.
    assertThat(resolved("http://localhost:8000/v1/chat/completions"))
        .isEqualTo("http://localhost:8000/v1/chat/completions");
    assertThat(resolved("http://ovms:8000/v3/chat/completions"))
        .isEqualTo("http://ovms:8000/v3/chat/completions");
    assertThat(resolved("https://gw.example/openai/v1/chat/completions"))
        .isEqualTo("https://gw.example/openai/v1/chat/completions");
  }

  @Test
  void urlsWithAQueryString_areUsedVerbatim() {
    String azure = "https://res.openai.azure.com/openai/deployments/gpt-4o/chat/completions"
        + "?api-version=2024-10-21";
    assertThat(resolved(azure)).isEqualTo(azure);
    String watsonx = "https://us-south.ml.cloud.ibm.com/ml/v1/text/chat?version=2023-05-29";
    assertThat(resolved(watsonx)).isEqualTo(watsonx);
  }

  @Test
  void unusableUrls_areRefusedWithoutRepeatingThem() {
    for (String bad : new String[] {"file:///etc/passwd", "ftp://host/x", "http:///x",
        "not a url", "//no-scheme/x"}) {
      assertThatThrownBy(() -> VlmEndpoint.completionsUri(bad)).as(bad)
          .isInstanceOf(IllegalArgumentException.class)
          .satisfies(error -> assertThat(error.getMessage()).doesNotContain(bad));
    }
  }
}
