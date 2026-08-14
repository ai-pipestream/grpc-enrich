package ai.pipestream.enrich.vlm;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * VLM client for any OpenAI-compatible chat-completions endpoint (llama.cpp's
 * server, OVMS, vLLM, and friends). The endpoint is a base URL such as
 * {@code http://vlm:8080}; the request goes to
 * {@code <endpoint>/v1/chat/completions} unless the endpoint already ends with
 * that path.
 */
public final class OpenAiCompatVlmClient implements VlmClient {

  private static final String COMPLETIONS_PATH = "/v1/chat/completions";

  private final String completionsUrl;
  private final HttpClient http;

  public OpenAiCompatVlmClient(String endpoint) {
    String trimmed = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
    this.completionsUrl =
        trimmed.endsWith(COMPLETIONS_PATH) ? trimmed : trimmed + COMPLETIONS_PATH;
    this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  }

  @Override
  public String complete(String model, String prompt, String imageDataUri, Duration timeout)
      throws VlmException {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(completionsUrl))
            .timeout(timeout)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody(model, prompt, imageDataUri)))
            .build();
    final HttpResponse<String> response;
    try {
      response = http.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (InterruptedException interrupt) {
      Thread.currentThread().interrupt();
      throw new VlmException("interrupted waiting for the VLM endpoint", interrupt);
    } catch (Exception failure) {
      throw new VlmException("VLM endpoint call failed: " + failure.getMessage(), failure);
    }
    if (response.statusCode() != 200) {
      throw new VlmException(
          "VLM endpoint answered HTTP " + response.statusCode() + ": " + snippet(response.body()));
    }
    return extractContent(response.body());
  }

  private static String requestBody(String model, String prompt, String imageDataUri) {
    StringBuilder body = new StringBuilder(256 + prompt.length());
    body.append('{');
    if (model != null && !model.isEmpty()) {
      body.append("\"model\":").append(Json.quote(model)).append(',');
    }
    body.append("\"messages\":[{\"role\":\"user\",\"content\":[");
    body.append("{\"type\":\"text\",\"text\":").append(Json.quote(prompt)).append('}');
    if (imageDataUri != null) {
      body.append(",{\"type\":\"image_url\",\"image_url\":{\"url\":")
          .append(Json.quote(imageDataUri))
          .append("}}");
    }
    body.append("]}],\"max_tokens\":1024}");
    return body.toString();
  }

  /** Reads choices[0].message.content out of the chat-completions reply. */
  static String extractContent(String body) throws VlmException {
    final Map<String, Object> root;
    try {
      root = Json.asObject(Json.parse(body));
    } catch (IllegalArgumentException bad) {
      throw new VlmException("unparseable VLM response: " + snippet(body), bad);
    }
    if (root.containsKey("error")) {
      throw new VlmException("VLM endpoint returned an error: " + snippet(body));
    }
    try {
      List<Object> choices = Json.asArray(root.get("choices"));
      Map<String, Object> first = Json.asObject(choices.get(0));
      Map<String, Object> message = Json.asObject(first.get("message"));
      return Json.asString(message.get("content"));
    } catch (RuntimeException shape) {
      throw new VlmException("VLM response had no choices[0].message.content: " + snippet(body),
          shape);
    }
  }

  private static String snippet(String body) {
    if (body == null) {
      return "";
    }
    return body.length() <= 200 ? body : body.substring(0, 200) + "...";
  }
}
