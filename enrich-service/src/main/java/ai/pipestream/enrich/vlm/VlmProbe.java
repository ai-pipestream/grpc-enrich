package ai.pipestream.enrich.vlm;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Asks a VLM endpoint whether it is there. The probe is a GET of the
 * endpoint's model list: it costs no generation, it does not queue behind
 * the completions the endpoint is busy with (so a loaded VLM does not read
 * as a dead one), and its reply names what the endpoint is serving.
 *
 * <p>Serving means the endpoint's HTTP server answered and did not refuse
 * the request: a 200 (with the model ids, when the body is a model list), or
 * any other status below 500 that is not a refused credential, because an
 * endpoint with no model list still answers 404 or 405 when it is up. Not
 * serving is a connection failure, a timeout, a 5xx (llama.cpp answers 503
 * while it loads a model), or 401/403, where every real call would fail the
 * same way.
 *
 * <p>The reply body is read up to {@link #MAX_BODY_BYTES}; a longer one is
 * an endpoint that answered, with a model list this probe does not read.
 */
public final class VlmProbe {

  /** Most model-list bytes read. */
  static final int MAX_BODY_BYTES = 1 << 20;

  /** What one probe found. {@code detail} says why when not serving. */
  public record Result(boolean serving, String detail, List<String> models) {

    static Result serving(List<String> models) {
      return new Result(true, "", List.copyOf(models));
    }

    static Result notServing(String detail) {
      return new Result(false, detail, List.of());
    }
  }

  private final HttpClient http;
  private final Duration timeout;

  /** A probe whose connect and whose whole request each get {@code timeout}. */
  public VlmProbe(Duration timeout) {
    this.timeout = timeout;
    this.http = HttpClient.newBuilder()
        .connectTimeout(timeout)
        .followRedirects(HttpClient.Redirect.NEVER)
        .build();
  }

  /**
   * GETs {@code uri} and classifies the answer.
   *
   * @param uri the endpoint's model-list URL, or its chat-completions URL
   *     when it has no model list this service can name
   * @param apiKey bearer token for the endpoint; empty for none
   */
  public Result probe(URI uri, String apiKey) {
    HttpRequest.Builder request = HttpRequest.newBuilder(uri).timeout(timeout).GET();
    if (!apiKey.isEmpty()) {
      request.header("Authorization", "Bearer " + apiKey);
    }
    final HttpResponse<InputStream> response;
    try {
      response = http.send(request.build(), HttpResponse.BodyHandlers.ofInputStream());
    } catch (java.net.http.HttpTimeoutException slow) {
      return Result.notServing("no answer within " + timeout.toMillis() + " ms");
    } catch (IOException unreachable) {
      return Result.notServing("unreachable: " + describe(unreachable));
    } catch (InterruptedException interrupt) {
      Thread.currentThread().interrupt();
      return Result.notServing("probe interrupted");
    }
    try (InputStream body = response.body()) {
      int status = response.statusCode();
      if (status == 401 || status == 403) {
        return Result.notServing("endpoint refused the credential (HTTP " + status + ")");
      }
      if (status >= 500) {
        return Result.notServing("HTTP " + status);
      }
      return Result.serving(status == 200 ? modelIds(body) : List.of());
    } catch (IOException cut) {
      return Result.notServing("reply cut short: " + describe(cut));
    }
  }

  /** The failure in words an operator can act on, without the URL. */
  private static String describe(IOException failure) {
    Throwable cause = failure.getCause() != null ? failure.getCause() : failure;
    String message = cause.getMessage();
    return cause.getClass().getSimpleName()
        + (message == null || message.isBlank() ? "" : " (" + message + ")");
  }

  /** The ids of an OpenAI-style model list ({@code {"data":[{"id":…}]}}).
   * Empty when the body is anything else: the endpoint answered, which is
   * what the probe is for. */
  private static List<String> modelIds(InputStream body) throws IOException {
    byte[] bytes = body.readNBytes(MAX_BODY_BYTES + 1);
    if (bytes.length > MAX_BODY_BYTES) {
      return List.of();
    }
    List<String> ids = new ArrayList<>();
    try {
      Map<String, Object> root =
          Json.asObject(Json.parse(new String(bytes, StandardCharsets.UTF_8)));
      for (Object model : Json.asArray(root.get("data"))) {
        Object id = Json.asObject(model).get("id");
        if (id instanceof String name && !name.isEmpty()) {
          ids.add(name);
        }
      }
    } catch (RuntimeException notAModelList) {
      return List.of();
    }
    return ids;
  }
}
