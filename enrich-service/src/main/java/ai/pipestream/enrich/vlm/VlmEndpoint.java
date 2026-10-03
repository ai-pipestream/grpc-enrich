package ai.pipestream.enrich.vlm;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * VLM endpoint URLs: where a configured URL sends its chat-completions
 * requests, and the origin-only form that may appear in logs and replies.
 *
 * <p><b>Base URL or full URL.</b> This service has always taken a base URL
 * ({@code http://vlm:8080}) and appended {@code /v1/chat/completions}, while
 * Docling's {@code picture_description_api.url}, which gRParse forwards
 * verbatim, is the full endpoint: {@code http://localhost:8000/v1/chat/completions},
 * an Azure deployment's {@code .../chat/completions?api-version=...}, OVMS's
 * {@code /v3/chat/completions}. Both are accepted. A URL that carries a query
 * string, or whose path ends in {@code /chat/completions}, is used verbatim.
 * A path ending in a version segment ({@code /v1}, {@code /v3}: an OpenAI
 * SDK base_url) gets {@code /chat/completions}. Anything else, a bare origin
 * or a proxy prefix, gets {@code /v1/chat/completions}.
 *
 * <p><b>Origins.</b> A configured URL may carry a credential in its userinfo
 * or query, so only {@code scheme://host[:port]} ever leaves the process,
 * and the same normalized origin is what an operator's endpoint allowlist
 * matches.
 */
public final class VlmEndpoint {

  private static final String COMPLETIONS_PATH = "/chat/completions";
  private static final Pattern VERSION_SEGMENT = Pattern.compile(".*/v\\d+");

  private VlmEndpoint() {}

  /**
   * The URL a chat-completions request for {@code endpoint} goes to.
   *
   * @throws IllegalArgumentException when {@code endpoint} is not an absolute
   *     http or https URL with a host; the message never repeats the URL
   */
  public static URI completionsUri(String endpoint) {
    URI uri = parse(endpoint);
    if (uri.getRawQuery() != null) {
      return uri;
    }
    String path = uri.getRawPath() == null ? "" : uri.getRawPath();
    while (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    if (!path.endsWith(COMPLETIONS_PATH)) {
      path += VERSION_SEGMENT.matcher(path).matches()
          ? COMPLETIONS_PATH
          : "/v1" + COMPLETIONS_PATH;
    }
    return URI.create(uri.getScheme() + "://" + uri.getRawAuthority() + path);
  }

  /**
   * {@code scheme://host[:port]} of {@code endpoint}, lowercased, with the
   * scheme's default port left out; userinfo, path, query, and fragment are
   * dropped. Empty for a blank or unusable URL.
   */
  public static String origin(String endpoint) {
    if (endpoint == null || endpoint.isBlank()) {
      return "";
    }
    final URI uri;
    try {
      uri = parse(endpoint);
    } catch (IllegalArgumentException unusable) {
      return "";
    }
    String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
    int port = uri.getPort();
    boolean defaultPort = port == -1
        || (scheme.equals("http") && port == 80)
        || (scheme.equals("https") && port == 443);
    return scheme + "://" + uri.getHost().toLowerCase(Locale.ROOT)
        + (defaultPort ? "" : ":" + port);
  }

  private static URI parse(String endpoint) {
    final URI uri;
    try {
      uri = new URI(endpoint.strip());
    } catch (URISyntaxException bad) {
      throw new IllegalArgumentException("not a valid URL");
    }
    String scheme = uri.getScheme();
    if (scheme == null
        || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
      throw new IllegalArgumentException("not an http or https URL");
    }
    if (uri.getHost() == null || uri.getHost().isEmpty()) {
      throw new IllegalArgumentException("URL has no host");
    }
    return uri;
  }
}
