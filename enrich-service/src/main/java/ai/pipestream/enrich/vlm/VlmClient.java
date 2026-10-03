package ai.pipestream.enrich.vlm;

import com.google.protobuf.ByteString;
import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * A single-shot completion client for a remote VLM server. Implementations
 * call the model server over HTTP; no model weights live in this process.
 */
public interface VlmClient {

  /**
   * Runs one completion.
   *
   * @param request what to send: model, prompt, image, generation
   *     parameters, extra headers, and the per-call timeout
   * @return the model's text response
   * @throws VlmException when the endpoint is unreachable, answers non-200,
   *     times out, or returns an unparseable or oversized body
   */
  String complete(VlmRequest request) throws VlmException;

  /** Releases what this client holds for itself (a connection pool of its
   * own); a no-op for one that shares the process's. */
  default void close() {}

  /**
   * Runs one completion with only a token cap: no sampling parameters and no
   * extra headers.
   *
   * @param model the model name to ask for, or null/empty for the endpoint's
   *     default model
   * @param prompt the instruction text
   * @param imageDataUri a {@code data:<mime>;base64,<bytes>} URI for vision
   *     calls, or null for text-only calls
   * @param maxTokens generation cap
   * @param timeout per-call timeout
   */
  default String complete(
      String model, String prompt, String imageDataUri, int maxTokens, Duration timeout)
      throws VlmException {
    return complete(new VlmRequest(model, prompt,
        imageDataUri == null ? null : new VlmImage.DataUri(imageDataUri), maxTokens,
        OptionalDouble.empty(), OptionalDouble.empty(), OptionalLong.empty(), List.of(),
        timeout));
  }

  /**
   * One chat-completions call.
   *
   * @param model the model name to ask for, or null/empty for the endpoint's
   *     default model
   * @param prompt the instruction text
   * @param image the picture for a vision call, or null for a text-only call
   * @param maxTokens generation cap (200 for descriptions, 2048 for
   *     code/formula, 4096 for chart tables, unless the caller overrides it)
   * @param temperature sampling temperature, sent only when present
   * @param topP nucleus-sampling mass, sent only when present
   * @param seed sampling seed, sent only when present
   * @param headers extra HTTP headers (credentials for the endpoint)
   * @param timeout per-attempt timeout, covering the response body too
   */
  record VlmRequest(
      String model,
      String prompt,
      VlmImage image,
      long maxTokens,
      OptionalDouble temperature,
      OptionalDouble topP,
      OptionalLong seed,
      List<Header> headers,
      Duration timeout) {}

  /**
   * The picture for a vision call. An ItemImage crop stays raw bytes until
   * the request body for its own call is built, so a document's crops are
   * never all base64-encoded and held at once.
   */
  sealed interface VlmImage {
    /** A complete {@code data:} URI, as an ImageRef carries it inline. */
    record DataUri(String uri) implements VlmImage {}

    /** Raw image bytes from an ItemImage crop; empty mimetype means PNG. */
    record Bytes(String mimetype, ByteString data) implements VlmImage {}
  }

  /**
   * One extra HTTP request header. The value is usually a credential, so
   * {@link #toString()} never prints it.
   */
  record Header(String name, String value) {
    @Override
    public String toString() {
      return name + ": [REDACTED]";
    }
  }

  /**
   * A failed VLM call. Always an item-level failure, never an RPC failure.
   *
   * <p><b>Safe message.</b> {@link #getMessage()} may quote what the endpoint
   * sent (a body snippet, a transport error that repeats a malformed status
   * line). {@link #safeMessage()} leaves that out; it is what a failure of a
   * caller-chosen endpoint may report back, so the service cannot be used to
   * read responses from hosts the caller could not reach itself.
   */
  final class VlmException extends Exception {
    private final String safeMessage;

    public VlmException(String message) {
      this(message, (Throwable) null);
    }

    public VlmException(String message, Throwable cause) {
      super(message, cause);
      this.safeMessage = message;
    }

    /**
     * A failure whose full message adds {@code endpointText}, text that came
     * from the endpoint, to {@code safeMessage}.
     */
    public VlmException(String safeMessage, String endpointText, Throwable cause) {
      super(endpointText == null || endpointText.isEmpty()
          ? safeMessage : safeMessage + ": " + endpointText, cause);
      this.safeMessage = safeMessage;
    }

    /** The message without anything the endpoint sent. */
    public String safeMessage() {
      return safeMessage;
    }
  }

  /** Builds a client bound to one endpoint URL. */
  @FunctionalInterface
  interface Factory {
    VlmClient create(String endpoint);

    /**
     * A client bound to {@code endpoint} whose calls connect to
     * {@code address} only, never resolving the endpoint's host again,
     * while still presenting that host name (Host header, TLS server name
     * and certificate check). A factory that cannot pin refuses, so a
     * caller-chosen endpoint is never called unpinned.
     *
     * @throws IllegalArgumentException when this factory cannot pin
     */
    default VlmClient createPinned(String endpoint, InetAddress address) {
      throw new IllegalArgumentException("this VLM client cannot pin an endpoint's address");
    }
  }
}
