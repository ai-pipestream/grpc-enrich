package ai.pipestream.enrich.vlm;

import java.time.Duration;

/**
 * A single-shot completion client for a remote VLM server. Implementations
 * call the model server over HTTP; no model weights live in this process.
 */
public interface VlmClient {

  /**
   * Runs one completion.
   *
   * @param model the model name to ask for, or null/empty for the endpoint's
   *     default model
   * @param prompt the instruction text
   * @param imageDataUri a {@code data:<mime>;base64,<bytes>} URI for vision
   *     calls, or null for text-only calls
   * @param maxTokens generation cap (Docling parity: 200 for descriptions,
   *     2048 for code/formula, 4096 for chart tables)
   * @param timeout per-call timeout
   * @return the model's text response
   * @throws VlmException when the endpoint is unreachable, answers non-200,
   *     times out, or returns an unparseable body
   */
  String complete(String model, String prompt, String imageDataUri, int maxTokens, Duration timeout)
      throws VlmException;

  /** A failed VLM call. Always an item-level failure, never an RPC failure. */
  final class VlmException extends Exception {
    public VlmException(String message) {
      super(message);
    }

    public VlmException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /** Builds a client bound to one endpoint URL. */
  @FunctionalInterface
  interface Factory {
    VlmClient create(String endpoint);
  }
}
