package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import ai.pipestream.enrich.vlm.OpenAiCompatVlmClient;
import ai.pipestream.enrich.vlm.VlmClient.VlmException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

/**
 * Retry behavior of the HTTP client: transient 503/429/5xx
 * and connection drops retry with exponential backoff; other 4xx and read
 * timeouts do not. Backoff is shrunk to 1ms so no test sleeps noticeably.
 */
class OpenAiCompatVlmClientTest {

  private static final Duration TINY_BACKOFF = Duration.ofMillis(1);

  private static OpenAiCompatVlmClient client(FakeVlmServer vlm) {
    return new OpenAiCompatVlmClient(vlm.url(), TINY_BACKOFF);
  }

  @Test
  void transient503Then200_succeeds() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.statusForCall = call -> call == 1 ? 503 : 200;
      String content = client(vlm).complete("m", "describe", null, 200, Duration.ofSeconds(5));

      assertThat(content).isEqualTo("fake description");
      assertThat(vlm.requests.size()).as("one 503 must trigger exactly one retry").isEqualTo(2);
    }
  }

  @Test
  void persistent503_throwsAfterFiveRetries() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.statusForCall = call -> 503;
      OpenAiCompatVlmClient client = client(vlm);

      assertThatThrownBy(() -> client.complete("m", "describe", null, 200, Duration.ofSeconds(5)))
          .isInstanceOf(VlmException.class);
      assertThat(vlm.requests.size()).as("1 initial attempt + 5 retries").isEqualTo(6);
    }
  }

  @Test
  void badRequest400_doesNotRetry() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.statusForCall = call -> 400;
      OpenAiCompatVlmClient client = client(vlm);

      assertThatThrownBy(() -> client.complete("m", "describe", null, 200, Duration.ofSeconds(5)))
          .isInstanceOf(VlmException.class);
      assertThat(vlm.requests.size()).as("4xx other than 429 must not retry").isEqualTo(1);
    }
  }

  @Test
  void connectionRefused_retriesThenThrows() throws Exception {
    int closedPort;
    try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
      closedPort = socket.getLocalPort();
    }
    OpenAiCompatVlmClient client =
        new OpenAiCompatVlmClient("http://127.0.0.1:" + closedPort, TINY_BACKOFF);

    VlmException failure = catchThrowableOfType(VlmException.class,
        () -> client.complete("m", "describe", null, 200, Duration.ofSeconds(5)));
    assertThat(failure.getMessage())
        .as("after 5 retries the connection failure surfaces as a VlmException").contains("failed");
  }

  @Test
  void requestTimeout_doesNotRetry() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      CountDownLatch never = new CountDownLatch(1);
      vlm.gates = Map.of(1, never);
      OpenAiCompatVlmClient client = client(vlm);

      assertThatThrownBy(() -> client.complete("m", "describe", null, 200, Duration.ofMillis(300)))
          .isInstanceOf(VlmException.class);
      assertThat(vlm.requests.size()).as("a read timeout must not retry").isEqualTo(1);
    }
  }
}
