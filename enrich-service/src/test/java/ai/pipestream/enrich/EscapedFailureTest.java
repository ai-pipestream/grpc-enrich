package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.engine.EnrichmentEngine;
import ai.pipestream.enrich.server.EnrichServiceImpl;
import ai.pipestream.enrich.v1.EnrichDocumentRequest;
import ai.pipestream.enrich.v1.EnrichDocumentResponse;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.EnrichServiceGrpc;
import ai.pipestream.enrich.vlm.VlmClient;
import ai.pipestream.enrich.vlm.VlmClient.VlmException;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * An RPC always ends with a status. A failure that escapes the enrichment
 * (anything the per-item guards do not cover) must reach the client as a
 * non-OK status rather than leaving the call open until the client's own
 * deadline, and an enrichment that stopped before its trailer must not end
 * the stream OK.
 */
class EscapedFailureTest {

  /** How long a test waits for the RPC to end. The broken behaviour is a
   * call that never ends, so this bounds the red run. */
  private static final Duration RPC_END = Duration.ofSeconds(5);

  private final List<AutoCloseable> cleanups = new ArrayList<>();

  @AfterEach
  void tearDown() throws Exception {
    for (AutoCloseable cleanup : cleanups) {
      cleanup.close();
    }
  }

  /** Runs submitted work on virtual threads and remembers the last thread
   * it started, so a test can interrupt the enrichment itself. */
  private static final class RememberingExecutor extends AbstractExecutorService {
    final AtomicReference<Thread> last = new AtomicReference<>();
    private final ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor();

    @Override
    public void execute(Runnable command) {
      threads.execute(() -> {
        last.set(Thread.currentThread());
        command.run();
      });
    }

    @Override
    public void shutdown() {
      threads.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      return threads.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
      return threads.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return threads.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return threads.awaitTermination(timeout, unit);
    }
  }

  private record Ended(List<EnrichDocumentResponse> events, StatusRuntimeException error,
      boolean completedOk) {}

  private EnrichServiceGrpc.EnrichServiceStub start(
      VlmClient.Factory clients, ExecutorService serviceExecutor) throws Exception {
    ExecutorService calls = Executors.newVirtualThreadPerTaskExecutor();
    EnrichmentEngine engine = new EnrichmentEngine(clients,
        EndpointPolicy.defaultOnly("http://vlm.invalid"), 4, 16, Duration.ofSeconds(30), calls);
    EnrichServiceImpl service = new EnrichServiceImpl(
        64L * 1024 * 1024, engine, serviceExecutor, "http://vlm.invalid", 16);
    String name = InProcessServerBuilder.generateName();
    Server server =
        InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
    ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    cleanups.add(() -> {
      channel.shutdownNow();
      server.shutdownNow();
      calls.shutdownNow();
      serviceExecutor.shutdownNow();
    });
    return EnrichServiceGrpc.newStub(channel);
  }

  private static BlockingQueue<Object> describeOnePicture(
      EnrichServiceGrpc.EnrichServiceStub stub) {
    BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
    StreamObserver<EnrichDocumentRequest> requester =
        stub.enrichDocument(InProcessEnrich.inbox(inbox));
    requester.onNext(InProcessEnrich.options(EnrichOptions.newBuilder()
        .setDoPictureDescription(true)
        .setDocument(InProcessEnrich.pictures(1))
        .build()));
    requester.onCompleted();
    return inbox;
  }

  /** Drains {@code inbox} until the RPC ends or {@link #RPC_END} passes. */
  private static Ended awaitEnd(BlockingQueue<Object> inbox) throws InterruptedException {
    List<EnrichDocumentResponse> events = new ArrayList<>();
    long deadline = System.nanoTime() + RPC_END.toNanos();
    while (true) {
      Object item = inbox.poll(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
      assertThat(item)
          .as("the RPC must end with a status; it was still open after " + RPC_END
              + " with events " + events)
          .isNotNull();
      if (item instanceof EnrichDocumentResponse event) {
        events.add(event);
      } else if (item instanceof StatusRuntimeException error) {
        return new Ended(events, error, false);
      } else {
        return new Ended(events, null, true);
      }
    }
  }

  @Test
  void exceptionEscapingTheEnrichment_endsTheRpcInternal() throws Exception {
    // Not one of the failures bind() turns into a skip: it leaves enrich().
    VlmClient.Factory broken = endpoint -> {
      throw new IllegalStateException("no default TLS context");
    };
    Ended ended = awaitEnd(describeOnePicture(
        start(broken, Executors.newVirtualThreadPerTaskExecutor())));

    assertThat(ended.completedOk()).as("a failed enrichment is not an OK stream").isFalse();
    assertThat(ended.error().getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
    assertThat(ended.error().getStatus().getDescription()).contains("IllegalStateException");
    assertThat(ended.events().stream().anyMatch(EnrichDocumentResponse::hasComplete))
        .as("no trailer claims the enrichment finished")
        .isFalse();
  }

  @Test
  void errorEscapingTheEnrichment_endsTheRpcInternal() throws Exception {
    VlmClient.Factory broken = endpoint -> {
      throw new StackOverflowError("deep");
    };
    Ended ended = awaitEnd(describeOnePicture(
        start(broken, Executors.newVirtualThreadPerTaskExecutor())));

    assertThat(ended.completedOk()).isFalse();
    assertThat(ended.error().getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
  }

  @Test
  void enrichmentInterruptedBeforeItsTrailer_doesNotEndTheStreamOk() throws Exception {
    CountDownLatch inFlight = new CountDownLatch(1);
    VlmClient.Factory stuck = endpoint -> request -> {
      inFlight.countDown();
      try {
        Thread.sleep(Duration.ofMinutes(5));
      } catch (InterruptedException interrupt) {
        Thread.currentThread().interrupt();
      }
      throw new VlmException("interrupted");
    };
    RememberingExecutor serviceExecutor = new RememberingExecutor();
    BlockingQueue<Object> inbox = describeOnePicture(start(stuck, serviceExecutor));
    assertThat(inFlight.await(10, TimeUnit.SECONDS)).isTrue();

    // What a server shutdown does to the enrichment thread while the client
    // is still connected.
    serviceExecutor.last.get().interrupt();

    Ended ended = awaitEnd(inbox);
    assertThat(ended.events().stream().anyMatch(EnrichDocumentResponse::hasComplete))
        .as("the enrichment did not finish, so there is no trailer")
        .isFalse();
    assertThat(ended.completedOk())
        .as("a stream without its EnrichComplete trailer must not end OK")
        .isFalse();
    assertThat(ended.error().getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
  }
}
