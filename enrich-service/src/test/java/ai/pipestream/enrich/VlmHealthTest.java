package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.server.EnrichHttpServer;
import ai.pipestream.enrich.server.EnrichServiceImpl;
import ai.pipestream.enrich.server.VlmHealth;
import ai.pipestream.enrich.v1.EnrichServiceGrpc;
import ai.pipestream.enrich.v1.GetServiceInfoRequest;
import ai.pipestream.enrich.v1.GetServiceInfoResponse;
import ai.pipestream.enrich.v1.VlmEndpointState;
import ai.pipestream.enrich.v1.VlmEndpointStatus;
import ai.pipestream.enrich.vlm.VlmProbe;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

/**
 * Health must follow the VLM. A grpc-enrich whose VLM is down, refusing its
 * key, or still loading answers every RPC OK and skips every item, so
 * {@code grpc.health.v1}, {@code GET /healthz} and GetServiceInfo have to
 * say so instead of answering SERVING from construction.
 */
class VlmHealthTest {

  /**
   * One server as GrpcEnrichServer wires it: the enrich service, the gRPC
   * health service and the HTTP front end, all reading one VlmHealth. Probe
   * rounds run when the test says, not on a timer.
   */
  private static final class Wired implements AutoCloseable {
    final HealthStatusManager manager = new HealthStatusManager();
    final VlmHealth health;
    final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Server server;
    private final ManagedChannel channel;
    private final EnrichHttpServer http;

    Wired(EndpointPolicy endpoints) throws Exception {
      this(endpoints, Duration.ofSeconds(5));
    }

    Wired(EndpointPolicy endpoints, Duration probeTimeout) throws Exception {
      health = new VlmHealth(endpoints, new VlmProbe(probeTimeout), manager,
          Duration.ofMillis(20));
      // The engine is never reached: these tests only ask about health.
      EnrichServiceImpl service = new EnrichServiceImpl(
          1 << 20, null, executor, endpoints.defaultEndpoint(), 2, health);
      String name = InProcessServerBuilder.generateName();
      server = InProcessServerBuilder.forName(name).directExecutor()
          .addService(service)
          .addService(manager.getHealthService())
          .build().start();
      channel = InProcessChannelBuilder.forName(name).directExecutor().build();
      http = new EnrichHttpServer(0, service, executor);
      http.start();
    }

    /** What a gRPC health check of {@code service} answers right now. */
    ServingStatus grpcHealth(String service) {
      return HealthGrpc.newBlockingStub(channel)
          .check(HealthCheckRequest.newBuilder().setService(service).build())
          .getStatus();
    }

    HttpResponse<String> healthz() throws Exception {
      try (HttpClient client = HttpClient.newHttpClient()) {
        return client.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + http.getPort() + "/healthz"))
                .GET().build(),
            HttpResponse.BodyHandlers.ofString());
      }
    }

    GetServiceInfoResponse serviceInfo() {
      return EnrichServiceGrpc.newBlockingStub(channel)
          .getServiceInfo(GetServiceInfoRequest.getDefaultInstance());
    }

    @Override
    public void close() {
      health.close();
      http.close();
      channel.shutdownNow();
      server.shutdownNow();
      executor.shutdownNow();
    }
  }

  /** The URL of a VLM that is not there: a port nothing listens on. */
  private static String deadUrl() {
    try (FakeVlmServer gone = new FakeVlmServer()) {
      return gone.url();
    }
  }

  private static EndpointPolicy allowlistOnly(String... origins) {
    return new EndpointPolicy("", "", false, Set.of(origins));
  }

  // -------------------------------------------------------------------------
  // The default endpoint decides
  // -------------------------------------------------------------------------

  @Test
  void unreachableVlm_isNotServing_everywhere() throws Exception {
    String dead = deadUrl();
    try (Wired wired = new Wired(EndpointPolicy.defaultOnly(dead))) {
      wired.health.probeAll();

      assertThat(wired.grpcHealth("")).isEqualTo(ServingStatus.NOT_SERVING);
      assertThat(wired.grpcHealth(EnrichServiceGrpc.SERVICE_NAME))
          .isEqualTo(ServingStatus.NOT_SERVING);
      HttpResponse<String> healthz = wired.healthz();
      assertThat(healthz.statusCode()).isEqualTo(503);
      assertThat(healthz.body()).isEqualTo("not serving");

      List<VlmEndpointStatus> endpoints = wired.serviceInfo().getVlmEndpointsList();
      assertThat(endpoints).hasSize(1);
      assertThat(endpoints.get(0).getOrigin()).isEqualTo(dead);
      assertThat(endpoints.get(0).getDefaultEndpoint()).isTrue();
      assertThat(endpoints.get(0).getState())
          .isEqualTo(VlmEndpointState.VLM_ENDPOINT_STATE_NOT_SERVING);
      assertThat(endpoints.get(0).getDetail()).startsWith("unreachable");
      assertThat(endpoints.get(0).getModelsList()).isEmpty();
    }
  }

  @Test
  void beforeTheFirstProbe_unknownIsNotHealthy() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        Wired wired = new Wired(EndpointPolicy.defaultOnly(vlm.url()))) {
      assertThat(wired.grpcHealth("")).isEqualTo(ServingStatus.NOT_SERVING);
      assertThat(wired.healthz().statusCode()).isEqualTo(503);
      VlmEndpointStatus endpoint = wired.serviceInfo().getVlmEndpoints(0);
      assertThat(endpoint.getState()).isEqualTo(VlmEndpointState.VLM_ENDPOINT_STATE_UNSPECIFIED);
      assertThat(endpoint.getDetail()).isEqualTo("not probed yet");
    }
  }

  @Test
  void answeringVlm_isServing_andNamesItsModels() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      vlm.modelIds = List.of("Qwen2.5-VL-7B-Instruct", "granite-docling");
      try (Wired wired = new Wired(EndpointPolicy.defaultOnly(vlm.url()))) {
        wired.health.probeAll();

        assertThat(wired.grpcHealth("")).isEqualTo(ServingStatus.SERVING);
        assertThat(wired.grpcHealth(EnrichServiceGrpc.SERVICE_NAME))
            .isEqualTo(ServingStatus.SERVING);
        HttpResponse<String> healthz = wired.healthz();
        assertThat(healthz.statusCode()).isEqualTo(200);
        assertThat(healthz.body()).isEqualTo("ok");

        VlmEndpointStatus endpoint = wired.serviceInfo().getVlmEndpoints(0);
        assertThat(endpoint.getOrigin()).isEqualTo(vlm.url());
        assertThat(endpoint.getState()).isEqualTo(VlmEndpointState.VLM_ENDPOINT_STATE_SERVING);
        assertThat(endpoint.getDetail()).isEmpty();
        assertThat(endpoint.getModelsList())
            .containsExactly("Qwen2.5-VL-7B-Instruct", "granite-docling");
        // The probe costs no generation.
        assertThat(vlm.calls()).isZero();
      }
    }
  }

  @Test
  void healthFollowsTheVlm_downAndBackUp() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        Wired wired = new Wired(EndpointPolicy.defaultOnly(vlm.url()))) {
      wired.health.probeAll();
      assertThat(wired.grpcHealth("")).isEqualTo(ServingStatus.SERVING);

      // llama.cpp answers 503 while it loads a model.
      vlm.modelsStatus = 503;
      wired.health.probeAll();
      assertThat(wired.grpcHealth("")).isEqualTo(ServingStatus.NOT_SERVING);
      assertThat(wired.healthz().statusCode()).isEqualTo(503);
      assertThat(wired.serviceInfo().getVlmEndpoints(0).getDetail()).isEqualTo("HTTP 503");

      vlm.modelsStatus = 200;
      wired.health.probeAll();
      assertThat(wired.grpcHealth("")).isEqualTo(ServingStatus.SERVING);
      assertThat(wired.healthz().statusCode()).isEqualTo(200);
    }
  }

  @Test
  void refusedCredential_isNotServing() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        Wired wired = new Wired(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.modelsStatus = 401;
      wired.health.probeAll();
      assertThat(wired.grpcHealth("")).isEqualTo(ServingStatus.NOT_SERVING);
      assertThat(wired.serviceInfo().getVlmEndpoints(0).getDetail())
          .isEqualTo("endpoint refused the credential (HTTP 401)");
    }
  }

  @Test
  void vlmThatDoesNotAnswerInTime_isNotServing() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      CountDownLatch never = new CountDownLatch(1);
      vlm.modelsGate = never;
      try (Wired wired = new Wired(EndpointPolicy.defaultOnly(vlm.url()),
          Duration.ofMillis(200))) {
        wired.health.probeAll();
        assertThat(wired.grpcHealth("")).isEqualTo(ServingStatus.NOT_SERVING);
        assertThat(wired.serviceInfo().getVlmEndpoints(0).getDetail())
            .isEqualTo("no answer within 200 ms");
      } finally {
        never.countDown();
      }
    }
  }

  @Test
  void endpointWithNoModelList_isServing_withNoModels() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        Wired wired = new Wired(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.modelsStatus = 404;
      wired.health.probeAll();
      assertThat(wired.grpcHealth("")).isEqualTo(ServingStatus.SERVING);
      VlmEndpointStatus endpoint = wired.serviceInfo().getVlmEndpoints(0);
      assertThat(endpoint.getState()).isEqualTo(VlmEndpointState.VLM_ENDPOINT_STATE_SERVING);
      assertThat(endpoint.getModelsList()).isEmpty();
    }
  }

  @Test
  void defaultEndpointDown_isNotServing_evenWhenAnAllowlistedOneAnswers() throws Exception {
    String dead = deadUrl();
    try (FakeVlmServer other = new FakeVlmServer();
        Wired wired = new Wired(new EndpointPolicy(dead, "", false, Set.of(other.url())))) {
      wired.health.probeAll();
      // Requests that name no endpoint depend on the default one.
      assertThat(wired.grpcHealth("")).isEqualTo(ServingStatus.NOT_SERVING);
      List<VlmEndpointStatus> endpoints = wired.serviceInfo().getVlmEndpointsList();
      assertThat(endpoints).extracting(VlmEndpointStatus::getOrigin)
          .containsExactly(dead, other.url());
      assertThat(endpoints).extracting(VlmEndpointStatus::getState).containsExactly(
          VlmEndpointState.VLM_ENDPOINT_STATE_NOT_SERVING,
          VlmEndpointState.VLM_ENDPOINT_STATE_SERVING);
    }
  }

  // -------------------------------------------------------------------------
  // No default endpoint: the allowlisted origins are what there is
  // -------------------------------------------------------------------------

  @Test
  void noDefault_oneAllowlistedOriginAnswers_isServing() throws Exception {
    String dead = deadUrl();
    try (FakeVlmServer vlm = new FakeVlmServer();
        Wired wired = new Wired(allowlistOnly(vlm.url(), dead))) {
      wired.health.probeAll();
      assertThat(wired.grpcHealth("")).isEqualTo(ServingStatus.SERVING);
      assertThat(wired.healthz().statusCode()).isEqualTo(200);
      List<VlmEndpointStatus> endpoints = wired.serviceInfo().getVlmEndpointsList();
      assertThat(endpoints).hasSize(2);
      assertThat(endpoints).noneMatch(VlmEndpointStatus::getDefaultEndpoint);
      assertThat(endpoints).filteredOn(endpoint -> endpoint.getOrigin().equals(vlm.url()))
          .singleElement()
          .satisfies(up -> {
            assertThat(up.getState()).isEqualTo(VlmEndpointState.VLM_ENDPOINT_STATE_SERVING);
            assertThat(up.getModelsList()).containsExactly("fake-vlm");
          });
      assertThat(endpoints).filteredOn(endpoint -> endpoint.getOrigin().equals(dead))
          .singleElement()
          .satisfies(down -> assertThat(down.getState())
              .isEqualTo(VlmEndpointState.VLM_ENDPOINT_STATE_NOT_SERVING));
    }
  }

  @Test
  void noDefault_noAllowlistedOriginAnswers_isNotServing() throws Exception {
    try (Wired wired = new Wired(allowlistOnly(deadUrl(), deadUrl()))) {
      wired.health.probeAll();
      assertThat(wired.grpcHealth("")).isEqualTo(ServingStatus.NOT_SERVING);
      assertThat(wired.healthz().statusCode()).isEqualTo(503);
    }
  }

  @Test
  void anyRequestEndpoint_withNoneNamed_hasNothingToProbe_andIsServing() throws Exception {
    try (Wired wired = new Wired(new EndpointPolicy("", "", true, Set.of()))) {
      assertThat(wired.grpcHealth("")).isEqualTo(ServingStatus.SERVING);
      wired.health.probeAll();
      assertThat(wired.grpcHealth("")).isEqualTo(ServingStatus.SERVING);
      assertThat(wired.serviceInfo().getVlmEndpointsList()).isEmpty();
    }
  }

  // -------------------------------------------------------------------------
  // The probe itself
  // -------------------------------------------------------------------------

  @Test
  void operatorKey_goesToTheDefaultEndpointOnly() throws Exception {
    try (FakeVlmServer own = new FakeVlmServer();
        FakeVlmServer other = new FakeVlmServer();
        Wired wired = new Wired(
            new EndpointPolicy(own.url(), "s3cret-key", false, Set.of(other.url())))) {
      wired.health.probeAll();
      assertThat(own.modelListRequests).hasSize(1);
      assertThat(headerValues(own, "Authorization")).containsExactly("Bearer s3cret-key");
      assertThat(other.modelListRequests).hasSize(1);
      assertThat(headerValues(other, "Authorization")).isEmpty();
    }
  }

  @Test
  void versionedEndpoint_isProbedAtItsOwnModelList() throws Exception {
    // A full endpoint URL: the model list is the sibling of chat/completions.
    try (FakeVlmServer vlm = new FakeVlmServer();
        Wired wired = new Wired(
            EndpointPolicy.defaultOnly(vlm.url() + "/v1/chat/completions"))) {
      wired.health.probeAll();
      assertThat(vlm.modelListRequests).hasSize(1);
      assertThat(wired.serviceInfo().getVlmEndpoints(0).getModelsList())
          .containsExactly("fake-vlm");
    }
  }

  @Test
  void started_probesOnItsOwn_andKeepsProbing() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        Wired wired = new Wired(EndpointPolicy.defaultOnly(vlm.url()))) {
      wired.health.start();
      await().atMost(Duration.ofSeconds(10))
          .until(() -> wired.grpcHealth("") == ServingStatus.SERVING);
      vlm.modelsStatus = 500;
      await().atMost(Duration.ofSeconds(10))
          .until(() -> wired.grpcHealth("") == ServingStatus.NOT_SERVING);
    }
  }

  @Test
  void serviceWithNoHealthTracker_reportsNoEndpointStates() throws Exception {
    try (InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(""))) {
      assertThat(enrich.service.vlmServing()).isTrue();
    }
  }

  private static List<String> headerValues(FakeVlmServer vlm, String name) {
    return vlm.modelListRequests.stream()
        .flatMap(headers -> headers.entrySet().stream())
        .filter(entry -> entry.getKey().equalsIgnoreCase(name))
        .flatMap(entry -> entry.getValue().stream())
        .toList();
  }
}
