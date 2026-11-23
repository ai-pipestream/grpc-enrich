package ai.pipestream.enrich.server;

import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.v1.EnrichServiceGrpc;
import ai.pipestream.enrich.v1.VlmEndpointState;
import ai.pipestream.enrich.v1.VlmEndpointStatus;
import ai.pipestream.enrich.vlm.VlmEndpoint;
import ai.pipestream.enrich.vlm.VlmProbe;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.protobuf.services.HealthStatusManager;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Keeps this server's health honest about the one thing it cannot work
 * without: a VLM. Every endpoint the operator named (ENRICH_VLM_URL, then
 * each ENRICH_VLM_ENDPOINT_ALLOWLIST origin) is probed at startup and on an
 * interval, and {@code grpc.health.v1} follows the result instead of
 * answering SERVING from construction.
 *
 * <p><b>The rule.</b> With a default endpoint, health is that endpoint's
 * state: requests that name no endpoint depend on it. With no default
 * endpoint, health is SERVING while at least one allowlisted origin answers.
 * With no operator-named endpoint at all (ENRICH_ALLOW_REQUEST_ENDPOINT
 * alone) there is nothing to probe and health is SERVING. Until the first
 * probe finishes, health is NOT_SERVING: unknown is not healthy.
 *
 * <p>A down VLM never stops the server from starting or from taking RPCs:
 * items still come back as ItemSkipped{SKIP_REASON_VLM_ERROR}. Health is
 * what tells an orchestrator that this is all they will get.
 */
public final class VlmHealth implements AutoCloseable {

  private static final System.Logger LOG = System.getLogger(VlmHealth.class.getName());

  /** One endpoint to probe and what it last answered. */
  private static final class Target {
    final String origin;
    final boolean defaultEndpoint;
    final URI probeUri;
    final String apiKey;
    volatile VlmEndpointStatus status;

    Target(String origin, boolean defaultEndpoint, URI probeUri, String apiKey) {
      this.origin = origin;
      this.defaultEndpoint = defaultEndpoint;
      this.probeUri = probeUri;
      this.apiKey = apiKey;
      this.status = VlmEndpointStatus.newBuilder()
          .setOrigin(origin)
          .setDefaultEndpoint(defaultEndpoint)
          .setState(VlmEndpointState.VLM_ENDPOINT_STATE_UNSPECIFIED)
          .setDetail("not probed yet")
          .build();
    }
  }

  private final List<Target> targets;
  private final VlmProbe probe;
  private final HealthStatusManager health;
  private final Duration interval;
  private volatile boolean serving;
  private ScheduledExecutorService scheduler;

  /**
   * @param endpoints the operator's endpoint policy: its default endpoint
   *     and allowlisted origins are what gets probed
   * @param probe how an endpoint is asked
   * @param health the gRPC health service to keep in step
   * @param interval time between probe rounds
   */
  public VlmHealth(EndpointPolicy endpoints, VlmProbe probe, HealthStatusManager health,
      Duration interval) {
    this.probe = probe;
    this.health = health;
    this.interval = interval;
    this.targets = targets(endpoints);
    this.serving = targets.isEmpty();
    publish();
  }

  private static List<Target> targets(EndpointPolicy endpoints) {
    List<Target> targets = new ArrayList<>();
    String defaultOrigin = VlmEndpoint.origin(endpoints.defaultEndpoint());
    if (!endpoints.defaultEndpoint().isEmpty()) {
      URI models = VlmEndpoint.modelsUri(endpoints.defaultEndpoint());
      targets.add(new Target(defaultOrigin, true,
          models != null ? models : VlmEndpoint.completionsUri(endpoints.defaultEndpoint()),
          endpoints.defaultApiKey()));
    }
    // An allowlist entry is an origin only: its model list is asked for
    // where OpenAI-compatible servers keep it. The operator's key is never
    // sent to an origin the operator did not attach it to.
    endpoints.allowedRequestOrigins().stream()
        .filter(origin -> !origin.equals(defaultOrigin))
        .sorted()
        .forEach(origin -> targets.add(
            new Target(origin, false, VlmEndpoint.modelsUri(origin), "")));
    return List.copyOf(targets);
  }

  /** Probes now, on a thread of its own, then every interval. */
  public void start() {
    scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
      Thread thread = new Thread(task, "grpc-enrich-vlm-health");
      thread.setDaemon(true);
      return thread;
    });
    scheduler.scheduleWithFixedDelay(this::probeAll, 0, interval.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  /** One probe round over every target; health follows the result. */
  public void probeAll() {
    try {
      for (Target target : targets) {
        VlmProbe.Result result = probe.probe(target.probeUri, target.apiKey);
        VlmEndpointStatus before = target.status;
        target.status = VlmEndpointStatus.newBuilder()
            .setOrigin(target.origin)
            .setDefaultEndpoint(target.defaultEndpoint)
            .setState(result.serving()
                ? VlmEndpointState.VLM_ENDPOINT_STATE_SERVING
                : VlmEndpointState.VLM_ENDPOINT_STATE_NOT_SERVING)
            .setDetail(result.detail())
            .addAllModels(result.models())
            .build();
        if (before.getState() != target.status.getState()
            || !before.getModelsList().equals(target.status.getModelsList())) {
          LOG.log(result.serving() ? System.Logger.Level.INFO : System.Logger.Level.WARNING,
              "VLM endpoint " + target.origin + (result.serving()
                  ? " is serving" + (result.models().isEmpty() ? "" : " " + result.models())
                  : " is NOT serving: " + result.detail()));
        }
      }
      serving = decide();
      publish();
    } catch (RuntimeException unexpected) {
      // A probe round must never kill the schedule; an unknown state is
      // reported as what it is.
      LOG.log(System.Logger.Level.ERROR, "VLM health probe failed", unexpected);
      serving = false;
      publish();
    }
  }

  private boolean decide() {
    if (targets.isEmpty()) {
      return true;
    }
    Target first = targets.get(0);
    if (first.defaultEndpoint) {
      return first.status.getState() == VlmEndpointState.VLM_ENDPOINT_STATE_SERVING;
    }
    return targets.stream().anyMatch(
        target -> target.status.getState() == VlmEndpointState.VLM_ENDPOINT_STATE_SERVING);
  }

  private void publish() {
    ServingStatus status = serving ? ServingStatus.SERVING : ServingStatus.NOT_SERVING;
    health.setStatus(HealthStatusManager.SERVICE_NAME_ALL_SERVICES, status);
    health.setStatus(EnrichServiceGrpc.SERVICE_NAME, status);
  }

  /** Whether health is SERVING right now. */
  public boolean serving() {
    return serving;
  }

  /** Each operator-named endpoint and what it last answered, default first. */
  public List<VlmEndpointStatus> endpoints() {
    return targets.stream().map(target -> target.status).toList();
  }

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }
}
