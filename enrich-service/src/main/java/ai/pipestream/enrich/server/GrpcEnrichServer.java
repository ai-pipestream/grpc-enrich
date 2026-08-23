package ai.pipestream.enrich.server;

import ai.pipestream.enrich.engine.EnrichmentEngine;
import ai.pipestream.enrich.vlm.OpenAiCompatVlmClient;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * grpc-enrich: Document in, live item-annotation stream out. The VLM is a
 * remote server this process calls over HTTP (ENRICH_VLM_URL); no model
 * weights, no torch, no transformers in this binary. Diskless by doctrine:
 * documents live only in memory for the duration of the RPC.
 */
public final class GrpcEnrichServer {

  /** Default port for the HTTP front end when ENRICH_HTTP_PORT is unset. */
  static final int DEFAULT_HTTP_PORT = EnrichConfig.DEFAULT_HTTP_PORT;

  private GrpcEnrichServer() {}

  public static void main(String[] args) throws Exception {
    EnrichConfig config = EnrichConfig.fromEnv();
    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    EnrichmentEngine engine = new EnrichmentEngine(
        OpenAiCompatVlmClient::new,
        config.vlmUrl(),
        config.maxConcurrentVlm(),
        config.maxConcurrentVlm(),
        config.vlmTimeout(),
        executor);
    EnrichServiceImpl service = new EnrichServiceImpl(
        config.maxDocumentBytes(), engine, executor, config.vlmUrl(), config.maxConcurrentVlm());

    Server server = startGrpcServer(config, service);
    EnrichHttpServer httpServer = startHttpServer(config, service, executor);
    ScheduledExecutorService metrics = startMetrics(config, service);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (metrics != null) {
        metrics.shutdownNow();
      }
      if (httpServer != null) {
        httpServer.close();
      }
      server.shutdown();
      try {
        if (!server.awaitTermination(30, TimeUnit.SECONDS)) {
          server.shutdownNow();
        }
      } catch (InterruptedException interrupt) {
        server.shutdownNow();
      }
      executor.shutdown();
    }, "grpc-enrich-shutdown"));
    server.awaitTermination();
  }

  private static Server startGrpcServer(EnrichConfig config, EnrichServiceImpl service)
      throws IOException {
    HealthStatusManager health = new HealthStatusManager();
    Server server =
        Grpc.newServerBuilderForPort(config.port(), InsecureServerCredentials.create())
            // Allow single-chunk uploads of a full-size document plus framing.
            .maxInboundMessageSize(
                (int) Math.min(Integer.MAX_VALUE, config.maxDocumentBytes() + (1 << 20)))
            .addService(service)
            .addService(health.getHealthService())
            .addService(ProtoReflectionService.newInstance())
            .addService(ProtoReflectionServiceV1.newInstance())
            .build()
            .start();
    System.out.println("grpc-enrich " + EnrichServiceImpl.SERVICE_VERSION
        + " listening on 0.0.0.0:" + config.port() + " (vlm endpoint: "
        + (config.vlmUrl().isEmpty() ? "unconfigured" : config.vlmUrl()) + ", max "
        + (config.maxDocumentBytes() >> 20) + " MiB, " + config.maxConcurrentVlm()
        + " concurrent VLM calls)");
    return server;
  }

  /** Starts the HTTP front end, or returns null when it is disabled. */
  private static EnrichHttpServer startHttpServer(
      EnrichConfig config, EnrichServiceImpl service, ExecutorService executor)
      throws IOException {
    if (config.httpPort() == null) {
      return null;
    }
    EnrichHttpServer httpServer = new EnrichHttpServer(config.httpPort(), service, executor);
    httpServer.start();
    System.out.println("grpc-enrich HTTP front end listening on 0.0.0.0:" + httpServer.getPort()
        + " (POST /v1/enrich, POST /v1/enrich/stream, GET /healthz)");
    return httpServer;
  }

  /** Schedules the periodic metrics line, or returns null when disabled. */
  private static ScheduledExecutorService startMetrics(
      EnrichConfig config, EnrichServiceImpl service) {
    long seconds = config.metricsInterval().toSeconds();
    if (seconds <= 0) {
      return null;
    }
    ScheduledExecutorService metrics = Executors.newSingleThreadScheduledExecutor(task -> {
      Thread thread = new Thread(task, "grpc-enrich-metrics");
      thread.setDaemon(true);
      return thread;
    });
    metrics.scheduleAtFixedRate(
        () -> System.out.println("grpc-enrich metrics: docs{enriched=" + service.enriched.get()
            + ",rejected=" + service.rejected.get() + "}"),
        seconds, seconds, TimeUnit.SECONDS);
    return metrics;
  }
}
