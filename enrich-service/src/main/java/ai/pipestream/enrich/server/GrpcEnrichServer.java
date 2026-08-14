package ai.pipestream.enrich.server;

import ai.pipestream.enrich.engine.EnrichmentEngine;
import ai.pipestream.enrich.vlm.OpenAiCompatVlmClient;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * grpc-enrich: Document in, live item-annotation stream out. The VLM is a
 * remote server this process calls over HTTP (ENRICH_VLM_URL); no model
 * weights, no torch, no transformers in this binary. Diskless by doctrine:
 * documents live only in memory for the duration of the RPC.
 */
public final class GrpcEnrichServer {

  public static void main(String[] args) throws Exception {
    final int port = intFromEnv("ENRICH_PORT", 50056, 1, 65535);
    final String vlmUrl = System.getenv().getOrDefault("ENRICH_VLM_URL", "");
    final long maxDocumentBytes =
        intFromEnv("ENRICH_MAX_DOCUMENT_MIB", 70, 1, 4096) * 1024L * 1024L;
    final int cores = Runtime.getRuntime().availableProcessors();
    final int maxConcurrentVlm =
        intFromEnv("ENRICH_MAX_CONCURRENT_VLM", Math.max(2, cores), 1, 256);
    final int vlmTimeoutSeconds = intFromEnv("ENRICH_VLM_TIMEOUT_SECONDS", 300, 1, 86400);
    final int metricsInterval = intFromEnv("ENRICH_METRICS_INTERVAL_SECONDS", 60, 0, 86400);

    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    EnrichmentEngine engine =
        new EnrichmentEngine(OpenAiCompatVlmClient::new, vlmUrl, maxConcurrentVlm,
            maxConcurrentVlm, Duration.ofSeconds(vlmTimeoutSeconds), executor);
    EnrichServiceImpl service =
        new EnrichServiceImpl(maxDocumentBytes, engine, executor, vlmUrl, maxConcurrentVlm);
    HealthStatusManager health = new HealthStatusManager();
    Server server =
        Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
            // Allow single-chunk uploads of a full-size document plus framing.
            .maxInboundMessageSize((int) Math.min(Integer.MAX_VALUE, maxDocumentBytes + (1 << 20)))
            .addService(service)
            .addService(health.getHealthService())
            .addService(ProtoReflectionService.newInstance())
            .addService(ProtoReflectionServiceV1.newInstance())
            .build()
            .start();
    System.out.println("grpc-enrich " + EnrichServiceImpl.SERVICE_VERSION
        + " listening on 0.0.0.0:" + port + " (vlm endpoint: "
        + (vlmUrl.isEmpty() ? "unconfigured" : vlmUrl) + ", max "
        + (maxDocumentBytes >> 20) + " MiB, " + maxConcurrentVlm + " concurrent VLM calls)");

    if (metricsInterval > 0) {
      Thread metrics = new Thread(() -> {
        while (true) {
          try {
            TimeUnit.SECONDS.sleep(metricsInterval);
          } catch (InterruptedException interrupt) {
            return;
          }
          System.out.println("grpc-enrich metrics: docs{enriched=" + service.enriched.get()
              + ",rejected=" + service.rejected.get() + "}");
        }
      }, "grpc-enrich-metrics");
      metrics.setDaemon(true);
      metrics.start();
    }

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
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

  private static int intFromEnv(String name, int fallback, int min, int max) {
    String configured = System.getenv(name);
    if (configured == null || configured.isBlank()) {
      return fallback;
    }
    final int value;
    try {
      value = Integer.parseInt(configured.strip());
    } catch (NumberFormatException bad) {
      throw new IllegalArgumentException(name + " must be an integer, got: " + configured);
    }
    if (value < min || value > max) {
      throw new IllegalArgumentException(name + " must be in [" + min + ", " + max + "], got: "
          + value);
    }
    return value;
  }
}
