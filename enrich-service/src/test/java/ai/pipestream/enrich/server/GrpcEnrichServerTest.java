package ai.pipestream.enrich.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * The HTTP front end's unset-env default must stay a port grpc-enrich alone
 * owns (see the workspace port table in grpc-services/AGENTS.md): 50056 is
 * the gRPC port, 50068 is the HTTP port, neither collides with a sibling
 * service's default.
 */
class GrpcEnrichServerTest {

  @Test
  void defaultHttpPortDoesNotCollideWithGrpcPort() {
    assertEquals(50068, GrpcEnrichServer.DEFAULT_HTTP_PORT);
  }
}
