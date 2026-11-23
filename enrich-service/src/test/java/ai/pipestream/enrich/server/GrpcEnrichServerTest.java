package ai.pipestream.enrich.server;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The HTTP front end's reserved port must stay one grpc-enrich alone owns
 * (see the workspace port table in grpc-services/AGENTS.md): 50056 is the
 * gRPC port, 50068 is the HTTP port an operator sets to turn the shim on,
 * neither collides with a sibling service's default.
 */
class GrpcEnrichServerTest {

  @Test
  void reservedHttpPortDoesNotCollideWithGrpcPort() {
    assertThat(GrpcEnrichServer.RESERVED_HTTP_PORT).isEqualTo(50068);
  }

  @Test
  void startupSaysTheHttpFrontEndIsOff() {
    assertThat(GrpcEnrichServer.httpLine(null))
        .contains("disabled")
        .contains("ENRICH_HTTP_PORT=50068");
    assertThat(GrpcEnrichServer.httpLine(50068)).contains("0.0.0.0:50068");
  }
}
