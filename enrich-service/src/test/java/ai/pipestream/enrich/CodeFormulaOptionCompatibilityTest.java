package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.v1.CodeFormulaPreset;
import ai.pipestream.enrich.v1.EnrichOptions;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

/** Newly understood wire options must fail explicitly rather than disappear. */
class CodeFormulaOptionCompatibilityTest {
  @Test
  void unsupportedFeaturesAreRejectedBeforeAnyWork() throws Exception {
    try (InProcessEnrich enrich = InProcessEnrich.start(
        EndpointPolicy.defaultOnly("http://unused.test:8080"))) {
      for (EnrichOptions options : new EnrichOptions[] {
          EnrichOptions.newBuilder().setCodeFormulaExpansionFactor(0.18).build(),
          EnrichOptions.newBuilder().setCodeFormulaStopOnRepetition(true).build(),
          EnrichOptions.newBuilder()
              .setCodeFormulaPreset(CodeFormulaPreset.CODE_FORMULA_PRESET_GRANITE_DOCLING).build()
      }) {
        var result = enrich.run(options);
        assertThat(Status.fromThrowable(result.error()).getCode())
            .isEqualTo(Status.Code.UNIMPLEMENTED);
        assertThat(result.events()).isEmpty();
      }
    }
  }

  @Test
  void unsupportedHttpOptionsReturn501() throws Exception {
    try (InProcessEnrich enrich = InProcessEnrich.start(
        EndpointPolicy.defaultOnly("http://unused.test:8080"))) {
      var request = java.net.http.HttpRequest.newBuilder(
          java.net.URI.create(enrich.httpBase() + "/v1/enrich"))
          .header("Content-Type", "application/json")
          .POST(java.net.http.HttpRequest.BodyPublishers.ofString(
              "{\"options\":{\"codeFormulaStopOnRepetition\":true}}"))
          .build();
      var response = java.net.http.HttpClient.newHttpClient().send(request,
          java.net.http.HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).isEqualTo(501);
    }
  }

  @Test
  void invalidExpansionIsInvalidArgumentRatherThanUnsupported() throws Exception {
    try (InProcessEnrich enrich = InProcessEnrich.start(
        EndpointPolicy.defaultOnly("http://unused.test:8080"))) {
      for (double factor : new double[] {-0.1, Double.NaN, Double.POSITIVE_INFINITY}) {
        var result = enrich.run(EnrichOptions.newBuilder()
            .setCodeFormulaExpansionFactor(factor).build());
        assertThat(Status.fromThrowable(result.error()).getCode())
            .isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(result.events()).isEmpty();
      }
    }
  }

  @Test
  void absentOptionsAndExplicitNoOpsKeepExistingBehavior() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      for (EnrichOptions.Builder options : new EnrichOptions.Builder[] {
          EnrichOptions.newBuilder(),
          EnrichOptions.newBuilder().setCodeFormulaExpansionFactor(0.0)
              .setCodeFormulaStopOnRepetition(false)
      }) {
        var result = enrich.run(options.setDocument(InProcessEnrich.pictures(1))
            .setDoPictureDescription(true).build());
        assertThat(result.error()).isNull();
        assertThat(result.annotations()).hasSize(1);
      }
      assertThat(vlm.recorded()).hasSize(2);
    }
  }
}
