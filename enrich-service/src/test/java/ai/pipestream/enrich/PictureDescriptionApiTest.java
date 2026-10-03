package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.document.v1.BaseTextItem;
import ai.pipestream.document.v1.CodeItem;
import ai.pipestream.document.v1.DocItemLabel;
import ai.pipestream.document.v1.Document;
import ai.pipestream.enrich.InProcessEnrich.Collected;
import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.v1.ChartExtractionOptions;
import ai.pipestream.enrich.v1.EnrichDocumentResponse;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.VlmGenerationParams;
import ai.pipestream.enrich.v1.VlmHeader;
import ai.pipestream.enrich.vlm.Json;
import ai.pipestream.enrich.vlm.VlmClient;
import com.google.protobuf.DebugFormat;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Docling picture_description_api parity: the prompt override, the typed
 * generation parameters (model, max_tokens, temperature, top_p, seed), and
 * caller headers, which go only to the endpoint the caller named and never
 * show up in events, errors, or output.
 */
class PictureDescriptionApiTest {

  private static final String CALLER_TOKEN = "caller-sentinel-token-93ab";

  private static EnrichOptions.Builder describe(int pictures) {
    return EnrichOptions.newBuilder()
        .setDoPictureDescription(true)
        .setDocument(InProcessEnrich.pictures(pictures));
  }

  private static EndpointPolicy allowAny(String defaultEndpoint) {
    return new EndpointPolicy(defaultEndpoint, "", true, Set.of());
  }

  private static VlmHeader header(String name, String value) {
    return VlmHeader.newBuilder().setName(name).setValue(value).build();
  }

  private static Map<String, Object> body(FakeVlmServer.RecordedRequest request) {
    return Json.asObject(Json.parse(request.body()));
  }

  // -------------------------------------------------------------------------
  // Prompt
  // -------------------------------------------------------------------------

  @Test
  void prompt_replacesThePresetPromptForDescriptionsOnly() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      Document document = InProcessEnrich.pictures(1).toBuilder()
          .addTexts(BaseTextItem.newBuilder().setCode(CodeItem.newBuilder()
              .setSelfRef("#/texts/0")
              .setLabel(DocItemLabel.DOC_ITEM_LABEL_CODE)
              .setText("print( 1 )")))
          .build();
      Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDoCodeEnrichment(true)
          .setPictureDescriptionPrompt("Describe this figure for a screen reader.")
          .setDocument(document)
          .build());

      assertThat(result.error()).isNull();
      assertThat(vlm.recorded()).extracting(FakeVlmServer.RecordedRequest::prompt)
          .containsExactlyInAnyOrder("Describe this figure for a screen reader.",
              "Transcribe and normalize the following code block. Reply with the code only."
                  + "\n\nprint( 1 )");
    }
  }

  // -------------------------------------------------------------------------
  // Typed generation parameters
  // -------------------------------------------------------------------------

  @Test
  void params_areSentAsTypedJsonFields() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      Collected result = enrich.run(describe(1)
          .setPictureDescriptionParams(VlmGenerationParams.newBuilder()
              .setModel("gpt-4o-mini")
              .setMaxTokens(321)
              .setTemperature(0.25)
              .setTopP(0.9)
              .setSeed(42))
          .build());

      assertThat(result.error()).isNull();
      Map<String, Object> sent = body(vlm.recorded().get(0));
      assertThat(sent.get("model")).isEqualTo("gpt-4o-mini");
      assertThat(sent.get("max_tokens")).isEqualTo(321.0);
      assertThat(sent.get("temperature")).isEqualTo(0.25);
      assertThat(sent.get("top_p")).isEqualTo(0.9);
      assertThat(sent.get("seed")).isEqualTo(42.0);
      // The messages array is ours; params cannot replace it.
      assertThat(Json.asArray(sent.get("messages"))).hasSize(1);
      assertThat(result.annotations()).singleElement()
          .satisfies(annotation -> assertThat(annotation.getModel()).isEqualTo("gpt-4o-mini"));
    }
  }

  @Test
  void params_unsetFieldsAreNotSent() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      Collected result = enrich.run(describe(1)
          .setPictureDescriptionPresetRaw("smolvlm-raw")
          .setPictureDescriptionParams(VlmGenerationParams.newBuilder().setTemperature(0.0))
          .build());

      assertThat(result.error()).isNull();
      Map<String, Object> sent = body(vlm.recorded().get(0));
      assertThat(sent).containsEntry("model", "smolvlm-raw")
          .containsEntry("max_tokens", 200.0)
          .containsEntry("temperature", 0.0)
          .doesNotContainKeys("top_p", "seed");
    }
  }

  @Test
  void params_doNotApplyToOtherJobs() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.responder = body -> "year,sales\n2023,10";
      Document document = Document.newBuilder().setName("chart")
          .addPictures(InProcessEnrich.picture("#/pictures/0").toBuilder()
              .setLabel(DocItemLabel.DOC_ITEM_LABEL_CHART))
          .build();
      Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoChartExtraction(true)
          .setPictureDescriptionPrompt("ignored by charts")
          .setPictureDescriptionParams(VlmGenerationParams.newBuilder()
              .setModel("description-model").setMaxTokens(7).setTemperature(1.5))
          .setDocument(document)
          .build());

      assertThat(result.error()).isNull();
      Map<String, Object> sent = body(vlm.recorded().get(0));
      assertThat(sent).containsEntry("max_tokens", 4096.0).doesNotContainKeys("temperature");
      assertThat(sent.get("model")).isNotEqualTo("description-model");
      assertThat(vlm.recorded().get(0).prompt()).isNotEqualTo("ignored by charts");
    }
  }

  @Test
  void invalidParams_areInvalidArgument() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      for (VlmGenerationParams params : List.of(
          VlmGenerationParams.newBuilder().setMaxTokens(0).build(),
          VlmGenerationParams.newBuilder().setTemperature(Double.NaN).build(),
          VlmGenerationParams.newBuilder().setTemperature(-0.5).build(),
          VlmGenerationParams.newBuilder().setTemperature(Double.POSITIVE_INFINITY).build(),
          VlmGenerationParams.newBuilder().setTopP(1.5).build())) {
        Collected result = enrich.run(describe(1).setPictureDescriptionParams(params).build());
        assertThat(result.error()).as("%s", params).isNotNull();
        assertThat(result.error().getStatus().getCode()).as("%s", params)
            .isEqualTo(Status.Code.INVALID_ARGUMENT);
      }
      assertThat(vlm.calls()).isZero();
    }
  }

  // -------------------------------------------------------------------------
  // Headers
  // -------------------------------------------------------------------------

  @Test
  void headers_goToTheRequestEndpointOnly() throws Exception {
    try (FakeVlmServer operator = new FakeVlmServer();
        FakeVlmServer chartModel = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(allowAny(operator.url()))) {
      chartModel.responder = body -> "year,sales\n2023,10";
      Document document = Document.newBuilder().setName("mixed")
          .addPictures(InProcessEnrich.picture("#/pictures/0").toBuilder()
              .setLabel(DocItemLabel.DOC_ITEM_LABEL_CHART))
          .addPictures(InProcessEnrich.picture("#/pictures/1"))
          .build();
      Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDoChartExtraction(true)
          .setChartExtraction(ChartExtractionOptions.newBuilder()
              .setVlmEndpoint(chartModel.publicUrl()))
          .addVlmHeaders(header("Authorization", "Bearer " + CALLER_TOKEN))
          .addVlmHeaders(header("X-Tenant", "acme"))
          .setDocument(document)
          .build());

      assertThat(result.error()).isNull();
      assertThat(chartModel.recorded()).singleElement().satisfies(request -> {
        assertThat(request.header("Authorization")).containsExactly("Bearer " + CALLER_TOKEN);
        assertThat(request.header("X-Tenant")).containsExactly("acme");
      });
      assertThat(operator.recorded()).singleElement().satisfies(request -> {
        assertThat(request.header("Authorization")).isEmpty();
        assertThat(request.header("X-Tenant")).isEmpty();
      });
    }
  }

  @Test
  void headers_withoutARequestEndpoint_areInvalidArgument() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(allowAny(vlm.url()))) {
      Collected result = enrich.run(describe(1)
          .addVlmHeaders(header("Authorization", "Bearer " + CALLER_TOKEN))
          .build());

      assertThat(result.error()).isNotNull();
      assertThat(result.error().getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(result.error().getStatus().getDescription()).doesNotContain(CALLER_TOKEN);
      assertThat(vlm.calls()).isZero();
    }
  }

  @Test
  void reservedOrMalformedHeaders_areInvalidArgumentWithoutTheValue() throws Exception {
    try (FakeVlmServer target = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(allowAny(""))) {
      List<VlmHeader> refused = List.of(
          header("Host", CALLER_TOKEN),
          header("content-length", CALLER_TOKEN),
          header("Content-Type", CALLER_TOKEN),
          header("Transfer-Encoding", CALLER_TOKEN),
          header("Connection", CALLER_TOKEN),
          header("Proxy-Authorization", CALLER_TOKEN),
          header("Bad Name", CALLER_TOKEN),
          header("", CALLER_TOKEN),
          header("X-Injected", CALLER_TOKEN + "\r\nX-Evil: 1"),
          header("X-Nul", CALLER_TOKEN + "\u0000"),
          header("X-Wide", CALLER_TOKEN + "Ā"),
          header("X-Huge", CALLER_TOKEN + "x".repeat(9000)));
      for (VlmHeader bad : refused) {
        Collected result = enrich.run(describe(1)
            .setVlmEndpoint(target.publicUrl())
            .addVlmHeaders(bad)
            .build());
        assertThat(result.error()).as(bad.getName()).isNotNull();
        assertThat(result.error().getStatus().getCode()).as(bad.getName())
            .isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(result.error().getStatus().getDescription()).as(bad.getName())
            .doesNotContain(CALLER_TOKEN);
      }
      assertThat(target.calls()).isZero();
    }
  }

  @Test
  void tooManyHeaders_areInvalidArgument() throws Exception {
    try (FakeVlmServer target = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(allowAny(""))) {
      EnrichOptions.Builder options = describe(1).setVlmEndpoint(target.publicUrl());
      for (int i = 0; i < 33; i++) {
        options.addVlmHeaders(header("X-Header-" + i, "v"));
      }
      Collected result = enrich.run(options.build());

      assertThat(result.error()).isNotNull();
      assertThat(result.error().getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(target.calls()).isZero();
    }
  }

  @Test
  void headerValue_neverAppearsInEventsOrOutput() throws Exception {
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    PrintStream originalErr = System.err;
    List<EnrichDocumentResponse> events = new ArrayList<>();
    try (FakeVlmServer target = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(allowAny(""))) {
      // The first call fails with a 404 whose body repeats the token (not
      // retried); the other two succeed.
      target.statusForCall = call -> call == 1 ? 404 : 200;
      target.errorBody = "{\"error\":\"echo " + CALLER_TOKEN + "\"}";
      PrintStream capture = new PrintStream(captured, true, StandardCharsets.UTF_8);
      System.setOut(capture);
      System.setErr(capture);
      Collected result = enrich.run(describe(3)
          .setVlmEndpoint(target.publicUrl())
          .setConcurrency(1)
          .addVlmHeaders(header("Authorization", "Bearer " + CALLER_TOKEN))
          .build());
      events.addAll(result.events());

      assertThat(target.recorded()).hasSize(3).allSatisfy(request ->
          assertThat(request.header("Authorization")).containsExactly("Bearer " + CALLER_TOKEN));
      assertThat(result.skips()).isNotEmpty();
    } finally {
      System.setOut(originalOut);
      System.setErr(originalErr);
    }
    for (EnrichDocumentResponse event : events) {
      assertThat(JsonFormat.printer().print(event)).doesNotContain(CALLER_TOKEN);
      assertThat(event.toString()).doesNotContain(CALLER_TOKEN);
    }
    assertThat(captured.toString(StandardCharsets.UTF_8)).doesNotContain(CALLER_TOKEN);
  }

  @Test
  void headerValue_isRedactedByDebugPrinters() {
    EnrichOptions options = describe(1)
        .setVlmEndpoint("http://vlm.example:8080")
        .addVlmHeaders(header("Authorization", "Bearer " + CALLER_TOKEN))
        .build();

    assertThat(DebugFormat.singleLine().toString(options))
        .contains("Authorization").contains("[REDACTED]").doesNotContain(CALLER_TOKEN);
    assertThat(new VlmClient.Header("Authorization", "Bearer " + CALLER_TOKEN).toString())
        .doesNotContain(CALLER_TOKEN);
    assertThat(new EndpointPolicy("http://vlm:8080", CALLER_TOKEN, false, Set.of()).toString())
        .doesNotContain(CALLER_TOKEN);
  }

  @Test
  void httpShim_carriesTypedFieldsAndHeaders() throws Exception {
    try (FakeVlmServer target = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(allowAny(""))) {
      target.responder = body -> "described over HTTP";
      String body = "{\"options\":{\"doPictureDescription\":true,"
          + "\"vlmEndpoint\":\"" + target.publicUrl() + "/v1/chat/completions\","
          + "\"pictureDescriptionPrompt\":\"What is this?\","
          + "\"pictureDescriptionParams\":{\"model\":\"m\",\"maxTokens\":64,\"seed\":\"7\"},"
          + "\"vlmHeaders\":[{\"name\":\"Authorization\",\"value\":\"Bearer " + CALLER_TOKEN
          + "\"}],"
          + "\"document\":" + JsonFormat.printer().print(InProcessEnrich.pictures(1)) + "}}";
      HttpResponse<String> response = HttpClient.newHttpClient().send(
          HttpRequest.newBuilder(URI.create(enrich.httpBase() + "/v1/enrich"))
              .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
          HttpResponse.BodyHandlers.ofString());

      assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(200);
      assertThat(response.body()).contains("described over HTTP").doesNotContain(CALLER_TOKEN);
      assertThat(target.recorded()).singleElement().satisfies(request -> {
        assertThat(request.prompt()).isEqualTo("What is this?");
        assertThat(request.header("Authorization")).containsExactly("Bearer " + CALLER_TOKEN);
        assertThat(body(request)).containsEntry("model", "m").containsEntry("max_tokens", 64.0)
            .containsEntry("seed", 7.0);
      });
    }
  }
}
