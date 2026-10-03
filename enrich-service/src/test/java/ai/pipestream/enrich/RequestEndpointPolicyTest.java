package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.document.v1.DocItemLabel;
import ai.pipestream.document.v1.Document;
import ai.pipestream.enrich.InProcessEnrich.Collected;
import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.v1.ChartExtractionOptions;
import ai.pipestream.enrich.v1.EnrichDocumentResponse;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.GetServiceInfoRequest;
import ai.pipestream.enrich.v1.GetServiceInfoResponse;
import ai.pipestream.enrich.v1.ItemSkipped;
import ai.pipestream.enrich.v1.SkipReason;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * A per-request VLM endpoint is a server-side request the caller aims:
 * refused unless the operator allows it (any endpoint, allowlisted origins,
 * or the origin of the operator's own endpoint), never given the operator's key, and never a way to read what
 * the target answered. Also covers the redacted default endpoint in
 * GetServiceInfo.
 */
class RequestEndpointPolicyTest {

  private static final String OPERATOR_KEY = "operator-sentinel-key-6f1d";

  private static EnrichOptions describe(Document document) {
    return EnrichOptions.newBuilder()
        .setDoPictureDescription(true)
        .setDocument(document)
        .build();
  }

  private static EndpointPolicy allowAny(String defaultEndpoint) {
    return new EndpointPolicy(defaultEndpoint, "", true, Set.of());
  }

  // -------------------------------------------------------------------------
  // The gate
  // -------------------------------------------------------------------------

  @Test
  void requestEndpoint_refusedByDefault() throws Exception {
    try (FakeVlmServer operator = new FakeVlmServer();
        FakeVlmServer target = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(operator.url()))) {
      Collected result = enrich.run(describe(InProcessEnrich.pictures(1)).toBuilder()
          .setVlmEndpoint(target.url())
          .build());

      assertThat(result.error()).isNotNull();
      assertThat(result.error().getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
      assertThat(result.error().getStatus().getDescription())
          .contains("ENRICH_ALLOW_REQUEST_ENDPOINT");
      assertThat(result.events()).as("refused before any event").isEmpty();
      assertThat(target.calls()).as("the caller's target must never be contacted").isZero();
      assertThat(operator.calls()).isZero();
    }
  }

  @Test
  void requestEndpoint_usedWhenTheOperatorAllowsAny() throws Exception {
    try (FakeVlmServer operator = new FakeVlmServer();
        FakeVlmServer target = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(allowAny(operator.url()))) {
      target.responder = body -> "from the caller's endpoint";
      Collected result = enrich.run(describe(InProcessEnrich.pictures(1)).toBuilder()
          .setVlmEndpoint(target.publicUrl())
          .build());

      assertThat(result.error()).isNull();
      assertThat(result.annotations()).singleElement()
          .satisfies(annotation -> assertThat(annotation.getDescription().getText())
              .isEqualTo("from the caller's endpoint"));
      assertThat(target.calls()).isEqualTo(1);
      assertThat(operator.calls()).isZero();
    }
  }

  @Test
  void requestEndpoint_allowlistAdmitsOnlyItsOrigins() throws Exception {
    try (FakeVlmServer operator = new FakeVlmServer();
        FakeVlmServer listed = new FakeVlmServer();
        FakeVlmServer unlisted = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(
            new EndpointPolicy(operator.url(), "", false, Set.of(listed.url())))) {
      // A full endpoint URL on an allowlisted origin is admitted.
      Collected admitted = enrich.run(describe(InProcessEnrich.pictures(1)).toBuilder()
          .setVlmEndpoint(listed.url() + "/v1/chat/completions")
          .build());
      assertThat(admitted.error()).isNull();
      assertThat(admitted.annotations()).hasSize(1);
      assertThat(listed.calls()).isEqualTo(1);

      Collected refused = enrich.run(describe(InProcessEnrich.pictures(1)).toBuilder()
          .setVlmEndpoint(unlisted.url())
          .build());
      assertThat(refused.error()).isNotNull();
      assertThat(refused.error().getStatus().getCode())
          .isEqualTo(Status.Code.PERMISSION_DENIED);
      assertThat(unlisted.calls()).isZero();
    }
  }

  @Test
  void requestEndpoint_onTheOperatorOrigin_isAllowedByDefault() throws Exception {
    // gRParse sends GRPARSE_ENRICH_VLM_ENDPOINT as vlm_endpoint and a chart
    // preset's url as chart_extraction.vlm_endpoint on every request; when
    // they name ENRICH_VLM_URL's origin they work without an allowlist.
    try (FakeVlmServer operator = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(
            new EndpointPolicy(operator.url() + "/v1", OPERATOR_KEY, false, Set.of()))) {
      Document document = Document.newBuilder()
          .setName("mixed")
          .addPictures(InProcessEnrich.picture("#/pictures/0").toBuilder()
              .setLabel(DocItemLabel.DOC_ITEM_LABEL_CHART))
          .addPictures(InProcessEnrich.picture("#/pictures/1"))
          .build();
      Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDoChartExtraction(true)
          .setVlmEndpoint(operator.url() + "/v1/chat/completions")
          .setChartExtraction(ChartExtractionOptions.newBuilder()
              .setVlmEndpoint(operator.url() + "/"))
          .setDocument(document)
          .build());

      assertThat(result.error()).isNull();
      assertThat(result.annotations()).hasSize(2);
      // Named by the caller, so the key stays off even on the operator's origin.
      assertThat(operator.recorded()).hasSize(2).allSatisfy(request -> {
        assertThat(request.header("Authorization")).isEmpty();
        assertThat(request.headers().toString()).doesNotContain(OPERATOR_KEY);
      });
    }
  }

  @Test
  void requestEndpoint_onlyTheOperatorOriginItself_isAllowedByDefault() throws Exception {
    try (FakeVlmServer operator = new FakeVlmServer();
        FakeVlmServer other = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(operator.url()))) {
      String hostPort = operator.url().substring("http://".length());
      for (String endpoint : List.of(
          other.url(),
          "https://" + hostPort,
          "http://" + hostPort + "@evil.invalid/")) {
        Collected result = enrich.run(describe(InProcessEnrich.pictures(1)).toBuilder()
            .setVlmEndpoint(endpoint)
            .build());
        assertThat(result.error()).as(endpoint).isNotNull();
        assertThat(result.error().getStatus().getCode()).as(endpoint)
            .isEqualTo(Status.Code.PERMISSION_DENIED);
      }
      assertThat(operator.calls()).isZero();
      assertThat(other.calls()).isZero();
    }
  }

  @Test
  void requestEndpoint_userinfoCannotDisguiseTheHost() throws Exception {
    try (FakeVlmServer listed = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(
            new EndpointPolicy("", "", false, Set.of(listed.url())))) {
      // "http://<listed>@evil.invalid/" names evil.invalid as the host.
      String disguised = "http://" + listed.url().substring("http://".length())
          + "@evil.invalid/";
      Collected result = enrich.run(describe(InProcessEnrich.pictures(1)).toBuilder()
          .setVlmEndpoint(disguised)
          .build());

      assertThat(result.error()).isNotNull();
      assertThat(result.error().getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    }
  }

  @Test
  void requestEndpoint_nonHttpScheme_isInvalidArgument() throws Exception {
    try (InProcessEnrich enrich = InProcessEnrich.start(allowAny(""))) {
      for (String endpoint : List.of("file:///etc/passwd", "gopher://127.0.0.1:6379/_INFO",
          "http:///no-host", "not a url")) {
        Collected result = enrich.run(describe(InProcessEnrich.pictures(1)).toBuilder()
            .setVlmEndpoint(endpoint)
            .build());
        assertThat(result.error()).as(endpoint).isNotNull();
        assertThat(result.error().getStatus().getCode()).as(endpoint)
            .isEqualTo(Status.Code.INVALID_ARGUMENT);
      }
    }
  }

  @Test
  void refusedEndpoint_isSkippedByTheEngineEvenWithoutServiceValidation() throws Exception {
    try (FakeVlmServer target = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(""))) {
      List<EnrichDocumentResponse> events = new java.util.ArrayList<>();
      enrich.engine.enrich(InProcessEnrich.pictures(1), Map.of(),
          describe(InProcessEnrich.pictures(1)).toBuilder().setVlmEndpoint(target.url()).build(),
          events::add);

      assertThat(events.stream().filter(EnrichDocumentResponse::hasSkipped)
          .map(EnrichDocumentResponse::getSkipped))
          .singleElement()
          .satisfies(skip -> {
            assertThat(skip.getReason()).isEqualTo(SkipReason.SKIP_REASON_VLM_ERROR);
            assertThat(skip.getDetail()).contains("not allowed");
          });
      assertThat(target.calls()).isZero();
    }
  }

  // -------------------------------------------------------------------------
  // The operator's key
  // -------------------------------------------------------------------------

  @Test
  void operatorKey_goesToTheOperatorEndpointOnly() throws Exception {
    try (FakeVlmServer operator = new FakeVlmServer();
        FakeVlmServer chartModel = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(
            new EndpointPolicy(operator.url(), OPERATOR_KEY, true, Set.of()))) {
      chartModel.responder = body -> "year,sales\n2023,10";
      Document document = Document.newBuilder()
          .setName("mixed")
          .addPictures(InProcessEnrich.picture("#/pictures/0").toBuilder()
              .setLabel(DocItemLabel.DOC_ITEM_LABEL_CHART))
          .addPictures(InProcessEnrich.picture("#/pictures/1"))
          .build();
      Collected result = enrich.run(EnrichOptions.newBuilder()
          .setDoPictureDescription(true)
          .setDoChartExtraction(true)
          .setChartExtraction(ChartExtractionOptions.newBuilder()
              .setVlmEndpoint(chartModel.publicUrl()))
          .setDocument(document)
          .build());

      assertThat(result.error()).isNull();
      assertThat(result.annotations()).hasSize(2);
      assertThat(operator.recorded()).singleElement()
          .satisfies(request -> assertThat(request.header("Authorization"))
              .containsExactly("Bearer " + OPERATOR_KEY));
      assertThat(chartModel.recorded()).singleElement().satisfies(request -> {
        assertThat(request.header("Authorization")).isEmpty();
        assertThat(request.headers().toString()).doesNotContain(OPERATOR_KEY);
        assertThat(request.body()).doesNotContain(OPERATOR_KEY);
      });
    }
  }

  @Test
  void operatorKey_neverFollowsARequestEndpoint() throws Exception {
    try (FakeVlmServer operator = new FakeVlmServer();
        FakeVlmServer target = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(
            new EndpointPolicy(operator.url(), OPERATOR_KEY, true, Set.of()))) {
      Collected result = enrich.run(describe(InProcessEnrich.pictures(2)).toBuilder()
          .setVlmEndpoint(target.publicUrl())
          .build());

      assertThat(result.error()).isNull();
      assertThat(operator.calls()).isZero();
      assertThat(target.recorded()).hasSize(2).allSatisfy(request -> {
        assertThat(request.header("Authorization")).isEmpty();
        assertThat(request.headers().toString()).doesNotContain(OPERATOR_KEY);
      });
    }
  }

  // -------------------------------------------------------------------------
  // No response echo from a caller's endpoint
  // -------------------------------------------------------------------------

  @Test
  void callerEndpointErrorBody_isNeverEchoed() throws Exception {
    try (FakeVlmServer target = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(allowAny(""))) {
      target.status = 404;
      target.errorBody = "<html>INTERNAL-ADMIN-PAGE db_password=hunter2</html>";
      Collected result = enrich.run(describe(InProcessEnrich.pictures(1)).toBuilder()
          .setVlmEndpoint(target.publicUrl())
          .build());

      assertThat(result.error()).isNull();
      ItemSkipped skip = result.skips().get(0);
      assertThat(skip.getReason()).isEqualTo(SkipReason.SKIP_REASON_VLM_ERROR);
      assertThat(skip.getDetail()).contains("HTTP 404")
          .doesNotContain("INTERNAL-ADMIN-PAGE").doesNotContain("hunter2");
    }
  }

  @Test
  void callerEndpointUnparseableReply_isNeverEchoed() throws Exception {
    try (FakeVlmServer target = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(allowAny(""))) {
      target.rawOkBody = "{\"instance-id\":\"i-0SECRET\",\"role\":\"metadata-admin\"}";
      Collected result = enrich.run(describe(InProcessEnrich.pictures(1)).toBuilder()
          .setVlmEndpoint(target.publicUrl())
          .build());

      assertThat(result.skips()).singleElement().satisfies(skip -> {
        assertThat(skip.getReason()).isEqualTo(SkipReason.SKIP_REASON_VLM_ERROR);
        assertThat(skip.getDetail()).doesNotContain("SECRET").doesNotContain("metadata-admin");
      });
    }
  }

  @Test
  void callerEndpointSpeakingAnotherProtocol_isNeverEchoed() throws Exception {
    // A non-HTTP service (here a fake Redis banner) makes the JDK client fail
    // with a message that repeats the bytes it got; none may reach the caller.
    try (ServerSocket banner = new ServerSocket(0);
        InProcessEnrich enrich = InProcessEnrich.start(allowAny(""))) {
      Set<Socket> accepted = ConcurrentHashMap.newKeySet();
      Thread.ofVirtual().start(() -> {
        while (!banner.isClosed()) {
          try {
            Socket socket = banner.accept();
            accepted.add(socket);
            socket.getOutputStream().write(
                "-ERR SECRET-BANNER redis_version:7.2 requirepass=hunter2\r\n\r\n"
                    .getBytes(StandardCharsets.US_ASCII));
            socket.close();
          } catch (java.io.IOException closed) {
            return;
          }
        }
      });
      Collected result = enrich.run(describe(InProcessEnrich.pictures(1)).toBuilder()
          .setVlmEndpoint("http://banner.test:" + banner.getLocalPort())
          .build());

      assertThat(result.skips()).singleElement().satisfies(skip -> {
        assertThat(skip.getReason()).isEqualTo(SkipReason.SKIP_REASON_VLM_ERROR);
        assertThat(skip.getDetail()).doesNotContain("SECRET").doesNotContain("hunter2")
            .doesNotContain("redis");
      });
      assertThat(accepted).isNotEmpty();
    }
  }

  @Test
  void operatorEndpointErrorBody_isStillReported() throws Exception {
    try (FakeVlmServer operator = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(operator.url()))) {
      operator.status = 400;
      operator.errorBody = "{\"error\":{\"message\":\"model smolvlm is not loaded\"}}";
      Collected result = enrich.run(describe(InProcessEnrich.pictures(1)));

      assertThat(result.skips()).singleElement()
          .satisfies(skip -> assertThat(skip.getDetail())
              .contains("HTTP 400").contains("model smolvlm is not loaded"));
    }
  }

  // -------------------------------------------------------------------------
  // HTTP shim: a refused endpoint is 403
  // -------------------------------------------------------------------------

  @Test
  void httpShim_refusedRequestEndpoint_is403() throws Exception {
    try (FakeVlmServer target = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(""))) {
      String body = "{\"options\":{\"doPictureDescription\":true,\"vlmEndpoint\":\""
          + target.url() + "\",\"document\":{\"name\":\"doc\"}}}";
      HttpResponse<String> response = HttpClient.newHttpClient().send(
          HttpRequest.newBuilder(URI.create(enrich.httpBase() + "/v1/enrich"))
              .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
          HttpResponse.BodyHandlers.ofString());

      assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(403);
      assertThat(target.calls()).isZero();
    }
  }

  // -------------------------------------------------------------------------
  // GetServiceInfo reports the default endpoint's origin only
  // -------------------------------------------------------------------------

  @Test
  void getServiceInfo_reportsOnlyTheDefaultEndpointOrigin() throws Exception {
    String configured = "https://svc-user:url-secret@vlm.internal:8443/openai/deployments/d"
        + "/chat/completions?api-version=2024-10-21&api_key=query-secret";
    try (InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(configured))) {
      BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
      enrich.stub.getServiceInfo(GetServiceInfoRequest.getDefaultInstance(),
          new StreamObserver<>() {
            @Override
            public void onNext(GetServiceInfoResponse response) {
              inbox.add(response);
            }

            @Override
            public void onError(Throwable error) {
              inbox.add(error);
            }

            @Override
            public void onCompleted() {
            }
          });
      Object reply = inbox.poll(10, TimeUnit.SECONDS);

      assertThat(reply).isInstanceOf(GetServiceInfoResponse.class);
      assertThat(((GetServiceInfoResponse) reply).getDefaultVlmEndpoint())
          .isEqualTo("https://vlm.internal:8443");
    }
  }
}
