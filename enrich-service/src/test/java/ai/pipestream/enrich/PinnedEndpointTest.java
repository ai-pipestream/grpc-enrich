package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.enrich.InProcessEnrich.Collected;
import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.engine.EnrichmentEngine;
import ai.pipestream.enrich.v1.EnrichDocumentResponse;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.SkipReason;
import ai.pipestream.enrich.vlm.OpenAiCompatVlmClient;
import ai.pipestream.enrich.vlm.PublicAddress;
import ai.pipestream.enrich.vlm.VlmClient;
import ai.pipestream.enrich.vlm.VlmClient.VlmException;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsServer;
import io.grpc.Status;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ENRICH_ALLOW_REQUEST_ENDPOINT=true lets a caller name any endpoint, but
 * not one on a loopback, private, link-local, or cloud-metadata address:
 * the host is resolved once, every address is checked, and the calls are
 * pinned to the checked address (so a DNS answer that changes afterwards,
 * DNS rebinding, changes nothing) while the host name still goes out as the
 * Host header and the TLS server name the certificate is checked against.
 */
class PinnedEndpointTest {

  private static EndpointPolicy allowAny(String defaultEndpoint) {
    return new EndpointPolicy(defaultEndpoint, "", true, Set.of());
  }

  private static EnrichOptions describe(int pictures, String endpoint) {
    return EnrichOptions.newBuilder()
        .setDoPictureDescription(true)
        .setDocument(InProcessEnrich.pictures(pictures))
        .setVlmEndpoint(endpoint)
        .build();
  }

  // -------------------------------------------------------------------------
  // Refused addresses
  // -------------------------------------------------------------------------

  @Test
  void nonPublicLiteral_isPermissionDeniedBeforeAnyCall() throws Exception {
    try (FakeVlmServer target = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(allowAny(""))) {
      int port = Integer.parseInt(target.url().substring(target.url().lastIndexOf(':') + 1));
      for (String endpoint : List.of(target.url(), "http://[::1]:" + port,
          "http://169.254.169.254/latest/meta-data", "http://[fd00:ec2::254]/",
          "http://[::ffff:127.0.0.1]:" + port, "http://10.0.0.1:8080",
          // Other spellings of loopback and metadata the JDK reads as literals.
          "http://2130706433:" + port, "http://0:" + port, "http://[::ffff:7f00:1]:" + port,
          "http://[::ffff:a9fe:a9fe]/", "http://[64:ff9b::a9fe:a9fe]/")) {
        Collected result = enrich.run(describe(1, endpoint));

        assertThat(result.error()).as(endpoint).isNotNull();
        assertThat(result.error().getStatus().getCode()).as(endpoint)
            .isEqualTo(Status.Code.PERMISSION_DENIED);
        assertThat(result.error().getStatus().getDescription()).contains("non-public");
        assertThat(result.events()).isEmpty();
      }
      assertThat(target.calls()).isZero();
    }
  }

  @Test
  void hostResolvingToAPrivateAddress_isSkippedWithoutACall() {
    PublicAddress.Resolver internal = host -> new InetAddress[] {
        InetAddress.ofLiteral("93.184.215.14"), InetAddress.ofLiteral("169.254.169.254")};
    RecordingFactory clients = new RecordingFactory();
    List<EnrichDocumentResponse> events =
        enrich(allowAny(""), internal, clients, describe(2, "http://metadata.example/v1"));

    assertThat(events.stream().filter(EnrichDocumentResponse::hasSkipped).toList())
        .hasSize(2)
        .allSatisfy(event -> {
          assertThat(event.getSkipped().getReason())
              .isEqualTo(SkipReason.SKIP_REASON_ENDPOINT_REFUSED);
          assertThat(event.getSkipped().getDetail()).contains("non-public")
              .contains("ENRICH_VLM_ENDPOINT_ALLOWLIST")
              .doesNotContain("169.254");
        });
    assertThat(clients.pinned).isEmpty();
    assertThat(clients.unpinned).isEmpty();
    assertThat(clients.calls).hasValue(0);
  }

  @Test
  void unresolvableHost_isSkippedWithoutACall() {
    PublicAddress.Resolver none = host -> {
      throw new java.net.UnknownHostException(host);
    };
    RecordingFactory clients = new RecordingFactory();
    List<EnrichDocumentResponse> events =
        enrich(allowAny(""), none, clients, describe(1, "http://nowhere.example"));

    assertThat(events.stream().filter(EnrichDocumentResponse::hasSkipped).toList())
        .singleElement()
        .satisfies(event -> {
          // Unreachable rather than refused by policy.
          assertThat(event.getSkipped().getReason()).isEqualTo(SkipReason.SKIP_REASON_VLM_ERROR);
          assertThat(event.getSkipped().getDetail()).contains("does not resolve");
        });
    assertThat(clients.calls).hasValue(0);
  }

  // -------------------------------------------------------------------------
  // Resolve once, pin, no rebinding
  // -------------------------------------------------------------------------

  @Test
  void hostIsResolvedOnce_andEveryCallUsesTheCheckedAddress() {
    // A rebinding DNS server: public on the first answer, loopback after.
    AtomicInteger lookups = new AtomicInteger();
    PublicAddress.Resolver rebinding = host -> new InetAddress[] {
        lookups.getAndIncrement() == 0
            ? InetAddress.ofLiteral("93.184.215.14")
            : InetAddress.getLoopbackAddress()};
    RecordingFactory clients = new RecordingFactory();
    List<EnrichDocumentResponse> events =
        enrich(allowAny(""), rebinding, clients, describe(3, "http://rebind.example:8080"));

    assertThat(events.stream().filter(EnrichDocumentResponse::hasAnnotation)).hasSize(3);
    assertThat(lookups).hasValue(1);
    assertThat(clients.pinned).containsExactly(InetAddress.ofLiteral("93.184.215.14"));
    assertThat(clients.unpinned).isEmpty();
    assertThat(clients.calls).hasValue(3);
    assertThat(clients.closed).hasValue(1);
  }

  @Test
  void operatorNamedOrigins_areNotChecked() {
    // The operator's own origin and its allowlist may be cluster-internal.
    AtomicInteger lookups = new AtomicInteger();
    PublicAddress.Resolver counting = host -> {
      lookups.incrementAndGet();
      return new InetAddress[] {InetAddress.getLoopbackAddress()};
    };
    RecordingFactory clients = new RecordingFactory();
    EndpointPolicy policy = new EndpointPolicy("http://vlm:8080", "", true,
        Set.of("http://chart-model:8087"));
    enrich(policy, counting, clients, describe(1, "http://vlm:8080/v1/chat/completions"));
    enrich(policy, counting, clients, describe(1, "http://chart-model:8087"));

    assertThat(lookups).hasValue(0);
    assertThat(clients.pinned).isEmpty();
    assertThat(clients.unpinned).hasSize(2);
  }

  @Test
  void pinnedClient_connectsToTheAddressAndSendsTheHostName() throws Exception {
    // .invalid never resolves, so a reply proves the name was not looked up.
    try (FakeVlmServer vlm = new FakeVlmServer()) {
      int port = Integer.parseInt(vlm.url().substring(vlm.url().lastIndexOf(':') + 1));
      VlmClient client = OpenAiCompatVlmClient.pinned("http://vlm.invalid:" + port,
          InetAddress.getLoopbackAddress(), Duration.ofMillis(5), SSLContext.getDefault());
      try {
        assertThat(client.complete("m", "describe", null, 10, Duration.ofSeconds(10)))
            .isEqualTo("fake description");
      } finally {
        client.close();
      }
      assertThat(vlm.recorded()).singleElement().satisfies(request ->
          assertThat(request.header("Host")).containsExactly("vlm.invalid:" + port));
    }
  }

  @Test
  void redirects_areNotFollowed() throws Exception {
    // A public endpoint answering 302 to an unchecked address must not move
    // the call there, pinned or not.
    try (FakeVlmServer target = new FakeVlmServer()) {
      HttpServer bouncer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      bouncer.createContext("/", exchange -> {
        exchange.getRequestBody().readAllBytes();
        exchange.getResponseHeaders().add("Location", target.url() + "/v1/chat/completions");
        exchange.sendResponseHeaders(302, -1);
        exchange.close();
      });
      bouncer.start();
      try {
        int port = bouncer.getAddress().getPort();
        List<VlmClient> clients = List.of(
            OpenAiCompatVlmClient.pinned("http://bounce.invalid:" + port,
                InetAddress.getLoopbackAddress(), Duration.ofMillis(1), SSLContext.getDefault()),
            new OpenAiCompatVlmClient("http://127.0.0.1:" + port, Duration.ofMillis(1)));
        for (VlmClient client : clients) {
          try {
            assertThatThrownBy(() -> client.complete("m", "describe", null, 10,
                Duration.ofSeconds(10)))
                .isInstanceOf(VlmException.class);
          } finally {
            client.close();
          }
        }
      } finally {
        bouncer.stop(0);
      }
      assertThat(target.calls()).isZero();
    }
  }

  // -------------------------------------------------------------------------
  // TLS: server name and certificate check use the host name
  // -------------------------------------------------------------------------

  @Test
  void pinnedTls_sendsTheServerNameAndChecksTheCertificateAgainstIt(@TempDir Path keys)
      throws Exception {
    KeyStore serverKeys = selfSigned(keys, "vlm.invalid");
    try (TlsVlm vlm = new TlsVlm(serverKeys)) {
      VlmClient client = OpenAiCompatVlmClient.pinned(
          "https://vlm.invalid:" + vlm.port() + "/v1", InetAddress.getLoopbackAddress(),
          Duration.ofMillis(5), trusting(serverKeys));
      try {
        assertThat(client.complete("m", "describe", null, 10, Duration.ofSeconds(10)))
            .isEqualTo("tls description");
      } finally {
        client.close();
      }
      assertThat(vlm.serverNames).singleElement()
          .isEqualTo(List.of(new SNIHostName("vlm.invalid")));
      assertThat(vlm.hosts).containsExactly("vlm.invalid:" + vlm.port());
    }
  }

  @Test
  void pinnedTls_refusesACertificateForAnotherName(@TempDir Path keys) throws Exception {
    KeyStore serverKeys = selfSigned(keys, "other.invalid");
    try (TlsVlm vlm = new TlsVlm(serverKeys)) {
      VlmClient client = OpenAiCompatVlmClient.pinned(
          "https://vlm.invalid:" + vlm.port() + "/v1", InetAddress.getLoopbackAddress(),
          Duration.ofMillis(1), trusting(serverKeys));
      try {
        assertThatThrownBy(() -> client.complete("m", "describe", null, 10,
            Duration.ofSeconds(10)))
            .isInstanceOf(VlmException.class);
      } finally {
        client.close();
      }
      assertThat(vlm.hosts).as("no request may cross a failed certificate check").isEmpty();
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static List<EnrichDocumentResponse> enrich(EndpointPolicy policy,
      PublicAddress.Resolver resolver, VlmClient.Factory clients, EnrichOptions options) {
    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      EnrichmentEngine engine = new EnrichmentEngine(clients, policy, 4, 4,
          Duration.ofSeconds(10), executor, resolver);
      assertThat(engine.validate(options).isOk()).isTrue();
      List<EnrichDocumentResponse> events = new CopyOnWriteArrayList<>();
      engine.enrich(options.getDocument(), Map.of(), options, events::add);
      return events;
    }
  }

  /** Records which clients were built, how many calls they ran, and closes. */
  private static final class RecordingFactory implements VlmClient.Factory {
    final ConcurrentLinkedQueue<String> unpinned = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<InetAddress> pinned = new ConcurrentLinkedQueue<>();
    final AtomicInteger calls = new AtomicInteger();
    final AtomicInteger closed = new AtomicInteger();

    @Override
    public VlmClient create(String endpoint) {
      unpinned.add(endpoint);
      return client();
    }

    @Override
    public VlmClient createPinned(String endpoint, InetAddress address) {
      pinned.add(address);
      return client();
    }

    private VlmClient client() {
      return new VlmClient() {
        @Override
        public String complete(VlmRequest request) {
          calls.incrementAndGet();
          return "described";
        }

        @Override
        public void close() {
          closed.incrementAndGet();
        }
      };
    }
  }

  /** A key pair and self-signed certificate for {@code name}, made by keytool. */
  private static KeyStore selfSigned(Path dir, String name) throws Exception {
    Path store = dir.resolve(name + ".p12");
    Process keytool = new ProcessBuilder(
        Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
        "-genkeypair", "-alias", "vlm", "-keyalg", "EC", "-groupname", "secp256r1",
        "-dname", "CN=" + name, "-ext", "SAN=dns:" + name, "-validity", "2",
        "-keystore", store.toString(), "-storetype", "PKCS12",
        "-storepass", "changeit", "-keypass", "changeit")
        .redirectErrorStream(true)
        .start();
    String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(keytool.waitFor(60, TimeUnit.SECONDS)).isTrue();
    assertThat(keytool.exitValue()).as(output).isZero();
    KeyStore keys = KeyStore.getInstance("PKCS12");
    try (InputStream in = new FileInputStream(store.toFile())) {
      keys.load(in, "changeit".toCharArray());
    }
    return keys;
  }

  /** A client TLS context that trusts only {@code serverKeys}' certificate. */
  private static SSLContext trusting(KeyStore serverKeys) throws Exception {
    Certificate certificate = serverKeys.getCertificate("vlm");
    KeyStore trust = KeyStore.getInstance("PKCS12");
    trust.load(null, null);
    trust.setCertificateEntry("vlm", certificate);
    TrustManagerFactory trustManagers =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagers.init(trust);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(null, trustManagers.getTrustManagers(), null);
    return context;
  }

  /** An HTTPS chat-completions fake on loopback that records the server
   * names the client asked for and the Host headers it sent. */
  private static final class TlsVlm implements AutoCloseable {
    final List<List<SNIServerName>> serverNames = new CopyOnWriteArrayList<>();
    final List<String> hosts = new CopyOnWriteArrayList<>();
    private final HttpsServer server;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    TlsVlm(KeyStore keys) throws Exception {
      KeyManagerFactory keyManagers =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagers.init(keys, "changeit".toCharArray());
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(keyManagers.getKeyManagers(), null, null);
      server = HttpsServer.create(
          new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
      server.setHttpsConfigurator(new HttpsConfigurator(context));
      server.createContext("/v1/chat/completions", exchange -> {
        exchange.getRequestBody().readAllBytes();
        serverNames.add(((ExtendedSSLSession) ((HttpsExchange) exchange).getSSLSession())
            .getRequestedServerNames());
        hosts.add(exchange.getRequestHeaders().getFirst("Host"));
        byte[] reply = ("{\"choices\":[{\"message\":{\"role\":\"assistant\","
            + "\"content\":\"tls description\"}}]}").getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, reply.length);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(reply);
        }
      });
      server.setExecutor(executor);
      server.start();
    }

    int port() {
      return server.getAddress().getPort();
    }

    @Override
    public void close() {
      server.stop(0);
      executor.close();
    }
  }
}
