package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.enrich.vlm.PublicAddress;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Which addresses a caller-chosen endpoint may reach in
 * ENRICH_ALLOW_REQUEST_ENDPOINT mode: public unicast only, every resolved
 * address checked, IPv4 hidden inside IPv6 judged as IPv4.
 */
class PublicAddressTest {

  @ParameterizedTest
  @ValueSource(strings = {
      "127.0.0.1", "127.255.0.9", "0.0.0.0", "10.1.2.3", "172.16.0.1", "172.31.255.254",
      "192.168.1.1", "169.254.169.254", "100.100.100.200", "100.64.0.1", "192.0.0.192",
      "198.18.0.1", "224.0.0.1", "255.255.255.255", "240.0.0.1",
      "::", "::1", "fe80::1", "fd00:ec2::254", "fc00::1", "ff02::1", "::127.0.0.1",
      "::ffff:127.0.0.1", "::ffff:169.254.169.254", "64:ff9b::a9fe:a9fe", "64:ff9b:1::1",
      "2002:a9fe:a9fe::1", "2002:7f00:1::1", "2001:db8::1", "2001:0:4136:e378::1"})
  void nonPublicAddresses_areRefused(String literal) {
    assertThat(PublicAddress.isPublic(InetAddress.ofLiteral(literal))).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "8.8.8.8", "1.1.1.1", "93.184.215.14", "172.32.0.1", "100.128.0.1",
      "2606:4700:4700::1111", "64:ff9b::808:808", "2002:808:808::1"})
  void publicAddresses_areAllowed(String literal) {
    assertThat(PublicAddress.isPublic(InetAddress.ofLiteral(literal))).isTrue();
  }

  @Test
  void ipv4MappedHeldAsIpv6_isJudgedByTheIpv4Inside() throws Exception {
    byte[] mapped = new byte[16];
    mapped[10] = (byte) 0xff;
    mapped[11] = (byte) 0xff;
    mapped[12] = 127;
    mapped[15] = 1;
    InetAddress address = Inet6Address.getByAddress(null, mapped, -1);
    assertThat(address).isInstanceOf(Inet6Address.class);
    assertThat(PublicAddress.isPublic(address)).isFalse();
  }

  @Test
  void everyResolvedAddress_isChecked() {
    PublicAddress.Resolver mixed = host -> new InetAddress[] {
        InetAddress.ofLiteral("8.8.8.8"), InetAddress.ofLiteral("10.0.0.7")};
    assertThatThrownBy(() -> PublicAddress.resolvePublic("mixed.example", mixed))
        .isInstanceOf(PublicAddress.Refused.class)
        .hasMessageContaining("non-public")
        .hasMessageNotContaining("10.0.0.7");
  }

  @Test
  void unresolvableHost_isRefused() {
    PublicAddress.Resolver none = host -> {
      throw new UnknownHostException(host);
    };
    assertThatThrownBy(() -> PublicAddress.resolvePublic("nowhere.example", none))
        .isInstanceOf(PublicAddress.Refused.class)
        .hasMessageContaining("does not resolve");
  }

  @Test
  void literalHost_isCheckedWithoutTheResolver() throws Exception {
    AtomicInteger lookups = new AtomicInteger();
    PublicAddress.Resolver counting = host -> {
      lookups.incrementAndGet();
      return new InetAddress[] {InetAddress.ofLiteral("8.8.8.8")};
    };
    assertThatThrownBy(() -> PublicAddress.resolvePublic("[::1]", counting))
        .isInstanceOf(PublicAddress.Refused.class);
    assertThat(PublicAddress.resolvePublic("1.1.1.1", counting))
        .isEqualTo(InetAddress.ofLiteral("1.1.1.1"));
    assertThat(lookups).hasValue(0);
  }

  @Test
  void publicHost_resolvesToItsFirstAddress() throws Exception {
    PublicAddress.Resolver two = host -> new InetAddress[] {
        InetAddress.ofLiteral("8.8.4.4"), InetAddress.ofLiteral("2001:4860:4860::8844")};
    assertThat(PublicAddress.resolvePublic("dns.example", two))
        .isEqualTo(InetAddress.ofLiteral("8.8.4.4"));
  }
}
