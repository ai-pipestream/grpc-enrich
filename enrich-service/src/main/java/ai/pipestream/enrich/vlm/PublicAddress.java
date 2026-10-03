package ai.pipestream.enrich.vlm;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

/**
 * Which IP addresses a caller-chosen VLM endpoint may resolve to: public
 * unicast only. Loopback, private, link-local (cloud metadata lives at
 * {@code 169.254.169.254} and {@code fd00:ec2::254}), carrier-grade NAT
 * (Alibaba's metadata at {@code 100.100.100.200}), multicast, and the
 * reserved and documentation ranges are refused. IPv6 forms that carry an
 * IPv4 address (mapped, NAT64, 6to4) are judged by the IPv4 address inside.
 */
public final class PublicAddress {

  /** An address prefix: the first {@code bits} of {@code network}. */
  private record Prefix(byte[] network, int bits) {
    static Prefix of(String literal, int bits) {
      return new Prefix(InetAddress.ofLiteral(literal).getAddress(), bits);
    }

    /** ::ffff:0:0/96, built by hand: as a literal the JDK turns it into the
     * IPv4 address 0.0.0.0. */
    static Prefix ipv4Mapped() {
      byte[] network = new byte[16];
      network[10] = (byte) 0xff;
      network[11] = (byte) 0xff;
      return new Prefix(network, 96);
    }

    boolean contains(byte[] address) {
      if (address.length != network.length) {
        return false;
      }
      int whole = bits / 8;
      for (int i = 0; i < whole; i++) {
        if (address[i] != network[i]) {
          return false;
        }
      }
      int rest = bits % 8;
      if (rest == 0) {
        return true;
      }
      int mask = (0xff << (8 - rest)) & 0xff;
      return (address[whole] & mask) == (network[whole] & mask);
    }
  }

  private static final List<Prefix> REFUSED_V4 = List.of(
      Prefix.of("0.0.0.0", 8),         // "this network"
      Prefix.of("10.0.0.0", 8),        // private
      Prefix.of("100.64.0.0", 10),     // carrier-grade NAT
      Prefix.of("127.0.0.0", 8),       // loopback
      Prefix.of("169.254.0.0", 16),    // link-local, cloud metadata
      Prefix.of("172.16.0.0", 12),     // private
      Prefix.of("192.0.0.0", 24),      // IETF protocol assignments (Oracle metadata)
      Prefix.of("192.0.2.0", 24),      // documentation
      Prefix.of("192.88.99.0", 24),    // 6to4 relay anycast
      Prefix.of("192.168.0.0", 16),    // private
      Prefix.of("198.18.0.0", 15),     // benchmarking
      Prefix.of("198.51.100.0", 24),   // documentation
      Prefix.of("203.0.113.0", 24),    // documentation
      Prefix.of("224.0.0.0", 4),       // multicast
      Prefix.of("240.0.0.0", 4));      // reserved, broadcast

  private static final List<Prefix> REFUSED_V6 = List.of(
      Prefix.of("::", 96),             // unspecified, loopback, IPv4-compatible
      Prefix.of("64:ff9b:1::", 48),    // local-use NAT64
      Prefix.of("100::", 64),          // discard
      Prefix.of("2001::", 32),         // Teredo
      Prefix.of("2001:db8::", 32),     // documentation
      Prefix.of("fc00::", 7),          // unique local (AWS metadata fd00:ec2::254)
      Prefix.of("fe80::", 10),         // link-local
      Prefix.of("fec0::", 10),         // site-local (deprecated)
      Prefix.of("ff00::", 8));         // multicast

  /** IPv6 prefixes whose last 32 bits are an IPv4 address. */
  private static final List<Prefix> EMBEDS_V4_AT_END = List.of(
      Prefix.ipv4Mapped(),             // IPv4-mapped
      Prefix.of("64:ff9b::", 96));     // NAT64
  private static final Prefix SIX_TO_FOUR = Prefix.of("2002::", 16);

  private PublicAddress() {}

  /** Whether {@code address} is public unicast, so a caller may make this
   * server connect to it. */
  public static boolean isPublic(InetAddress address) {
    byte[] bytes = address.getAddress();
    if (address instanceof Inet4Address) {
      return isPublicV4(bytes);
    }
    for (Prefix embeds : EMBEDS_V4_AT_END) {
      if (embeds.contains(bytes)) {
        return isPublicV4(Arrays.copyOfRange(bytes, 12, 16));
      }
    }
    if (SIX_TO_FOUR.contains(bytes)) {
      return isPublicV4(Arrays.copyOfRange(bytes, 2, 6));
    }
    return REFUSED_V6.stream().noneMatch(prefix -> prefix.contains(bytes));
  }

  private static boolean isPublicV4(byte[] bytes) {
    return REFUSED_V4.stream().noneMatch(prefix -> prefix.contains(bytes));
  }

  /** How a host name resolves; {@link InetAddress#getAllByName} in
   * production, a fixed table in tests. */
  @FunctionalInterface
  public interface Resolver {
    InetAddress[] resolve(String host) throws UnknownHostException;
  }

  /** The system resolver. */
  public static final Resolver SYSTEM = InetAddress::getAllByName;

  /**
   * The IP literal {@code host} names, or null when it is a name. Accepts
   * the bracketed IPv6 form a URI carries.
   */
  public static InetAddress literal(String host) {
    String bare = host.startsWith("[") && host.endsWith("]")
        ? host.substring(1, host.length() - 1)
        : host;
    try {
      return InetAddress.ofLiteral(bare);
    } catch (IllegalArgumentException name) {
      return null;
    }
  }

  /**
   * The one address a caller-chosen endpoint's calls connect to: {@code host}
   * is resolved once, every address it resolves to must be public, and the
   * first is returned. Connecting to that address (never resolving the name
   * again) is what keeps a DNS answer that changes between this check and
   * the connection (DNS rebinding) from reaching a private address.
   *
   * @throws Refused when {@code host} does not resolve, or any of its
   *     addresses is not public; the message never names an address
   */
  public static InetAddress resolvePublic(String host, Resolver resolver) throws Refused {
    InetAddress literal = literal(host);
    InetAddress[] addresses;
    if (literal != null) {
      addresses = new InetAddress[] {literal};
    } else {
      try {
        addresses = resolver.resolve(host);
      } catch (UnknownHostException unknown) {
        throw new Refused("per-request VLM endpoint host does not resolve");
      }
    }
    if (addresses == null || addresses.length == 0) {
      throw new Refused("per-request VLM endpoint host does not resolve");
    }
    for (InetAddress address : addresses) {
      if (!isPublic(address)) {
        throw new Refused("per-request VLM endpoint resolves to a loopback, private,"
            + " link-local, or other non-public address, which this server does not call"
            + " for a caller");
      }
    }
    return addresses[0];
  }

  /** A caller-chosen endpoint this server will not connect to. */
  public static final class Refused extends Exception {
    public Refused(String message) {
      super(message);
    }
  }
}
