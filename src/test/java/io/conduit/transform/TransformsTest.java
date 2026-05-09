package io.conduit.transform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.conduit.config.ConfigException;
import io.conduit.core.Event;
import io.conduit.core.Severity;
import io.conduit.testkit.Events;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TransformsTest {
    private static final Transform.Chain END = e -> e;

    @Test
    void timestampIsShiftedToUtcAndListedFieldsAreNormalized() {
        Clock clock = Clock.fixed(Instant.parse("2026-10-25T12:00:00Z"), ZoneOffset.UTC);
        TimestampNormalizer t = new TimestampNormalizer(List.of("login_time", "epoch"), null, clock);
        Event in = Events.of("auth", "login_time", "2026-10-25T09:00:00-04:00", "epoch", 1793016000)
                .withTimestamp(OffsetDateTime.parse("2026-10-25T14:30:00+02:00"));

        Event out = t.apply(in, END);

        assertThat(out.timestamp()).isEqualTo(OffsetDateTime.parse("2026-10-25T12:30:00Z"));
        assertThat(out.timestamp().getOffset()).isEqualTo(ZoneOffset.UTC);
        assertThat(out.attributes()).containsEntry("login_time", "2026-10-25T13:00:00Z")
                .containsEntry("epoch", "2026-10-26T12:00:00Z");
    }

    @Test
    void futureTimestampsBeyondSkewAreReplacedAndKept() {
        Clock clock = Clock.fixed(Instant.parse("2026-10-25T12:00:00Z"), ZoneOffset.UTC);
        TimestampNormalizer t = new TimestampNormalizer(List.of(), Duration.ofMinutes(5), clock);
        Event out = t.apply(Events.of("auth").withTimestamp(OffsetDateTime.parse("2027-01-01T00:00:00Z")), END);
        assertThat(out.timestamp().toInstant()).isEqualTo(Instant.parse("2026-10-25T12:00:00Z"));
        assertThat(out.attributes()).containsEntry("original_timestamp", "2027-01-01T00:00:00Z");
    }

    @Test
    void redactHashesNamedFieldsAndEmailsAnywhereIncludingTheKey() {
        PiiRedactor r = new PiiRedactor(List.of("email"), true, PiiRedactor.Mode.HASH, "salt");
        Event in = Events.of("process", "email", "alice@contoso.com",
                "cmdline", "notify bob@fabrikam.com and alice@contoso.com",
                "nested", Map.of("list", List.of("x", "carol@example.org")), "pid", 42).withKey("alice@contoso.com");

        Event out = r.apply(in, END);

        String aliceToken = r.hash("alice@contoso.com");
        assertThat(aliceToken).startsWith("redacted:").hasSize(25);
        assertThat(out.attributes().get("email")).isEqualTo(aliceToken);
        assertThat((String) out.attributes().get("cmdline"))
                .isEqualTo("notify " + r.hash("bob@fabrikam.com") + " and " + aliceToken);
        assertThat(out.attribute("nested.list.1")).isEqualTo(r.hash("carol@example.org"));
        assertThat(out.key()).as("same input, same token: joins still work").isEqualTo(aliceToken);
        assertThat(out.attributes().get("pid")).isEqualTo(42);
        assertThat(out.toString()).doesNotContain("@contoso.com", "@fabrikam.com", "@example.org");
    }

    @Test
    void redactHashDependsOnSalt() {
        assertThat(new PiiRedactor(List.of(), true, PiiRedactor.Mode.HASH, "a").hash("x@y.com"))
                .isNotEqualTo(new PiiRedactor(List.of(), true, PiiRedactor.Mode.HASH, "b").hash("x@y.com"));
    }

    @Test
    void redactMaskAndRemoveModes() {
        Event in = Events.of("auth", "email", "alice@contoso.com", "phone", "555-0100");
        Event masked = new PiiRedactor(List.of("email"), false, PiiRedactor.Mode.MASK, null).apply(in, END);
        assertThat(masked.attributes()).containsEntry("email", "a***@contoso.com");
        Event removed = new PiiRedactor(List.of("phone"), false, PiiRedactor.Mode.REMOVE, null).apply(in, END);
        assertThat(removed.attributes()).doesNotContainKey("phone").containsKey("email");
    }

    @Test
    void untouchedEventsAreNotCopied() {
        Event in = Events.of("network", "dst_port", 443, "note", "no personal data here");
        assertThat(new PiiRedactor(List.of("email"), true, PiiRedactor.Mode.HASH, "s").apply(in, END)).isSameAs(in);
    }

    @Test
    void ipTableUsesLongestPrefixMatch(@TempDir Path dir) throws Exception {
        Path csv = dir.resolve("t.csv");
        Files.writeString(csv, """
                cidr,zone,country
                10.0.0.0/8,internal,
                10.1.0.0/16,datacenter,
                10.1.2.3/32,jumpbox,
                0.0.0.0/0,external,
                """);
        IpTable t = IpTable.load(csv);
        assertThat(t.size()).isEqualTo(4);
        assertThat(t.lookup("10.1.2.3")).containsEntry("zone", "jumpbox");
        assertThat(t.lookup("10.1.9.9")).containsEntry("zone", "datacenter");
        assertThat(t.lookup("10.200.0.1")).containsEntry("zone", "internal");
        assertThat(t.lookup("8.8.8.8")).containsEntry("zone", "external").doesNotContainKey("country");
        assertThat(t.lookup("not-an-ip")).isNull();
        assertThat(t.lookup("256.1.1.1")).isNull();
    }

    @Test
    void ipv4Parser() {
        assertThat(IpTable.parseIpv4("192.168.1.10")).isEqualTo(0xC0A8010A);
        assertThat(IpTable.parseIpv4("255.255.255.255")).isEqualTo(-1);
        for (String bad : new String[] {"1.2.3", "1.2.3.4.5", "1..2.3", "a.b.c.d", "1.2.3.256", ".1.2.3", "1.2.3."}) {
            assertThat(IpTable.parseIpv4(bad)).as(bad).isNull();
        }
    }

    @Test
    void enricherSwapsInANewTableOnReloadAndKeepsTheOldOneOnABadFile(@TempDir Path dir) throws Exception {
        Path csv = dir.resolve("t.csv");
        Files.writeString(csv, "cidr,zone\n10.0.0.0/8,internal\n");
        IpEnricher e = new IpEnricher(List.of("src_ip"), csv, Duration.ZERO, "_info", new SimpleMeterRegistry());
        Event in = Events.of("auth", "src_ip", "10.1.1.1", "dst_ip", "10.2.2.2");

        assertThat(e.apply(in, END).attribute("src_ip_info.zone")).isEqualTo("internal");
        assertThat(e.apply(in, END).attributes()).doesNotContainKey("dst_ip_info");

        Files.writeString(csv, "cidr,zone\n10.0.0.0/8,corp\n");
        Files.setLastModifiedTime(csv, FileTime.from(Instant.now().plusSeconds(5)));
        assertThat(e.reloadIfChanged()).isTrue();
        assertThat(e.apply(in, END).attribute("src_ip_info.zone")).isEqualTo("corp");

        Files.writeString(csv, "cidr,zone\nnot-a-cidr,broken\n");
        Files.setLastModifiedTime(csv, FileTime.from(Instant.now().plusSeconds(10)));
        assertThat(e.reloadIfChanged()).isFalse();
        assertThat(e.apply(in, END).attribute("src_ip_info.zone")).as("previous table stays").isEqualTo("corp");
    }

    @Test
    void enricherRefusesToStartWithoutAUsableTable(@TempDir Path dir) {
        assertThatThrownBy(() -> new IpEnricher(List.of("src_ip"), dir.resolve("missing.csv"), Duration.ZERO, "_info",
                new SimpleMeterRegistry())).isInstanceOf(ConfigException.class).hasMessageContaining("missing.csv");
    }

    @Test
    void chainRunsStepsInOrderAndCanDropAndNamesTheFailingStep() {
        Transform tagA = (e, next) -> next.proceed(e.withKey(e.key() + "A"));
        Transform tagB = (e, next) -> next.proceed(e.withKey(e.key() + "B"));
        Transform dropHeartbeats = (e, next) -> e.type().equals("heartbeat") ? null : next.proceed(e);
        Transform boom = (e, next) -> {
            throw new IllegalStateException("bad");
        };
        TransformChain chain = new TransformChain(List.of(new TransformChain.Step("a", tagA),
                new TransformChain.Step("drop", dropHeartbeats), new TransformChain.Step("b", tagB)));

        assertThat(chain.apply(Events.of("auth")).key()).isEqualTo("kAB");
        assertThat(chain.apply(Events.of("heartbeat", Severity.INFO))).isNull();

        TransformChain failing = new TransformChain(List.of(new TransformChain.Step("a", tagA),
                new TransformChain.Step("explode", boom)));
        assertThatThrownBy(() -> failing.apply(Events.of("auth"))).isInstanceOf(TransformException.class)
                .extracting(ex -> ((TransformException) ex).step()).isEqualTo("explode");
    }
}
