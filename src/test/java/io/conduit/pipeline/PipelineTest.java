package io.conduit.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import io.conduit.core.Event;
import io.conduit.core.Json;
import io.conduit.observe.Accounting;
import io.conduit.testkit.CollectingSink;
import io.conduit.testkit.TestPipelines;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PipelineTest {
    @TempDir
    Path tmp;

    @AfterEach
    void reset() {
        TestPipelines.resetAll();
    }

    @Test
    void allFiveStagesFromOneConfig() throws Exception {
        Path input = tmp.resolve("in.jsonl");
        Files.writeString(input, """
                {"type":"auth","ts":"2026-10-25T09:00:00-04:00","user":"alice","email":"alice@contoso.com","src_ip":"10.1.2.3","severity":"high"}
                {"type":"heartbeat","host":"h1"}
                {"type":"network","src_ip":"52.1.1.1","severity":"low"}
                {"user":"no type"}
                {"type":"process","cmdline":"mail bob@fabrikam.com","severity":"critical"}
                """);
        Path table = tmp.resolve("ip.csv");
        Files.writeString(table, "cidr,zone\n10.0.0.0/8,internal\n0.0.0.0/0,external\n");

        ShutdownReport r = TestPipelines.runClean("""
                pipeline: { workers: 2 }
                sources:
                  - { name: file, type: file, path: '%s', exitAtEof: true, keyField: user }
                filter:
                  drop: { type: heartbeat }
                transforms:
                  - { type: timestamp }
                  - { type: redact, fields: [email], salt: s }
                  - { type: enrich-ip, table: '%s', reloadInterval: 0ms }
                routes:
                  rules:
                    - { name: hot, when: { severity: { gte: HIGH } }, to: [alerts] }
                    - { name: auth, when: { type: auth }, to: [graph] }
                  default: [archive]
                sinks:
                  - { name: alerts, type: collect }
                  - { name: graph, type: collect }
                  - { name: archive, type: collect }
                """.formatted(input, table), tmp);

        Accounting.Snapshot a = r.accounting();
        assertThat(a.ingested()).isEqualTo(5);
        assertThat(a.parseFailed()).isEqualTo(1);
        assertThat(a.filtered()).isEqualTo(1);
        assertThat(a.routed()).isEqualTo(3);
        assertThat(a.deliveries()).as("auth+high fans out to two sinks").isEqualTo(4);
        assertThat(a.published()).isEqualTo(4);

        List<Event> alerts = CollectingSink.events("alerts");
        assertThat(alerts).extracting(Event::type).containsExactlyInAnyOrder("auth", "process");
        Event auth = CollectingSink.events("graph").get(0);
        assertThat(auth.key()).isEqualTo("alice");
        assertThat(auth.timestamp().toString()).isEqualTo("2026-10-25T13:00Z");
        assertThat(auth.attributes().get("email").toString()).startsWith("redacted:");
        assertThat(auth.attribute("src_ip_info.zone")).isEqualTo("internal");
        assertThat(CollectingSink.events("archive")).singleElement()
                .satisfies(e -> assertThat(e.attribute("src_ip_info.zone")).isEqualTo("external"));
        assertThat(Json.toJson(alerts.get(0)) + Json.toJson(alerts.get(1))).doesNotContain("fabrikam", "contoso");

        List<String> dead = deadLetters();
        assertThat(dead).singleElement().asString().contains("\"stage\":\"parse\"")
                .contains("missing required field 'type'").contains("\"raw\":");
    }

    @Test
    void syntheticRunBalancesAndWritesFiles() throws Exception {
        ShutdownReport r = TestPipelines.runClean("""
                pipeline: { workers: 4 }
                sources:
                  - { name: synth, type: synthetic, rate: 0, count: 50000, malformedRate: 0.01, seed: 7 }
                sinks:
                  - { name: out, type: file, dir: '${TMP}/out', workers: 2, maxFileBytes: 2MB }
                """, tmp);
        Accounting.Snapshot a = r.accounting();
        assertThat(a.ingested()).isEqualTo(50_000);
        assertThat(a.parseFailed()).isBetween(300L, 700L);
        assertThat(a.published()).isEqualTo(50_000 - a.parseFailed());

        long lines;
        try (Stream<Path> files = Files.list(tmp.resolve("out"))) {
            List<Path> all = files.toList();
            assertThat(all.size()).as("rolled into several files across two lanes").isGreaterThan(4);
            assertThat(all).anyMatch(p -> p.getFileName().toString().startsWith("out-0-"))
                    .anyMatch(p -> p.getFileName().toString().startsWith("out-1-"));
            lines = all.stream().mapToLong(PipelineTest::lineCount).sum();
        }
        assertThat(lines).isEqualTo(a.published());
        assertThat(deadLetters()).hasSize((int) a.parseFailed());
    }

    @Test
    void transformFailuresAreDeadLetteredWithTheStepName() throws Exception {
        ShutdownReport r = TestPipelines.runClean("""
                sources:
                  - { name: burst, type: burst, count: 1000, keys: 10 }
                transforms:
                  - { type: redact, fields: [email], salt: s, mode: mask }
                  - { name: flaky-step, type: explode, everyNth: 10 }
                sinks:
                  - { name: out, type: collect }
                """, tmp);
        assertThat(r.accounting().transformFailed()).isEqualTo(100);
        assertThat(CollectingSink.events("out")).hasSize(900);
        assertThat(deadLetters()).hasSize(100).allSatisfy(l -> assertThat(l)
                .contains("\"stage\":\"transform\"", "\"origin\":\"transform:flaky-step\"", "boom"));
    }

    @Test
    void healthReportsComponentState() throws Exception {
        Pipeline p = TestPipelines.create("""
                sources:
                  - { name: synth, type: synthetic, rate: 100 }
                sinks:
                  - { name: out, type: collect, workers: 2 }
                """, tmp);
        assertThat(p.health().status()).isEqualTo(Health.Status.DOWN);
        p.start();
        Health h = p.health();
        assertThat(h.status()).isEqualTo(Health.Status.UP);
        assertThat(h.details()).containsKeys("sources", "processing", "sinks", "accounting");
        @SuppressWarnings("unchecked")
        Map<String, Object> sinks = (Map<String, Object>) h.details().get("sinks");
        assertThat(sinks).containsKey("out");
        assertThat(p.stop(Duration.ofSeconds(10)).ok()).isTrue();
        assertThat(p.health().status()).isEqualTo(Health.Status.DOWN);
    }

    private List<String> deadLetters() throws Exception {
        Path dir = tmp.resolve("dlq");
        if (!Files.exists(dir)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(dir)) {
            return files.flatMap(f -> {
                try {
                    return Files.readAllLines(f).stream();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }).filter(l -> !l.isBlank()).toList();
        }
    }

    static long lineCount(Path p) {
        try (Stream<String> s = Files.lines(p)) {
            return s.filter(l -> !l.isBlank()).count();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
