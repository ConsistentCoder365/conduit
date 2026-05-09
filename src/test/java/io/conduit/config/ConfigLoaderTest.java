package io.conduit.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.conduit.core.Severity;
import io.conduit.pipeline.Pipeline;
import io.conduit.route.Router;
import io.conduit.testkit.Events;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigLoaderTest {
    private static final String VALID = """
            pipeline: { workers: 3, queueCapacity: 100, shutdownTimeout: 5s }
            metrics: { enabled: false }
            sources:
              - { name: synth, type: synthetic, rate: 0, count: 10 }
            filter:
              drop: { type: heartbeat }
            transforms:
              - { type: timestamp }
            routes:
              policy: first-match
              rules:
                - { name: auth, when: { type: auth }, to: [a] }
              default: [b]
            sinks:
              - { name: a, type: blackhole, workers: 2, batchSize: 50, retry: { maxAttempts: 2 } }
              - { name: b, type: blackhole, circuitBreaker: { enabled: false }, overflow: block }
            """;

    private static ConduitConfig parse(String yaml) {
        return ConfigLoader.parse(yaml, Map.<String, String>of()::get);
    }

    @Test
    void parsesAValidConfig() {
        ConduitConfig c = parse(VALID);
        assertThat(c.workers()).isEqualTo(3);
        assertThat(c.queueCapacity()).isEqualTo(100);
        assertThat(c.shutdownTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(c.router().policy()).isEqualTo(Router.Policy.FIRST_MATCH);
        assertThat(c.router().route(Events.of("auth"))).containsExactly("a");
        assertThat(c.router().route(Events.of("process"))).containsExactly("b");
        assertThat(c.filter().accept(Events.of("heartbeat"))).isFalse();
        SinkSettings a = c.sinks().get(0).settings();
        assertThat(a.workers()).isEqualTo(2);
        assertThat(a.batchSize()).isEqualTo(50);
        assertThat(a.queueCapacity()).as("inherits pipeline queueCapacity").isEqualTo(100);
        assertThat(a.retry().maxAttempts()).isEqualTo(2);
        assertThat(c.sinks().get(1).settings().breaker().enabled()).isFalse();
        assertThat(c.sinks().get(1).settings().overflow()).isEqualTo(SinkSettings.Overflow.BLOCK);
    }

    @Test
    void substitutesEnvironmentVariables() {
        String yaml = VALID.replace("rate: 0", "rate: ${RATE:-5}").replace("count: 10", "count: ${COUNT}");
        ConduitConfig c = ConfigLoader.parse(yaml, Map.of("COUNT", "77")::get);
        assertThat(c.sources().get(0).component().fields()).containsEntry("rate", 5).containsEntry("count", 77);
        assertThatThrownBy(() -> parse(yaml)).hasMessageContaining("COUNT is not set");
    }

    @Test
    void noRoutesMeansEverySink() {
        ConduitConfig c = parse(VALID.replaceAll("(?s)routes:.*?sinks:", "sinks:"));
        assertThat(c.router().route(Events.of("auth"))).containsExactly("a", "b");
    }

    @Test
    void rejectsUnknownTopLevelKeys() {
        assertThatThrownBy(() -> parse(VALID + "sinkz: []\n")).isInstanceOf(ConfigException.class)
                .hasMessageContaining("unknown field(s) [sinkz] at config");
    }

    @Test
    void rejectsUnknownTypesListingTheKnownOnes() {
        assertThatThrownBy(() -> parse(VALID.replace("type: synthetic", "type: kafka")))
                .hasMessageContaining("unknown source type 'kafka'").hasMessageContaining("synthetic");
        assertThatThrownBy(() -> parse(VALID.replace("type: timestamp", "type: geoip")))
                .hasMessageContaining("unknown transform type 'geoip'");
    }

    @Test
    void rejectsMissingFields() {
        assertThatThrownBy(() -> parse(VALID.replace("name: synth, ", "")))
                .hasMessageContaining("missing required field 'name' at config.sources[0]");
        assertThatThrownBy(() -> parse(VALID.replace("{ name: auth, when: { type: auth }, to: [a] }",
                "{ name: auth, to: [a] }"))).hasMessageContaining("missing required field 'when'");
    }

    @Test
    void rejectsBrokenReferencesAndDuplicatesAllAtOnce() {
        String yaml = VALID.replace("to: [a]", "to: [nope]").replace("name: b,", "name: a,");
        assertThatThrownBy(() -> parse(yaml))
                .hasMessageContaining("route 'auth' sends to unknown sink 'nope'")
                .hasMessageContaining("duplicate sink name 'a'")
                .hasMessageContaining("default route sends to unknown sink 'b'");
    }

    @Test
    void requiresADefaultRouteWhenRoutesExistAndThereAreSeveralSinks() {
        assertThatThrownBy(() -> parse(VALID.replace("default: [b]", "")))
                .hasMessageContaining("routes.default is required");
    }

    @Test
    void rejectsBadValues() {
        assertThatThrownBy(() -> parse(VALID.replace("workers: 3", "workers: 0")))
                .hasMessageContaining("invalid 'workers' at config.pipeline: must be at least 1");
        assertThatThrownBy(() -> parse(VALID.replace("shutdownTimeout: 5s", "shutdownTimeout: soon")))
                .hasMessageContaining("expected a duration");
        assertThatThrownBy(() -> parse(VALID.replace("overflow: block", "overflow: explode")))
                .hasMessageContaining("expected one of [dead-letter, block]");
        assertThatThrownBy(() -> parse(VALID.replace("batchSize: 50", "batchSize: 500")))
                .hasMessageContaining("batchSize (500) exceeds queueCapacity (100)");
        assertThatThrownBy(() -> parse(VALID.replace("name: synth", "name: 'my source'")))
                .hasMessageContaining("may only use letters");
        assertThatThrownBy(() -> parse("sources: [unclosed")).hasMessageContaining("not valid YAML");
    }

    @Test
    void componentFieldsAreCheckedWhenThePipelineIsBuilt() {
        ConduitConfig c = parse(VALID.replace("count: 10", "count: 10, colour: blue"));
        assertThatThrownBy(() -> Pipeline.create(c, new SimpleMeterRegistry()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("unknown field(s) [colour] at config.sources[0] (synth)");

        ConduitConfig noSalt = parse(VALID.replace("{ type: timestamp }", "{ type: redact }"));
        assertThatThrownBy(() -> Pipeline.create(noSalt, new SimpleMeterRegistry()))
                .hasMessageContaining("missing required field 'salt'");
    }

    @Test
    void refusesToStartWhenASinkIsUnreachable(@TempDir Path tmp) {
        String yaml = VALID.replace("{ name: b, type: blackhole, circuitBreaker: { enabled: false }, overflow: block }",
                "{ name: b, type: postgres, url: 'jdbc:postgresql://127.0.0.1:1/none' }")
                + "deadLetter: { dir: '" + tmp + "' }\n";
        Pipeline p = Pipeline.create(parse(yaml), new SimpleMeterRegistry());
        assertThatThrownBy(p::start).isInstanceOf(ConfigException.class)
                .hasMessageContaining("sink 'b' is unreachable");
    }

    @Test
    void validatesProgrammaticBuildsToo() {
        assertThatThrownBy(() -> ConduitConfig.builder().build())
                .hasMessageContaining("at least one source is required")
                .hasMessageContaining("at least one sink is required");
        assertThat(Severity.valueOf("HIGH")).isNotNull();
    }
}
