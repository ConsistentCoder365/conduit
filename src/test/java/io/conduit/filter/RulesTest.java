package io.conduit.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.conduit.config.ConfigException;
import io.conduit.core.Event;
import io.conduit.core.Severity;
import io.conduit.testkit.Events;
import org.junit.jupiter.api.Test;

class RulesTest {
    private static final YAMLMapper YAML = new YAMLMapper();

    private static Rule rule(String yaml) throws Exception {
        return Rules.parse(YAML.readValue(yaml, Object.class), "test");
    }

    private final Event authHigh = Events.of("auth", Severity.HIGH, "user", "svc-backup", "dst_port", 3389,
            "bytes", "2048", "geo", java.util.Map.of("country", "DE"));
    private final Event netInfo = Events.of("network", Severity.INFO, "dst_port", 443);

    @Test
    void severityComparisons() throws Exception {
        assertThat(rule("severity: HIGH").test(authHigh)).isTrue();
        assertThat(rule("severity: { gte: MEDIUM }").test(authHigh)).isTrue();
        assertThat(rule("severity: { gte: MEDIUM }").test(netInfo)).isFalse();
        assertThat(rule("severity: { lt: LOW }").test(netInfo)).isTrue();
        assertThat(rule("severity: [LOW, CRITICAL]").test(authHigh)).isFalse();
        assertThat(rule("severity: { gt: LOW, lte: HIGH }").test(authHigh)).isTrue();
    }

    @Test
    void stringOperators() throws Exception {
        assertThat(rule("type: auth").test(authHigh)).isTrue();
        assertThat(rule("type: [process, network]").test(authHigh)).isFalse();
        assertThat(rule("type: { ne: auth }").test(netInfo)).isTrue();
        assertThat(rule("source: { matches: '^te' }").test(authHigh)).isTrue();
        assertThat(rule("type: { notIn: [auth] }").test(authHigh)).isFalse();
    }

    @Test
    void fieldOperatorsReachIntoAttributes() throws Exception {
        assertThat(rule("field: { name: user, startsWith: svc- }").test(authHigh)).isTrue();
        assertThat(rule("field: { name: dst_port, in: [22, 3389] }").test(authHigh)).isTrue();
        assertThat(rule("field: { name: dst_port, in: [22, 3389] }").test(netInfo)).isFalse();
        assertThat(rule("field: { name: bytes, gt: 1000 }").test(authHigh)).isTrue();
        assertThat(rule("field: { name: geo.country, eq: DE }").test(authHigh)).isTrue();
        assertThat(rule("field: { name: user, exists: false }").test(netInfo)).isTrue();
        assertThat(rule("field: { name: missing, gt: 1 }").test(netInfo)).isFalse();
    }

    @Test
    void composesWithAndOrNot() throws Exception {
        Rule r = rule("""
                or:
                  - and: [ { type: auth }, { severity: { gte: HIGH } } ]
                  - not: { field: { name: dst_port, eq: 443 } }
                """);
        assertThat(r.test(authHigh)).isTrue();
        assertThat(r.test(netInfo)).isFalse();
        assertThat(rule("{ type: auth, severity: INFO }").test(authHigh)).as("multi-key map is AND").isFalse();
    }

    @Test
    void reportsWhereAndWhyARuleIsInvalid() {
        assertThatThrownBy(() -> rule("colour: red")).isInstanceOf(ConfigException.class)
                .hasMessageContaining("unknown rule 'colour' at test.colour");
        assertThatThrownBy(() -> rule("severity: { gte: SEVERE }")).hasMessageContaining("unknown severity: SEVERE");
        assertThatThrownBy(() -> rule("field: { eq: 1 }")).hasMessageContaining("missing required field 'name'");
        assertThatThrownBy(() -> rule("field: { name: x, matches: '[' }")).hasMessageContaining("invalid regex");
        assertThatThrownBy(() -> rule("and: []")).hasMessageContaining("empty rule list");
        assertThatThrownBy(() -> rule("type: { like: x }")).hasMessageContaining("unknown operator 'like'");
    }

    @Test
    void filterStageKeepsAndDrops() throws Exception {
        FilterStage f = new FilterStage(rule("severity: { gte: LOW }"), rule("type: heartbeat"));
        assertThat(f.accept(authHigh)).isTrue();
        assertThat(f.accept(netInfo)).isFalse();
        assertThat(f.accept(Events.of("heartbeat", Severity.HIGH))).isFalse();
        assertThat(FilterStage.PASS_ALL.accept(netInfo)).isTrue();
    }
}
