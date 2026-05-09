package io.conduit.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class EventParserTest {
    private static final Instant NOW = Instant.parse("2026-10-25T12:00:00Z");
    private final EventParser parser = new EventParser(Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void mapsKnownFieldsAndFoldsTheRestIntoAttributes() throws Exception {
        Event e = parser.parseJson("""
                {"id":"e1","type":"auth","timestamp":"2026-10-25T09:15:02-04:00","severity":"warn",
                 "key":"alice","user":"alice","attributes":{"geo":{"country":"US"}},"source":"edge-7"}""",
                "http", null);

        assertThat(e.id()).isEqualTo("e1");
        assertThat(e.type()).isEqualTo("auth");
        assertThat(e.source()).isEqualTo("http");
        assertThat(e.severity()).isEqualTo(Severity.MEDIUM);
        assertThat(e.key()).isEqualTo("alice");
        assertThat(e.timestamp()).isEqualTo(OffsetDateTime.parse("2026-10-25T09:15:02-04:00"));
        assertThat(e.attributes()).containsEntry("user", "alice").containsEntry("origin", "edge-7");
        assertThat(e.attribute("geo.country")).isEqualTo("US");
    }

    @Test
    void fillsDefaults() throws Exception {
        Event e = parser.parseJson("{\"type\":\"heartbeat\"}", "synth", null);
        assertThat(e.severity()).isEqualTo(Severity.INFO);
        assertThat(e.timestamp().toInstant()).isEqualTo(NOW);
        assertThat(e.id()).hasSize(36);
        assertThat(e.key()).isEqualTo(e.id());
    }

    @Test
    void keyFallsBackToConfiguredFieldThenId() throws Exception {
        assertThat(parser.parseJson("{\"type\":\"auth\",\"user\":\"bob\"}", "s", "user").key()).isEqualTo("bob");
        assertThat(parser.parseJson("{\"type\":\"auth\",\"a\":{\"b\":7}}", "s", "a.b").key()).isEqualTo("7");
        Event noField = parser.parseJson("{\"type\":\"auth\",\"id\":\"x9\"}", "s", "user");
        assertThat(noField.key()).isEqualTo("x9");
    }

    @ParameterizedTest
    @CsvSource({
            "1793016000, 2026-10-26T12:00:00Z",
            "1793016000000, 2026-10-26T12:00:00Z",
            "'\"1793016000\"', 2026-10-26T12:00:00Z",
            "'\"2026-10-25T08:00:00\"', 2026-10-25T08:00:00Z",
            "'\"2026-10-25T08:00:00+02:00\"', 2026-10-25T06:00:00Z"
    })
    void acceptsTimestampShapes(String raw, String expectedUtc) throws Exception {
        Event e = parser.parseJson("{\"type\":\"t\",\"ts\":" + raw + "}", "s", null);
        assertThat(e.timestamp().toInstant()).isEqualTo(Instant.parse(expectedUtc));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "not json",
            "[1,2,3]",
            "{\"severity\":\"high\"}",
            "{\"type\":\"\"}",
            "{\"type\":\"auth\",\"severity\":\"apocalyptic\"}",
            "{\"type\":\"auth\",\"severity\":9}",
            "{\"type\":\"auth\",\"timestamp\":\"yesterday\"}",
            "{\"type\":\"auth\",\"attributes\":[1]}",
            "{\"type\":\"auth\",\"ts\":-5}",
            "null"
    })
    void rejectsMalformedRecordsWithAReason(String line) {
        assertThatThrownBy(() -> parser.parseJson(line, "s", null))
                .isInstanceOf(MalformedRecordException.class)
                .hasMessageMatching(".+");
    }

    @Test
    void severityAliases() {
        assertThat(Severity.parse("ERROR")).isEqualTo(Severity.HIGH);
        assertThat(Severity.parse("fatal")).isEqualTo(Severity.CRITICAL);
        assertThat(Severity.parse(2)).isEqualTo(Severity.MEDIUM);
        assertThat(Severity.fromSyslog(0)).isEqualTo(Severity.CRITICAL);
        assertThat(Severity.fromSyslog(4)).isEqualTo(Severity.MEDIUM);
        assertThat(Severity.fromSyslog(7)).isEqualTo(Severity.INFO);
    }

    @Test
    void eventsAreImmutable() throws Exception {
        Event e = parser.fromMap(Map.of("type", "auth", "user", "a"), "s", null);
        assertThatThrownBy(() -> e.attributes().put("x", 1)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void writesCanonicalJson() throws Exception {
        Event e = parser.parseJson("{\"id\":\"1\",\"type\":\"auth\",\"ts\":\"2026-10-25T12:00:00Z\",\"user\":\"a\"}",
                "s", null);
        assertThat(Json.toJson(e)).isEqualTo("{\"id\":\"1\",\"timestamp\":\"2026-10-25T12:00:00Z\",\"source\":\"s\","
                + "\"type\":\"auth\",\"severity\":\"INFO\",\"key\":\"1\",\"attributes\":{\"user\":\"a\"}}");
    }
}
