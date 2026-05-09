package io.conduit.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.conduit.core.MalformedRecordException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SyslogLineParserTest {

    @Test
    void rfc5424WithStructuredData() throws Exception {
        Map<String, Object> r = SyslogLineParser.parse(
                "<134>1 2026-10-25T12:00:00Z web-1 sshd 4242 auth [meta user=\"carol\" note=\"a \\\"quoted\\\" ]\"] "
                        + "Accepted password outcome=success");
        assertThat(r).containsEntry("timestamp", "2026-10-25T12:00:00Z").containsEntry("host", "web-1")
                .containsEntry("app", "sshd").containsEntry("type", "auth").containsEntry("user", "carol")
                .containsEntry("note", "a \"quoted\" ]").containsEntry("outcome", "success")
                .containsEntry("severity", "INFO").containsEntry("facility", 16)
                .containsEntry("message", "Accepted password outcome=success");
    }

    @Test
    void rfc5424NilFields() throws Exception {
        Map<String, Object> r = SyslogLineParser.parse("<11>1 - - - - - - disk full");
        assertThat(r).containsEntry("type", "syslog").containsEntry("severity", "HIGH")
                .containsEntry("message", "disk full").doesNotContainKeys("host", "timestamp");
    }

    @Test
    void jsonBodyAfterPriority() throws Exception {
        Map<String, Object> r = SyslogLineParser.parse("<10>{\"type\":\"process\",\"user\":\"dave\"}");
        assertThat(r).containsEntry("type", "process").containsEntry("user", "dave").containsEntry("severity", "CRITICAL");
    }

    @Test
    void jsonSeverityWinsOverPriority() throws Exception {
        assertThat(SyslogLineParser.parse("<10>{\"type\":\"x\",\"severity\":\"low\"}")).containsEntry("severity", "low");
    }

    @Test
    void freeTextExtractsKeyValuePairs() throws Exception {
        Map<String, Object> r = SyslogLineParser.parse(
                "Oct 25 12:00:01 fw kernel: type=network src_ip=10.0.0.5 msg=\"port scan\" action=deny");
        assertThat(r).containsEntry("type", "network").containsEntry("src_ip", "10.0.0.5")
                .containsEntry("msg", "port scan").containsEntry("action", "deny").doesNotContainKey("severity");
    }

    @Test
    void rejectsBrokenInput() {
        for (String bad : List.of("", "   ", "<999>hi", "<x>hi", "<13>{broken", "<13>1 too short",
                "<13>1 ts h a p m [unterminated", "<13>1 ts h a p m nosd")) {
            assertThatThrownBy(() -> SyslogLineParser.parse(bad)).as(bad).isInstanceOf(MalformedRecordException.class);
        }
    }

    @Test
    void httpBodySplitsArraysAndNdjson() {
        List<HttpBatchSource.Item> arr = HttpBatchSource.split("[{\"type\":\"a\"}, 5, {\"type\":\"b\"}]");
        assertThat(arr).hasSize(3);
        assertThat(arr.get(0).error()).isNull();
        assertThat(arr.get(1).error()).contains("not a JSON object");
        List<HttpBatchSource.Item> nd = HttpBatchSource.split("{\"type\":\"a\"}\n\n{oops\nnull\n");
        assertThat(nd).extracting(HttpBatchSource.Item::error).containsExactly(null,
                nd.get(1).error(), "record is not a JSON object");
        assertThat(nd.get(1).error()).startsWith("invalid JSON");
        assertThat(HttpBatchSource.split("[1, oops").get(0).error()).startsWith("invalid JSON array");
    }
}
