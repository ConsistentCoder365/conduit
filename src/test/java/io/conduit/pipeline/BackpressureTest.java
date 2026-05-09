package io.conduit.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import io.conduit.core.Json;
import io.conduit.source.HttpBatchSource;
import io.conduit.testkit.GateSink;
import io.conduit.testkit.TestPipelines;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Bounded queues everywhere: a stalled sink must stall the sources, not grow memory. */
class BackpressureTest {
    @TempDir
    Path tmp;

    @AfterEach
    void reset() {
        TestPipelines.resetAll();
    }

    @Test
    void stalledSinkBlocksTheSourceAtTheSumOfQueueCapacities() throws Exception {
        GateSink.close("gate");
        Pipeline p = TestPipelines.create("""
                pipeline: { workers: 2, queueCapacity: 100 }
                sources:
                  - { name: synth, type: synthetic, rate: 0 }
                sinks:
                  - { name: gate, type: gate, queueCapacity: 200, batchSize: 50, overflow: block }
                """, tmp);
        p.start();
        Thread.sleep(500);
        long first = p.accounting().snapshot().ingested();
        Thread.sleep(500);
        long second = p.accounting().snapshot().ingested();

        // 2 lanes x 100 + sink queue 200 + one batch held by the stuck write + one event per blocked put
        long bound = 2 * 100 + 200 + 50 + 3;
        assertThat(second).as("source is blocked, not buffering").isEqualTo(first).isLessThanOrEqualTo(bound);

        GateSink.open("gate");
        ShutdownReport r = p.stop(Duration.ofSeconds(10));
        assertThat(r.ok()).as(r.summary()).isTrue();
        assertThat(GateSink.written("gate")).isEqualTo(r.accounting().published()).isGreaterThanOrEqualTo(first);
    }

    @Test
    void httpSourceAnswers429WithResumePointThenAcceptsTheRest() throws Exception {
        GateSink.close("gate");
        Pipeline p = TestPipelines.create("""
                pipeline: { workers: 1, queueCapacity: 10 }
                sources:
                  - { name: http, type: http, port: 0, offerTimeout: 50ms, retryAfter: 2s }
                sinks:
                  - { name: gate, type: gate, queueCapacity: 10, batchSize: 5, overflow: block }
                """, tmp);
        p.start();
        int port = httpPort(p);
        HttpClient client = HttpClient.newHttpClient();
        String batch = "[" + IntStream.range(0, 100)
                .mapToObj(i -> "{\"type\":\"auth\",\"id\":\"e" + i + "\"}")
                .collect(Collectors.joining(",")) + ",{\"no\":\"type\"}]";

        HttpResponse<String> first = post(client, port, batch);
        assertThat(first.statusCode()).isEqualTo(429);
        assertThat(first.headers().firstValue("Retry-After")).hasValue("2");
        JsonNode body = Json.MAPPER.readTree(first.body());
        int resumeFrom = body.get("resumeFrom").asInt();
        assertThat(resumeFrom).isBetween(15, 40);
        assertThat(body.get("accepted").asInt()).isEqualTo(resumeFrom);
        assertThat(body.get("rejected").asInt()).isEqualTo(101 - resumeFrom);

        GateSink.open("gate");
        String rest = "[" + IntStream.range(resumeFrom, 100)
                .mapToObj(i -> "{\"type\":\"auth\",\"id\":\"e" + i + "\"}")
                .collect(Collectors.joining(",")) + ",{\"no\":\"type\"}]";
        HttpResponse<String> second = post(client, port, rest);
        assertThat(second.statusCode()).isEqualTo(202);
        assertThat(Json.MAPPER.readTree(second.body()).get("deadLettered").asInt()).isEqualTo(1);

        ShutdownReport r = p.stop(Duration.ofSeconds(10));
        assertThat(r.ok()).as(r.summary()).isTrue();
        assertThat(r.accounting().published()).isEqualTo(100);
        assertThat(r.accounting().rejected()).isPositive();
        assertThat(r.accounting().parseFailed()).isEqualTo(1);
    }

    @Test
    void httpRejectsWrongMethodAndOversizedBodies() throws Exception {
        Pipeline p = TestPipelines.create("""
                sources:
                  - { name: http, type: http, port: 0, maxBodyBytes: 100 }
                sinks:
                  - { name: out, type: blackhole }
                """, tmp);
        p.start();
        int port = httpPort(p);
        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> get = client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/ingest"))
                .GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(get.statusCode()).isEqualTo(405);
        assertThat(post(client, port, "[" + "{\"type\":\"a\"},".repeat(20) + "{}]").statusCode()).isEqualTo(413);
        assertThat(p.stop(Duration.ofSeconds(5)).ok()).isTrue();
    }

    private static HttpResponse<String> post(HttpClient c, int port, String body) throws Exception {
        return c.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/ingest"))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    static int httpPort(Pipeline p) {
        return ((HttpBatchSource) p.source("http")).boundPort();
    }
}
