package io.conduit.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import io.conduit.core.Event;
import io.conduit.source.TcpSyslogSource;
import io.conduit.testkit.CollectingSink;
import io.conduit.testkit.TestPipelines;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SourcesTest {
    @TempDir
    Path tmp;

    @AfterEach
    void reset() {
        TestPipelines.resetAll();
    }

    @Test
    void fileTailFollowsAppendsTruncationAndRotation() throws Exception {
        Path log = tmp.resolve("app.jsonl");
        Files.writeString(log, line(1) + line(2));
        // One worker: these events have distinct keys, and only per-key order is guaranteed.
        Pipeline p = TestPipelines.create("""
                pipeline: { workers: 1 }
                sources:
                  - { name: tail, type: file, path: '%s', pollInterval: 20ms }
                sinks:
                  - { name: out, type: collect, linger: 1ms }
                """.formatted(log), tmp);
        p.start();
        awaitCount(2);

        Files.writeString(log, line(3) + "{\"type\":\"auth\",\"n\":", StandardOpenOption.APPEND);
        awaitCount(3);
        Files.writeString(log, "4}\n", StandardOpenOption.APPEND);
        awaitCount(4);

        Files.writeString(log, line(5), StandardOpenOption.TRUNCATE_EXISTING);
        awaitCount(5);

        Files.move(log, tmp.resolve("app.jsonl.1"));
        Files.writeString(log, line(6) + line(7));
        awaitCount(7);

        ShutdownReport r = p.stop(Duration.ofSeconds(5));
        assertThat(r.ok()).as(r.summary()).isTrue();
        List<Object> ns = CollectingSink.events("out").stream().map(e -> e.attributes().get("n")).toList();
        assertThat(ns).containsExactly(1, 2, 3, 4, 5, 6, 7);
    }

    @Test
    void fileSourceStartingAtEndSkipsExistingLines() throws Exception {
        Path log = tmp.resolve("app.jsonl");
        Files.writeString(log, line(1));
        Pipeline p = TestPipelines.create("""
                sources:
                  - { name: tail, type: file, path: '%s', pollInterval: 20ms, fromBeginning: false }
                sinks:
                  - { name: out, type: collect, linger: 1ms }
                """.formatted(log), tmp);
        p.start();
        Thread.sleep(200);
        Files.writeString(log, line(2), StandardOpenOption.APPEND);
        awaitCount(1);
        p.stop(Duration.ofSeconds(5));
        assertThat(CollectingSink.events("out")).extracting(e -> e.attributes().get("n")).containsExactly(2);
    }

    @Test
    void tcpSourceAcceptsSeveralConnectionsAndDeadLettersBadLines() throws Exception {
        Pipeline p = TestPipelines.create("""
                sources:
                  - { name: syslog, type: tcp, port: 0, host: 127.0.0.1, keyField: user }
                sinks:
                  - { name: out, type: collect, linger: 1ms }
                """, tmp);
        p.start();
        int port = ((TcpSyslogSource) p.source("syslog")).boundPort();
        try (Socket a = new Socket("127.0.0.1", port); Socket b = new Socket("127.0.0.1", port)) {
            send(a, "<134>1 2026-10-25T12:00:00Z web-1 sshd - auth [m user=\"carol\"] ok\n");
            send(b, "<13>{\"type\":\"process\",\"user\":\"dave\"}\n<999>bad\n");
            send(a, "free text type=network src_ip=10.0.0.1\n");
        }
        awaitCount(3);
        ShutdownReport r = p.stop(Duration.ofSeconds(5));
        assertThat(r.ok()).as(r.summary()).isTrue();
        assertThat(r.accounting().parseFailed()).isEqualTo(1);
        assertThat(CollectingSink.events("out")).extracting(Event::type)
                .containsExactlyInAnyOrder("auth", "process", "network");
        assertThat(CollectingSink.events("out")).filteredOn(e -> e.type().equals("auth"))
                .singleElement().satisfies(e -> assertThat(e.key()).isEqualTo("carol"));
    }

    private static void send(Socket s, String text) throws Exception {
        OutputStream out = s.getOutputStream();
        out.write(text.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private static String line(int n) {
        return "{\"type\":\"auth\",\"n\":" + n + "}\n";
    }

    private static void awaitCount(int n) throws InterruptedException {
        await(() -> CollectingSink.events("out").size() >= n, "expected " + n + " events");
    }

    static void await(BooleanSupplier cond, String what) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!cond.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out: " + what);
            }
            Thread.sleep(10);
        }
    }
}
