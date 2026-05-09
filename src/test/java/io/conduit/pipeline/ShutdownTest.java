package io.conduit.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import io.conduit.testkit.GateSink;
import io.conduit.testkit.TestPipelines;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ShutdownTest {
    @TempDir
    Path tmp;

    @AfterEach
    void reset() {
        TestPipelines.resetAll();
    }

    @Test
    void concurrentStopCallsShareOneReport() throws Exception {
        Pipeline p = TestPipelines.create("""
                sources:
                  - { name: synth, type: synthetic, rate: 0 }
                sinks:
                  - { name: out, type: blackhole }
                """, tmp);
        p.start();
        Thread.sleep(100);
        CompletableFuture<ShutdownReport> a = CompletableFuture.supplyAsync(() -> p.stop(Duration.ofSeconds(10)));
        CompletableFuture<ShutdownReport> b = CompletableFuture.supplyAsync(() -> p.stop(Duration.ofSeconds(10)));
        assertThat(a.get(15, TimeUnit.SECONDS)).isSameAs(b.get(15, TimeUnit.SECONDS));
        assertThat(a.get().ok()).as(a.get().summary()).isTrue();
        assertThat(p.awaitCompletion()).isSameAs(a.get());
    }

    @Test
    void stuckSinkPastTheDeadlineIsForcedAndReportedAsFailed() throws Exception {
        GateSink.close("stuck");
        Pipeline p = TestPipelines.create("""
                pipeline: { workers: 2, queueCapacity: 50 }
                sources:
                  - { name: synth, type: synthetic, rate: 0 }
                sinks:
                  - { name: stuck, type: gate, queueCapacity: 50, batchSize: 10, overflow: block,
                      writeTimeout: 1h }
                """, tmp);
        p.start();
        Thread.sleep(200);
        long start = System.nanoTime();
        ShutdownReport r = p.stop(Duration.ofMillis(500));
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(8));
        assertThat(r.forced()).isTrue();
        assertThat(r.ok()).isFalse();
        assertThat(r.summary()).contains("SHUTDOWN CHECK FAILED");
    }

    @Test
    void sourcesFinishingTriggerAnAutomaticDrain() throws Exception {
        Pipeline p = TestPipelines.create("""
                sources:
                  - { name: a, type: synthetic, rate: 0, count: 1000 }
                  - { name: b, type: burst, count: 500 }
                sinks:
                  - { name: out, type: blackhole }
                """, tmp);
        ShutdownReport r = TestPipelines.runToCompletion(p, Duration.ofSeconds(20));
        assertThat(r.ok()).isTrue();
        assertThat(r.accounting().published()).isEqualTo(1500);
    }
}
