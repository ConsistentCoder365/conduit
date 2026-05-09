package io.conduit.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import io.conduit.observe.Accounting;
import io.conduit.testkit.CollectingSink;
import io.conduit.testkit.TestPipelines;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Sinks that throw, flap, slow down and hang. In every case the books must balance, and a broken
 * sink must not stop events reaching the healthy one.
 */
class FaultInjectionTest {
    @TempDir
    Path tmp;

    @AfterEach
    void reset() {
        TestPipelines.resetAll();
    }

    private static final String TWO_SINKS = """
            pipeline: { workers: 4, queueCapacity: 1024 }
            sources:
              - { name: synth, type: synthetic, rate: 0, count: %d, seed: 11 }
            routes:
              rules:
                - { name: everything, when: { always: true }, to: [healthy, broken] }
              default: [healthy]
            sinks:
              - { name: healthy, type: collect }
              - name: broken
                type: fault
                %s
                queueCapacity: 512
                batchSize: 100
                writeTimeout: 200ms
                offerTimeout: 50ms
                retry: { maxAttempts: 3, initialBackoff: 1ms, maxBackoff: 5ms }
                circuitBreaker: { failureThreshold: 2, openFor: 500ms }
            """;

    @Test
    void alwaysFailingSinkDeadLettersEverythingAndTheHealthySinkGetsAll() throws Exception {
        ShutdownReport r = TestPipelines.runClean(TWO_SINKS.formatted(20_000, "failureRate: 1.0"), tmp);
        Accounting.Snapshot a = r.accounting();
        assertThat(a.routed()).isEqualTo(20_000);
        assertThat(a.deliveries()).isEqualTo(40_000);
        assertThat(CollectingSink.events("healthy")).hasSize(20_000);
        assertThat(a.sinkFailed() + a.sinkOverflow()).isEqualTo(20_000);
        assertThat(a.deadLetterWritten()).isEqualTo(20_000);
    }

    @Test
    void flakySinkRecoversThroughRetries() throws Exception {
        ShutdownReport r = TestPipelines.runClean(TWO_SINKS.formatted(20_000, "failureRate: 0.3"), tmp);
        Accounting.Snapshot a = r.accounting();
        assertThat(CollectingSink.events("healthy")).hasSize(20_000);
        // 30% per attempt, 3 attempts: about 2.7% of batches exhaust retries; most get through.
        assertThat(a.published() - 20_000).as("fault sink acknowledged most deliveries").isGreaterThan(15_000);
        assertThat(a.balanced()).isTrue();
    }

    @Test
    @Timeout(60)
    void hangingSinkIsTimedOutAndShedSoTheHealthySinkKeepsFlowing() throws Exception {
        ShutdownReport r = TestPipelines.runClean(TWO_SINKS.formatted(200_000,
                "outageAfter: 0ms\n    outageMode: hang"), tmp);
        Accounting.Snapshot a = r.accounting();
        assertThat(CollectingSink.events("healthy")).hasSize(200_000);
        assertThat(a.sinkFailed() + a.sinkOverflow()).isEqualTo(200_000);
        assertThat(a.sinkOverflow()).as("queue filled while writes hung, so excess was shed").isPositive();
        assertThat(r.took()).isLessThan(Duration.ofSeconds(10));
    }

    @Test
    void outageMidRunThenRecovery() throws Exception {
        ShutdownReport r = TestPipelines.runClean("""
                pipeline: { workers: 4 }
                sources:
                  - { name: synth, type: synthetic, rate: 50000, count: 100000, seed: 5 }
                sinks:
                  - name: db
                    type: fault
                    outageAfter: 500ms
                    outageFor: 700ms
                    retry: { maxAttempts: 2, initialBackoff: 1ms, maxBackoff: 2ms }
                    circuitBreaker: { failureThreshold: 2, openFor: 200ms }
                """, tmp);
        Accounting.Snapshot a = r.accounting();
        assertThat(a.sinkFailed()).as("outage window was dead-lettered").isPositive();
        assertThat(a.published()).as("before and after the outage got through").isGreaterThan(50_000);
        assertThat(a.published() + a.sinkFailed() + a.sinkOverflow()).isEqualTo(100_000);
    }

    @Test
    void slowSinkWithBlockingOverflowAppliesBackpressureInsteadOfShedding() throws Exception {
        ShutdownReport r = TestPipelines.runClean("""
                pipeline: { workers: 2, queueCapacity: 64 }
                sources:
                  - { name: synth, type: synthetic, rate: 0, count: 5000, seed: 9 }
                sinks:
                  - { name: slow, type: fault, latency: 2ms, batchSize: 50, queueCapacity: 100, overflow: block }
                """, tmp);
        assertThat(r.accounting().published()).isEqualTo(5000);
        assertThat(r.accounting().deadLettered()).isZero();
    }
}
