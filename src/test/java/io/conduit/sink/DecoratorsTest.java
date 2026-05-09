package io.conduit.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.conduit.core.Event;
import io.conduit.testkit.Events;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DecoratorsTest {
    private static final List<Event> BATCH = List.of(Events.of("auth"));
    private static final RetryPolicy FAST = new RetryPolicy(4, Duration.ofMillis(1), Duration.ofMillis(4), 2.0);

    /** Fails the first {@code failures} writes, then succeeds. */
    static final class Flaky implements Sink {
        final AtomicInteger calls = new AtomicInteger();
        final int failures;
        final Exception error;

        Flaky(int failures, Exception error) {
            this.failures = failures;
            this.error = error;
        }

        @Override
        public void write(List<Event> batch) throws Exception {
            if (calls.incrementAndGet() <= failures) {
                throw error;
            }
        }

        @Override
        public boolean isRetryable(Exception e) {
            return !(e instanceof IllegalArgumentException);
        }
    }

    @Test
    void retrySucceedsWithinTheAttemptBudget() throws Exception {
        Flaky sink = new Flaky(3, new IOException("down"));
        List<Integer> attempts = new ArrayList<>();
        new RetryingSink(sink, FAST, (n, e) -> attempts.add(n)).write(BATCH);
        assertThat(sink.calls).hasValue(4);
        assertThat(attempts).containsExactly(1, 2, 3);
    }

    @Test
    void retryGivesUpAfterMaxAttempts() {
        Flaky sink = new Flaky(10, new IOException("down"));
        assertThatThrownBy(() -> new RetryingSink(sink, FAST, (n, e) -> { }).write(BATCH))
                .isInstanceOf(SinkWriteException.class).hasMessageContaining("gave up after 4 attempts")
                .extracting(e -> ((SinkWriteException) e).attempts()).isEqualTo(4);
        assertThat(sink.calls).hasValue(4);
    }

    @Test
    void retryStopsAtOnceOnNonRetryableErrors() {
        Flaky sink = new Flaky(10, new IllegalArgumentException("bad row"));
        assertThatThrownBy(() -> new RetryingSink(sink, FAST, (n, e) -> { }).write(BATCH))
                .isInstanceOf(SinkWriteException.class).hasMessageContaining("non-retryable");
        assertThat(sink.calls).hasValue(1);
    }

    @Test
    void backoffGrowsExponentiallyWithJitterAndIsCapped() {
        RetryPolicy p = new RetryPolicy(10, Duration.ofMillis(100), Duration.ofSeconds(1), 2.0);
        for (int i = 0; i < 200; i++) {
            assertThat(p.backoffMillis(1)).isBetween(50L, 100L);
            assertThat(p.backoffMillis(3)).isBetween(200L, 400L);
            assertThat(p.backoffMillis(9)).isBetween(500L, 1000L);
        }
    }

    static final class MutableClock extends Clock {
        long millis = Instant.parse("2026-10-25T12:00:00Z").toEpochMilli();

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }
    }

    @Test
    void breakerOpensAfterThresholdFailsFastThenHalfOpensAndCloses() throws Exception {
        Flaky sink = new Flaky(4, new IOException("down"));
        MutableClock clock = new MutableClock();
        List<String> transitions = new ArrayList<>();
        CircuitBreakerSink cb = new CircuitBreakerSink(sink, 3, Duration.ofSeconds(10), clock,
                (from, to) -> transitions.add(from + "->" + to));

        for (int i = 0; i < 3; i++) {
            assertThatThrownBy(() -> cb.write(BATCH)).isInstanceOf(IOException.class);
        }
        assertThat(cb.state()).isEqualTo(CircuitBreakerSink.State.OPEN);
        assertThatThrownBy(() -> cb.write(BATCH)).isInstanceOf(CircuitBreakerSink.OpenException.class);
        assertThat(sink.calls).as("open circuit does not touch the sink").hasValue(3);

        clock.millis += 10_000;
        assertThatThrownBy(() -> cb.write(BATCH)).isInstanceOf(IOException.class);
        assertThat(cb.state()).as("failed trial reopens").isEqualTo(CircuitBreakerSink.State.OPEN);

        clock.millis += 10_000;
        cb.write(BATCH);
        assertThat(cb.state()).isEqualTo(CircuitBreakerSink.State.CLOSED);
        assertThat(transitions).containsExactly("CLOSED->OPEN", "OPEN->HALF_OPEN", "HALF_OPEN->OPEN",
                "OPEN->HALF_OPEN", "HALF_OPEN->CLOSED");
    }

    @Test
    void timeLimitAbandonsAHangingWriteAndResetsTheSink() {
        AtomicInteger resets = new AtomicInteger();
        Sink hanging = new Sink() {
            @Override
            public void write(List<Event> batch) throws InterruptedException {
                Thread.sleep(Long.MAX_VALUE);
            }

            @Override
            public void reset() {
                resets.incrementAndGet();
            }
        };
        TimeLimitedSink t = new TimeLimitedSink(hanging, Duration.ofMillis(50), "test");
        long start = System.nanoTime();
        assertThatThrownBy(() -> t.write(BATCH)).isInstanceOf(TimeoutException.class);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
        assertThat(resets).hasValue(1);
    }

    @Test
    void timeLimitPassesThroughResultsAndErrors() throws Exception {
        TimeLimitedSink ok = new TimeLimitedSink(b -> { }, Duration.ofSeconds(1), "test");
        ok.write(BATCH);
        TimeLimitedSink failing = new TimeLimitedSink(new Flaky(1, new IOException("x")), Duration.ofSeconds(1), "t");
        assertThatThrownBy(() -> failing.write(BATCH)).isInstanceOf(IOException.class).hasMessage("x");
    }

    @Test
    void meteredRecordsAttemptsByOutcome() throws Exception {
        SimpleMeterRegistry reg = new SimpleMeterRegistry();
        MeteredSink m = new MeteredSink(new Flaky(1, new IOException("x")), reg, "s");
        assertThatThrownBy(() -> m.write(BATCH)).isInstanceOf(IOException.class);
        m.write(BATCH);
        assertThat(reg.get("conduit.sink.write").tag("outcome", "failure").timer().count()).isEqualTo(1);
        assertThat(reg.get("conduit.sink.write").tag("outcome", "success").timer().count()).isEqualTo(1);
        assertThat(ForwardingSink.unwrap(m)).isInstanceOf(Flaky.class);
    }
}
