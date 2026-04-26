package io.conduit.sink;

import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import io.conduit.core.Event;
import java.time.Duration;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A sink that misbehaves on purpose, for fault-injection tests, benchmarks and failure drills.
 *
 * <pre>
 * - name: chaos
 *   type: fault
 *   latency: 5ms            # added to every write
 *   failureRate: 0.2        # share of writes that throw
 *   outageAfter: 5s         # after this long, start an outage ...
 *   outageFor: 10s          # ... lasting this long (0 = forever)
 *   outageMode: fail        # fail = throw at once, hang = block until interrupted
 * </pre>
 */
public final class FaultSink implements Sink {

    public enum OutageMode { FAIL, HANG }

    public static final class Provider implements SinkProvider {
        @Override
        public String type() {
            return "fault";
        }

        @Override
        public Sink create(Props p, ComponentContext ctx) {
            return new FaultSink(p.duration("latency", Duration.ZERO), p.decimal("failureRate", 0, 0, 1),
                    p.duration("outageAfter", null), p.duration("outageFor", Duration.ZERO),
                    p.enumValue("outageMode", OutageMode.class, OutageMode.FAIL),
                    p.longValue("seed", ctx.lane() * 7919L + 1, Long.MIN_VALUE));
        }
    }

    /** Events acknowledged by every fault sink instance in this JVM, for test assertions. */
    public static final AtomicLong ACKNOWLEDGED = new AtomicLong();

    private final long latencyNanos;
    private final double failureRate;
    private final long outageAfterNanos;
    private final long outageForNanos;
    private final OutageMode outageMode;
    private final SplittableRandom random;
    private long startedAt;

    public FaultSink(Duration latency, double failureRate, Duration outageAfter, Duration outageFor,
                     OutageMode outageMode, long seed) {
        this.latencyNanos = latency.toNanos();
        this.failureRate = failureRate;
        this.outageAfterNanos = outageAfter == null ? -1 : outageAfter.toNanos();
        this.outageForNanos = outageFor.isZero() ? Long.MAX_VALUE : outageFor.toNanos();
        this.outageMode = outageMode;
        this.random = new SplittableRandom(seed);
    }

    @Override
    public void open() {
        startedAt = System.nanoTime();
    }

    @Override
    public void write(List<Event> batch) throws Exception {
        if (inOutage()) {
            if (outageMode == OutageMode.HANG) {
                Thread.sleep(Long.MAX_VALUE);
            }
            throw new java.io.IOException("injected outage");
        }
        if (latencyNanos > 0) {
            Thread.sleep(latencyNanos / 1_000_000, (int) (latencyNanos % 1_000_000));
        }
        if (failureRate > 0 && random.nextDouble() < failureRate) {
            throw new java.io.IOException("injected failure");
        }
        ACKNOWLEDGED.addAndGet(batch.size());
    }

    boolean inOutage() {
        if (outageAfterNanos < 0) {
            return false;
        }
        long elapsed = System.nanoTime() - startedAt - outageAfterNanos;
        return elapsed >= 0 && elapsed < outageForNanos;
    }
}
