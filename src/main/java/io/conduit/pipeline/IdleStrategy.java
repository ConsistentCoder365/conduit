package io.conduit.pipeline;

import java.util.concurrent.locks.LockSupport;

/**
 * What a consumer does when its queue is empty: spin briefly, then yield, then sleep in growing naps
 * of 10 µs up to 1 ms. Only after a long quiet spell does it fall back to a blocking take.
 *
 * <p>Why not just block? A consumer blocked on {@code take()} has to be woken by the producer's next
 * put, and that wake-up is a system call. When a consumer drains faster than its producer fills,
 * it blocks after almost every event, so every event pays for a wake-up and the producer slows down.
 * Measured here: 8 workers ran at a third of the speed of 4. Sleeping with {@code parkNanos} instead
 * leaves no waiter on the queue's condition, so puts stay cheap. The cost is up to 1 ms of added
 * latency on a quiet pipeline.
 */
final class IdleStrategy {
    private static final int SPINS = 64;
    private static final int YIELDS = 64;
    private static final int BLOCK_AFTER = SPINS + YIELDS + 256;
    private static final long MIN_PARK_NANOS = 10_000;
    private static final long MAX_PARK_NANOS = 1_000_000;

    private int idle;
    private long parkNanos = MIN_PARK_NANOS;

    /** Waits a little. Throws if interrupted, so forced shutdown still reaches idle consumers. */
    void idle() throws InterruptedException {
        if (idle < SPINS) {
            Thread.onSpinWait();
        } else if (idle < SPINS + YIELDS) {
            Thread.yield();
        } else {
            LockSupport.parkNanos(parkNanos);
            parkNanos = Math.min(parkNanos * 2, MAX_PARK_NANOS);
        }
        idle++;
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
    }

    /** True once the consumer has been idle long enough (roughly a quarter second) to block instead. */
    boolean shouldBlock() {
        return idle >= BLOCK_AFTER;
    }

    void reset() {
        idle = 0;
        parkNanos = MIN_PARK_NANOS;
    }
}
