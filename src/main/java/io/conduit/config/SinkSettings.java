package io.conduit.config;

import io.conduit.sink.RetryPolicy;
import java.time.Duration;

/**
 * Per-sink settings the core applies to every sink regardless of type: its bulkhead (lanes and
 * queue), batching, load shedding, and the resilience decorators.
 *
 * @param workers        lanes, each with its own queue, worker thread and sink instance
 * @param queueCapacity  capacity of each lane's queue
 * @param batchSize      most events per write
 * @param linger         longest a partial batch waits before it is written anyway
 * @param offerTimeout   how long a processing worker waits on a full sink queue before shedding
 * @param overflow       what happens after that wait
 * @param writeTimeout   longest a single write attempt may take before it is abandoned
 */
public record SinkSettings(int workers, int queueCapacity, int batchSize, Duration linger, Duration offerTimeout,
                           Overflow overflow, Duration writeTimeout, RetryPolicy retry, Breaker breaker) {

    public enum Overflow {
        /** Shed to dead-letter so one stuck sink cannot stall routing to the others (the bulkhead). */
        DEAD_LETTER,
        /** Wait for space. Backpressure reaches the sources, and every other sink slows down too. */
        BLOCK
    }

    public record Breaker(boolean enabled, int failureThreshold, Duration openFor) {
    }
}
