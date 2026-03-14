package io.conduit.pipeline;

import io.conduit.config.SinkSettings;
import io.conduit.core.DeadLetter;
import io.conduit.core.Event;
import io.conduit.sink.CircuitBreakerSink;
import io.conduit.sink.Sink;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * One sink's bulkhead: its own lanes, each with a worker thread and a decorated sink instance.
 * Nothing here is shared with any other sink, so a sink that slows down or dies only fills its own
 * queues.
 *
 * <p>Each lane has one bounded input queue per processing worker rather than one shared queue. With
 * a shared queue, every worker contended for its single lock on every event; measured with 8 workers,
 * half of all worker time was spent parked on that lock while the sink thread sat idle. Private
 * inputs make each hand-off single-producer. Per-key order survives because a key always comes from
 * the same worker, and so through the same input.
 *
 * <p>When an input stays full past {@code offerTimeout}, the lane is marked saturated and further
 * events for it are shed to dead-letter immediately instead of each waiting out the timeout. That
 * caps how long a dead sink can hold up a processing worker to one timeout per episode, which is what
 * keeps the other sinks flowing. The mark clears once the lane drains below half.
 */
final class SinkChannel {

    static final class Lane {
        final int index;
        final List<ArrayBlockingQueue<Event>> inputs;
        final int capacity;
        final Sink sink;
        final CircuitBreakerSink breaker;
        volatile boolean saturated;

        Lane(int index, int capacity, int producers, Sink sink, CircuitBreakerSink breaker) {
            this.index = index;
            int perInput = Math.max(1, (capacity + producers - 1) / producers);
            List<ArrayBlockingQueue<Event>> qs = new ArrayList<>(producers);
            for (int i = 0; i < producers; i++) {
                qs.add(new ArrayBlockingQueue<>(perInput));
            }
            this.inputs = List.copyOf(qs);
            this.capacity = perInput * producers;
            this.sink = sink;
            this.breaker = breaker;
        }

        int depth() {
            int d = 0;
            for (ArrayBlockingQueue<Event> q : inputs) {
                d += q.size();
            }
            return d;
        }

        void clearSaturationIfDrained() {
            if (saturated && depth() < capacity / 2) {
                saturated = false;
            }
        }
    }

    final String name;
    final String type;
    final SinkSettings settings;
    final List<Lane> lanes;
    private final DeadLetterQueue deadLetters;
    private final long offerTimeoutNanos;
    volatile String lastError;
    volatile long lastSuccessMillis;

    SinkChannel(String name, String type, SinkSettings settings, List<Lane> lanes, DeadLetterQueue deadLetters) {
        this.name = name;
        this.type = type;
        this.settings = settings;
        this.lanes = List.copyOf(lanes);
        this.deadLetters = deadLetters;
        this.offerTimeoutNanos = settings.offerTimeout().toNanos();
    }

    /**
     * Called by processing worker {@code producer} only, so each input queue has a single producer.
     * Never throws except on a forced-shutdown interrupt.
     */
    void offer(Event event, int producer) throws InterruptedException {
        Lane lane = lanes.get(Lanes.index(event.key(), lanes.size()));
        ArrayBlockingQueue<Event> q = lane.inputs.get(producer);
        if (q.offer(event)) {
            return;
        }
        if (settings.overflow() == SinkSettings.Overflow.BLOCK) {
            q.put(event);
            return;
        }
        if (!lane.saturated && q.offer(event, offerTimeoutNanos, TimeUnit.NANOSECONDS)) {
            return;
        }
        lane.saturated = true;
        deadLetters.submit(DeadLetter.ofEvent(DeadLetter.Stage.SINK_OVERFLOW, name,
                "sink queue full for longer than offerTimeout (" + settings.offerTimeout().toMillis() + " ms)",
                event, 0));
    }
}
