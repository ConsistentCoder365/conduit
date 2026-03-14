package io.conduit.sink;

import io.conduit.core.Event;
import java.time.Clock;
import java.time.Duration;
import java.util.List;

/**
 * Stops hammering a sink that keeps failing. After {@code failureThreshold} consecutive batches fail
 * (each already retried), the circuit opens and writes fail at once without touching the sink, so
 * the lane drains to dead-letter at full speed instead of sleeping through backoffs while its queue
 * backs up. After {@code openFor} one trial batch is let through (half-open): success closes the
 * circuit, failure opens it again.
 *
 * <p>Written by one lane thread; {@code state} is volatile so {@code /health} can read it.
 */
public final class CircuitBreakerSink extends ForwardingSink {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    public interface Listener {
        void onTransition(State from, State to);
    }

    public static final class OpenException extends Exception {
        public OpenException(String message) {
            super(message);
        }
    }

    private final int failureThreshold;
    private final long openForMillis;
    private final Clock clock;
    private final Listener listener;
    private volatile State state = State.CLOSED;
    private int consecutiveFailures;
    private long openedAt;

    public CircuitBreakerSink(Sink delegate, int failureThreshold, Duration openFor, Clock clock, Listener listener) {
        super(delegate);
        this.failureThreshold = failureThreshold;
        this.openForMillis = openFor.toMillis();
        this.clock = clock;
        this.listener = listener;
    }

    public State state() {
        return state;
    }

    @Override
    public void write(List<Event> batch) throws Exception {
        if (state == State.OPEN) {
            if (clock.millis() - openedAt < openForMillis) {
                throw new OpenException("circuit open");
            }
            transition(State.HALF_OPEN);
        }
        try {
            delegate.write(batch);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            consecutiveFailures++;
            if (state == State.HALF_OPEN || consecutiveFailures >= failureThreshold) {
                openedAt = clock.millis();
                transition(State.OPEN);
            }
            throw e;
        }
        consecutiveFailures = 0;
        if (state != State.CLOSED) {
            transition(State.CLOSED);
        }
    }

    private void transition(State to) {
        State from = state;
        if (from != to) {
            state = to;
            listener.onTransition(from, to);
        }
    }
}
