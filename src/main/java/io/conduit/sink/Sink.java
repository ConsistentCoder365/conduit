package io.conduit.sink;

import io.conduit.core.Event;
import java.util.List;

/**
 * A publishing destination. The core creates one instance per sink lane and only ever calls an
 * instance from that lane's worker thread, so implementations need no locking of their own.
 *
 * <p>Retries, circuit breaking, timeouts and metrics are not a sink's job: the core wraps every sink
 * in decorators for those. A sink just writes, and throws when it cannot.
 */
public interface Sink extends AutoCloseable {

    /** Connects or prepares storage. A failure here stops startup ("unreachable sink"). */
    default void open() throws Exception {
    }

    /** Writes the batch. Returning normally acknowledges every event in it; throwing fails all of it. */
    void write(List<Event> batch) throws Exception;

    /** Pushes any buffered data down to the destination. Called at shutdown. */
    default void flush() throws Exception {
    }

    /** False for errors that will fail again however often they are retried (bad data, not outages). */
    default boolean isRetryable(Exception error) {
        return true;
    }

    /**
     * Called after a write timed out and was abandoned. Drop connections or other state the stuck
     * call may still be holding, so the next attempt starts clean.
     */
    default void reset() {
    }

    @Override
    default void close() throws Exception {
    }
}
