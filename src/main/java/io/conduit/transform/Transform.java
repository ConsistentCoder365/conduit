package io.conduit.transform;

import io.conduit.core.Event;

/**
 * One step in the transform chain (chain of responsibility). A step can change the event and pass it
 * on with {@code next.proceed(event)}, pass it on untouched, or stop the chain by returning null,
 * which drops the event.
 *
 * <p>Steps are shared by every processing worker, so {@link #apply} must be thread-safe.
 */
public interface Transform extends AutoCloseable {

    Event apply(Event event, Chain next);

    /** Starts background work such as file reloading. Called once before events flow. */
    default void start() {
    }

    @Override
    default void close() {
    }

    @FunctionalInterface
    interface Chain {
        Event proceed(Event event);
    }
}
