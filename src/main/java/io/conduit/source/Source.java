package io.conduit.source;

/**
 * An ingestion source. Each source gets exactly one dedicated thread, which calls {@link #run}.
 *
 * <p>{@code run} returns when the input is exhausted (a finite file, a synthetic count) or after
 * {@link #stop()}. When it returns, the source must have no emits still in flight on any helper
 * thread: the pipeline relies on that to know every lane has received its last event.
 */
public interface Source extends AutoCloseable {

    /** Binds ports or opens files. A failure here stops startup. Called on the startup thread. */
    default void open() throws Exception {
    }

    /** Reads input and emits it until the input ends or {@link #stop()} is called. */
    void run(SourceContext context) throws Exception;

    /** Asks {@link #run} to return soon. Called from another thread; must not block for long. */
    void stop();

    @Override
    default void close() throws Exception {
    }
}
