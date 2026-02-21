package io.conduit.sink;

import io.conduit.core.Provider;

/**
 * Registered in {@code META-INF/services/io.conduit.sink.SinkProvider}. Called once per lane; use
 * {@link io.conduit.core.ComponentContext#lane()} when lanes must not share a resource such as a file.
 */
public interface SinkProvider extends Provider<Sink> {
}
