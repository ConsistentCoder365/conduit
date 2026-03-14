package io.conduit.sink;

import io.conduit.core.Event;
import java.util.List;

/** Base for decorators: forwards everything to the wrapped sink. */
public abstract class ForwardingSink implements Sink {
    protected final Sink delegate;

    protected ForwardingSink(Sink delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open() throws Exception {
        delegate.open();
    }

    @Override
    public void write(List<Event> batch) throws Exception {
        delegate.write(batch);
    }

    @Override
    public void flush() throws Exception {
        delegate.flush();
    }

    @Override
    public boolean isRetryable(Exception error) {
        return delegate.isRetryable(error);
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    /** The undecorated sink at the bottom of the stack. */
    public static Sink unwrap(Sink sink) {
        Sink s = sink;
        while (s instanceof ForwardingSink f) {
            s = f.delegate;
        }
        return s;
    }

    /** Finds a decorator of the given type in the stack, if present. */
    public static <T extends Sink> T find(Sink sink, Class<T> type) {
        Sink s = sink;
        while (s != null) {
            if (type.isInstance(s)) {
                return type.cast(s);
            }
            s = s instanceof ForwardingSink f ? f.delegate : null;
        }
        return null;
    }
}
