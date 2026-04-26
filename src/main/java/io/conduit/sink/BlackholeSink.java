package io.conduit.sink;

import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import io.conduit.core.Event;
import java.util.List;

/** Acknowledges and discards. For measuring the pipeline without any destination cost. */
public final class BlackholeSink implements Sink {

    public static final class Provider implements SinkProvider {
        @Override
        public String type() {
            return "blackhole";
        }

        @Override
        public Sink create(Props p, ComponentContext ctx) {
            return new BlackholeSink();
        }
    }

    @Override
    public void write(List<Event> batch) {
        // intentionally empty
    }
}
