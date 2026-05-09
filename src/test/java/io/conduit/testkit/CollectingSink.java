package io.conduit.testkit;

import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import io.conduit.core.Event;
import io.conduit.sink.Sink;
import io.conduit.sink.SinkProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/** Keeps every acknowledged event in memory, per sink name and in write order per lane. */
public final class CollectingSink implements Sink {

    public static final class Provider implements SinkProvider {
        @Override
        public String type() {
            return "collect";
        }

        @Override
        public Sink create(Props p, ComponentContext ctx) {
            return new CollectingSink(ctx.name());
        }
    }

    private static final Map<String, ConcurrentLinkedQueue<Event>> BY_SINK = new ConcurrentHashMap<>();

    private final ConcurrentLinkedQueue<Event> out;

    CollectingSink(String name) {
        this.out = BY_SINK.computeIfAbsent(name, k -> new ConcurrentLinkedQueue<>());
    }

    @Override
    public void write(List<Event> batch) {
        out.addAll(batch);
    }

    public static List<Event> events(String sink) {
        ConcurrentLinkedQueue<Event> q = BY_SINK.get(sink);
        return q == null ? List.of() : new ArrayList<>(q);
    }

    public static void clearAll() {
        BY_SINK.clear();
    }
}
