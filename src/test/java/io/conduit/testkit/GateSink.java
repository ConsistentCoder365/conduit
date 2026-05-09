package io.conduit.testkit;

import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import io.conduit.core.Event;
import io.conduit.sink.Sink;
import io.conduit.sink.SinkProvider;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/** Blocks every write until the test opens its gate. Lets tests freeze a sink to fill queues. */
public final class GateSink implements Sink {

    public static final class Provider implements SinkProvider {
        @Override
        public String type() {
            return "gate";
        }

        @Override
        public Sink create(Props p, ComponentContext ctx) {
            return new GateSink(ctx.name());
        }
    }

    private static final Map<String, CountDownLatch> GATES = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> WRITTEN = new ConcurrentHashMap<>();

    private final String name;

    GateSink(String name) {
        this.name = name;
    }

    @Override
    public void write(List<Event> batch) throws InterruptedException {
        GATES.computeIfAbsent(name, k -> new CountDownLatch(1)).await();
        WRITTEN.computeIfAbsent(name, k -> new AtomicLong()).addAndGet(batch.size());
    }

    public static void close(String name) {
        GATES.put(name, new CountDownLatch(1));
    }

    public static void open(String name) {
        GATES.computeIfAbsent(name, k -> new CountDownLatch(1)).countDown();
    }

    public static long written(String name) {
        AtomicLong n = WRITTEN.get(name);
        return n == null ? 0 : n.get();
    }

    public static void clearAll() {
        GATES.values().forEach(CountDownLatch::countDown);
        GATES.clear();
        WRITTEN.clear();
    }
}
