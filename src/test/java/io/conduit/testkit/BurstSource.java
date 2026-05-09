package io.conduit.testkit;

import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import io.conduit.core.Event;
import io.conduit.core.Severity;
import io.conduit.source.Source;
import io.conduit.source.SourceContext;
import io.conduit.source.SourceProvider;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Emits {@code count} events as fast as possible after a shared start latch opens, so many burst
 * sources (each on its own thread) hit the pipeline at the same instant. Each event carries its
 * per-key sequence number in {@code seq} so tests can check ordering.
 */
public final class BurstSource implements Source {

    public static final class Provider implements SourceProvider {
        @Override
        public String type() {
            return "burst";
        }

        @Override
        public Source create(Props p, ComponentContext ctx) {
            return new BurstSource(ctx.name(), p.longValue("count", 1000, 0), p.integer("keys", 16, 1),
                    p.string("latch", null), p.string("eventType", "auth"));
        }
    }

    private static final Map<String, CountDownLatch> LATCHES = new ConcurrentHashMap<>();

    public static CountDownLatch latch(String name) {
        return LATCHES.computeIfAbsent(name, k -> new CountDownLatch(1));
    }

    public static void clearAll() {
        LATCHES.values().forEach(CountDownLatch::countDown);
        LATCHES.clear();
    }

    private final String name;
    private final long count;
    private final int keys;
    private final String latch;
    private final String type;
    private volatile boolean running = true;

    BurstSource(String name, long count, int keys, String latch, String type) {
        this.name = name;
        this.count = count;
        this.keys = keys;
        this.latch = latch;
        this.type = type;
    }

    @Override
    public void run(SourceContext ctx) throws InterruptedException {
        if (latch != null) {
            latch(latch).await();
        }
        long[] seqByKey = new long[keys];
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        for (long i = 0; running && (count == 0 || i < count); i++) {
            int k = (int) (i % keys);
            String key = name + "/k" + k;
            ctx.emit(new Event(name + "-" + i, now, name, type, Severity.values()[(int) (i % 5)], key,
                    Map.of("seq", seqByKey[k]++, "user", "u" + k, "src_ip", "10.0.0." + (k % 250),
                            "host", "h" + (k % 7)),
                    System.nanoTime()));
        }
    }

    @Override
    public void stop() {
        running = false;
    }
}
