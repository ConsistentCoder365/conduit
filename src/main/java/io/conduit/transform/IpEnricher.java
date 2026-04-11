package io.conduit.transform;

import io.conduit.config.ConfigException;
import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import io.conduit.core.Event;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds network context to IP fields from a lookup table that can change while the pipeline runs
 * (FR-5). For each configured field holding an IPv4 address, the matching row's attributes are added
 * as {@code <field>_info}, e.g. {@code src_ip_info: {zone: external, country: US}}.
 *
 * <p>The table is an immutable snapshot behind an {@link AtomicReference}. A background thread polls
 * the file's modification time and, on change, builds a whole new table and swaps it in with one
 * reference write. Workers never lock and never see a half-loaded table. A bad file is logged and the
 * previous table stays in service.
 *
 * <pre>
 * - type: enrich-ip
 *   fields: [src_ip, dst_ip]
 *   table: config/ip-table.csv
 *   reloadInterval: 10s
 * </pre>
 */
public final class IpEnricher implements Transform {
    private static final Logger log = LoggerFactory.getLogger(IpEnricher.class);

    public static final class Provider implements TransformProvider {
        @Override
        public String type() {
            return "enrich-ip";
        }

        @Override
        public Transform create(Props p, ComponentContext ctx) {
            List<String> fields = p.stringList("fields", List.of("src_ip", "dst_ip"));
            Path table = Path.of(p.string("table"));
            Duration reload = p.duration("reloadInterval", Duration.ofSeconds(10));
            String suffix = p.string("suffix", "_info");
            return new IpEnricher(fields, table, reload, suffix, ctx.meters());
        }
    }

    private final List<String> fields;
    private final List<String> outputFields;
    private final Path tablePath;
    private final Duration reloadInterval;
    private final AtomicReference<IpTable> table = new AtomicReference<>();
    private final Counter reloads;
    private final Counter reloadFailures;
    private volatile FileTime loadedMtime;
    private ScheduledExecutorService reloader;

    public IpEnricher(List<String> fields, Path tablePath, Duration reloadInterval, String suffix,
                      MeterRegistry meters) {
        this.fields = List.copyOf(fields);
        this.outputFields = fields.stream().map(f -> f + suffix).toList();
        this.tablePath = tablePath;
        this.reloadInterval = reloadInterval;
        this.reloads = Counter.builder("conduit.ip_table.reloads").register(meters);
        this.reloadFailures = Counter.builder("conduit.ip_table.reload_failures").register(meters);
        Gauge.builder("conduit.ip_table.entries", table, t -> t.get() == null ? 0 : t.get().size()).register(meters);
        try {
            loadedMtime = Files.getLastModifiedTime(tablePath);
            table.set(IpTable.load(tablePath));
        } catch (IOException | RuntimeException e) {
            throw new ConfigException("cannot load IP table " + tablePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void start() {
        if (reloadInterval.isZero() || reloadInterval.isNegative()) {
            return;
        }
        reloader = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ip-table-reloader");
            t.setDaemon(true);
            return t;
        });
        long ms = reloadInterval.toMillis();
        reloader.scheduleWithFixedDelay(this::reloadIfChanged, ms, ms, TimeUnit.MILLISECONDS);
    }

    /** Checks the file and swaps in a new snapshot if it changed. Public so tests can drive it. */
    public boolean reloadIfChanged() {
        try {
            FileTime mtime = Files.getLastModifiedTime(tablePath);
            if (mtime.equals(loadedMtime)) {
                return false;
            }
            IpTable fresh = IpTable.load(tablePath);
            table.set(fresh);
            loadedMtime = mtime;
            reloads.increment();
            log.info("reloaded IP table {} ({} entries)", tablePath, fresh.size());
            return true;
        } catch (IOException | RuntimeException e) {
            reloadFailures.increment();
            log.error("reloading IP table {} failed; keeping the previous table: {}", tablePath, e.getMessage());
            return false;
        }
    }

    @Override
    public Event apply(Event event, Chain next) {
        IpTable snapshot = table.get();
        Map<String, Object> out = null;
        for (int i = 0; i < fields.size(); i++) {
            Object ip = event.attributes().get(fields.get(i));
            if (!(ip instanceof String s)) {
                continue;
            }
            Map<String, Object> info = snapshot.lookup(s);
            if (info != null) {
                if (out == null) {
                    out = new LinkedHashMap<>(event.attributes().size() * 2);
                    out.putAll(event.attributes());
                }
                out.put(outputFields.get(i), info);
            }
        }
        return next.proceed(out == null ? event : event.withAttributes(out));
    }

    IpTable table() {
        return table.get();
    }

    @Override
    public void close() {
        if (reloader != null) {
            reloader.shutdownNow();
        }
    }
}
