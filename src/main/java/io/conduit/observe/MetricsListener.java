package io.conduit.observe;

import io.conduit.core.DeadLetter;
import io.conduit.core.Event;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Exports the event flow as Micrometer meters (served as Prometheus text on {@code /metrics}).
 * Meters are looked up once per tag value and cached in lock-free maps.
 */
public final class MetricsListener implements PipelineListener {
    static final int LATENCY_SAMPLES_PER_BATCH = 64;

    private final MeterRegistry registry;
    private final Map<String, Counter> ingested = new ConcurrentHashMap<>();
    private final Map<String, Counter> rejected = new ConcurrentHashMap<>();
    private final Map<String, Counter> published = new ConcurrentHashMap<>();
    private final Map<String, Counter> sinkErrors = new ConcurrentHashMap<>();
    private final Map<String, Counter> deadLettered = new ConcurrentHashMap<>();
    private final Map<String, Timer> latency = new ConcurrentHashMap<>();
    private final Counter filtered;
    private final Counter routed;
    private final Counter deliveries;
    private final Counter deadLetterWritten;

    public MetricsListener(MeterRegistry registry) {
        this.registry = registry;
        this.filtered = Counter.builder("conduit.events.filtered")
                .description("Events dropped by the filter stage").register(registry);
        this.routed = Counter.builder("conduit.events.routed")
                .description("Events that reached the router").register(registry);
        this.deliveries = Counter.builder("conduit.events.deliveries")
                .description("Event-to-sink deliveries the router scheduled (fan-out counts each target)")
                .register(registry);
        this.deadLetterWritten = Counter.builder("conduit.deadletter.written")
                .description("Dead-letter records written to disk").register(registry);
    }

    /** The end-to-end latency timer for a sink: ingest to sink acknowledgement. */
    public Timer latency(String sink) {
        return latency.computeIfAbsent(sink, s -> Timer.builder("conduit.event.latency")
                .description("Time from ingest to sink acknowledgement (sampled, see onPublished)")
                .tag("sink", s)
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(30))
                .register(registry));
    }

    @Override
    public void onIngested(String source) {
        ingested.computeIfAbsent(source, s -> Counter.builder("conduit.source.records")
                .description("Records ingested, including malformed ones").tag("source", s).register(registry))
                .increment();
    }

    @Override
    public void onRejected(String source) {
        rejected.computeIfAbsent(source, s -> Counter.builder("conduit.source.rejected")
                .description("Records refused because the pipeline was full (backpressure)").tag("source", s)
                .register(registry)).increment();
    }

    @Override
    public void onFiltered(Event event) {
        filtered.increment();
    }

    @Override
    public void onRouted(Event event, int targets) {
        routed.increment();
        deliveries.increment(targets);
    }

    /**
     * Records latency for an evenly spaced sample of at most {@value #LATENCY_SAMPLES_PER_BATCH} events
     * per batch. Recording every event into the percentile histogram cost more than the rest of the
     * sink thread's work combined and capped throughput; a uniform sample of hundreds of thousands
     * of events per second gives the same percentiles. Event counts come from
     * {@code conduit.sink.published}, not from this timer's count.
     */
    @Override
    public void onPublished(String sink, List<Event> batch, long ackNanos) {
        published.computeIfAbsent(sink, s -> Counter.builder("conduit.sink.published")
                .description("Events acknowledged by the sink").tag("sink", s).register(registry))
                .increment(batch.size());
        Timer t = latency(sink);
        int step = Math.max(1, batch.size() / LATENCY_SAMPLES_PER_BATCH);
        for (int i = step / 2; i < batch.size(); i += step) {
            t.record(ackNanos - batch.get(i).ingestNanos(), TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public void onDeadLettered(DeadLetter.Stage stage, String origin, String reason, int count) {
        String k = stage.name() + '|' + origin;
        deadLettered.computeIfAbsent(k, x -> Counter.builder("conduit.deadletter.records")
                .description("Records or deliveries sent to dead-letter")
                .tag("stage", stage.name().toLowerCase())
                .tag("origin", origin)
                .register(registry)).increment(count);
    }

    @Override
    public void onDeadLetterWritten(int count) {
        deadLetterWritten.increment(count);
    }

    @Override
    public void onSinkError(String sink, Throwable error) {
        sinkErrors.computeIfAbsent(sink, s -> Counter.builder("conduit.sink.errors")
                .description("Failed sink write attempts (before retries are exhausted)").tag("sink", s)
                .register(registry)).increment();
    }
}
