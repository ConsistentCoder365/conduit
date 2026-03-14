package io.conduit.pipeline;

import io.conduit.config.ComponentSpec;
import io.conduit.config.ConduitConfig;
import io.conduit.config.ConduitConfig.SinkSpec;
import io.conduit.config.ConduitConfig.SourceSpec;
import io.conduit.config.ConfigException;
import io.conduit.config.Props;
import io.conduit.config.Registries;
import io.conduit.config.SinkSettings;
import io.conduit.core.ComponentContext;
import io.conduit.core.DeadLetter;
import io.conduit.core.Event;
import io.conduit.core.EventParser;
import io.conduit.observe.Accounting;
import io.conduit.observe.LoggingListener;
import io.conduit.observe.MetricsListener;
import io.conduit.observe.PipelineListener;
import io.conduit.route.Router;
import io.conduit.sink.CircuitBreakerSink;
import io.conduit.sink.MeteredSink;
import io.conduit.sink.RetryingSink;
import io.conduit.sink.Sink;
import io.conduit.sink.TimeLimitedSink;
import io.conduit.source.Source;
import io.conduit.source.SourceContext;
import io.conduit.transform.Transform;
import io.conduit.transform.TransformChain;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wires the stages together and owns every thread.
 *
 * <pre>
 *  source threads ──key hash──▶ lane queues ──▶ N processing workers ──route──▶ per-sink lane queues
 *  (one per source)             (bounded)       (filter, transform)              (bounded, own workers)
 *                                                       │                                 │
 *                                                       └────────────▶ dead-letter ◀──────┘
 * </pre>
 *
 * Every hand-off is an {@link ArrayBlockingQueue}, so a slow stage pushes back on the one before it
 * instead of growing memory. Shutdown runs the same path front to back: stop sources, poison and
 * drain the processing lanes, then the sink lanes, then the dead-letter writer, then check the books.
 */
public final class Pipeline {
    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);
    private static final int LANE_DRAIN = 256;

    private final ConduitConfig config;
    private final MeterRegistry registry;
    private final Accounting accounting = new Accounting();
    private final MetricsListener metrics;
    private final PipelineListener listener;
    private final EventParser parser;
    private final DeadLetterQueue deadLetters;
    private final List<SourceHandle> sources = new ArrayList<>();
    private final TransformChain transforms;
    private final List<SinkChannel> channels = new ArrayList<>();
    private final List<ArrayBlockingQueue<Event>> lanes = new ArrayList<>();
    private final EventProcessor processor;

    private final ExecutorService sourcePool;
    private final ExecutorService workerPool;
    private final List<ExecutorService> sinkPools = new ArrayList<>();
    private final ExecutorService deadLetterPool;
    private final CountDownLatch sourcesDone;
    private final AtomicInteger workersAlive = new AtomicInteger();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopping = new AtomicBoolean();
    private final CompletableFuture<ShutdownReport> completion = new CompletableFuture<>();

    private Pipeline(ConduitConfig config, MeterRegistry registry, Clock clock, List<PipelineListener> extra) {
        this.config = config;
        this.registry = registry;
        this.metrics = new MetricsListener(registry);
        List<PipelineListener> all = new ArrayList<>(List.of(accounting, metrics, new LoggingListener()));
        all.addAll(extra);
        this.listener = PipelineListener.composite(all);
        this.parser = new EventParser(clock);
        this.deadLetters = new DeadLetterQueue(config.deadLetter(), listener);
        Registries registries = Registries.discover();

        List<AutoCloseable> built = new ArrayList<>();
        try {
            for (SourceSpec spec : config.sources()) {
                ComponentSpec c = spec.component();
                Props props = c.props();
                Source source = registries.sources().get(c.type(), c.path())
                        .create(props, ComponentContext.of(c.name(), registry, clock));
                props.ensureAllUsed();
                built.add(source);
                sources.add(new SourceHandle(c.name(), source, spec.keyField()));
            }

            List<TransformChain.Step> steps = new ArrayList<>();
            for (ComponentSpec c : config.transforms()) {
                Props props = c.props();
                Transform t = registries.transforms().get(c.type(), c.path())
                        .create(props, ComponentContext.of(c.name(), registry, clock));
                props.ensureAllUsed();
                built.add(t);
                steps.add(new TransformChain.Step(c.name(), t));
            }
            this.transforms = new TransformChain(steps);

            Map<String, SinkChannel> byName = new LinkedHashMap<>();
            for (SinkSpec spec : config.sinks()) {
                SinkChannel channel = buildChannel(spec, registries, clock, built);
                channels.add(channel);
                byName.put(channel.name, channel);
            }
            Router<SinkChannel> router = config.router().map(byName::get);
            this.processor = new EventProcessor(config.filter(), transforms, router, listener, deadLetters);
        } catch (RuntimeException e) {
            built.forEach(Pipeline::closeQuietly);
            throw e;
        }

        for (int i = 0; i < config.workers(); i++) {
            ArrayBlockingQueue<Event> q = new ArrayBlockingQueue<>(config.queueCapacity());
            lanes.add(q);
            Gauge.builder("conduit.queue.depth", q, BlockingQueue::size).description("Events waiting in a queue")
                    .tag("stage", "process").tag("queue", "lane-" + i).register(registry);
        }
        Gauge.builder("conduit.queue.depth", deadLetters.queue(), BlockingQueue::size)
                .tag("stage", "dead-letter").tag("queue", "dead-letter").register(registry);

        this.sourcesDone = new CountDownLatch(sources.size());
        this.sourcePool = Executors.newFixedThreadPool(sources.size(), Lanes.named("source"));
        this.workerPool = Executors.newFixedThreadPool(config.workers(), Lanes.named("worker"));
        for (SinkChannel c : channels) {
            sinkPools.add(Executors.newFixedThreadPool(c.lanes.size(), Lanes.named("sink-" + c.name)));
        }
        this.deadLetterPool = Executors.newSingleThreadExecutor(Lanes.named("dead-letter"));
    }

    /** Builds every component from the config. Throws {@link ConfigException} on unknown types or fields. */
    public static Pipeline create(ConduitConfig config, MeterRegistry registry) {
        return create(config, registry, Clock.systemUTC(), List.of());
    }

    public static Pipeline create(ConduitConfig config, MeterRegistry registry, Clock clock,
                                  List<PipelineListener> extraListeners) {
        return new Pipeline(config, registry, clock, extraListeners);
    }

    private SinkChannel buildChannel(SinkSpec spec, Registries registries, Clock clock, List<AutoCloseable> built) {
        ComponentSpec c = spec.component();
        SinkSettings st = spec.settings();
        var provider = registries.sinks().get(c.type(), c.path());
        List<SinkChannel.Lane> channelLanes = new ArrayList<>();
        for (int i = 0; i < st.workers(); i++) {
            int lane = i;
            Props props = c.props();
            Sink raw = provider.create(props, ComponentContext.of(c.name(), registry, clock).forLane(i, st.workers()));
            props.ensureAllUsed();
            built.add(raw);

            // Decorator stack, outermost first: breaker -> retry -> metrics -> time limit -> sink.
            Sink s = new TimeLimitedSink(raw, st.writeTimeout(), "sink-" + c.name() + "-" + i);
            s = new MeteredSink(s, registry, c.name());
            s = new RetryingSink(s, st.retry(), (attempt, error) -> listener.onSinkError(c.name(), error));
            CircuitBreakerSink breaker = null;
            if (st.breaker().enabled()) {
                breaker = new CircuitBreakerSink(s, st.breaker().failureThreshold(), st.breaker().openFor(), clock,
                        (from, to) -> listener.onCircuitStateChange(c.name(), lane, from.name(), to.name()));
                s = breaker;
            }
            SinkChannel.Lane l = new SinkChannel.Lane(i, st.queueCapacity(), config.workers(), s, breaker);
            channelLanes.add(l);
            Gauge.builder("conduit.queue.depth", l, SinkChannel.Lane::depth)
                    .tag("stage", "sink").tag("queue", c.name() + "-" + i).register(registry);
            Gauge.builder("conduit.sink.circuit.open", l, x -> x.breaker == null
                            || x.breaker.state() == CircuitBreakerSink.State.CLOSED ? 0 : 1)
                    .description("1 while the lane's circuit breaker is open or half-open")
                    .tag("sink", c.name()).tag("lane", String.valueOf(i)).register(registry);
        }
        return new SinkChannel(c.name(), c.type(), st, channelLanes, deadLetters);
    }

    /**
     * Opens every sink and source, then starts the threads back to front so each stage has a consumer
     * before its producer starts. Throws {@link ConfigException} if a sink is unreachable or a source
     * cannot bind; no thread is started in that case.
     */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("pipeline already started");
        }
        try {
            deadLetters.open();
        } catch (Exception e) {
            abortStart();
            throw new ConfigException("dead-letter directory " + config.deadLetter().dir() + " is not usable: "
                    + e.getMessage(), e);
        }
        for (SinkChannel c : channels) {
            for (SinkChannel.Lane l : c.lanes) {
                try {
                    l.sink.open();
                } catch (Exception e) {
                    abortStart();
                    throw new ConfigException("sink '" + c.name + "' is unreachable: " + rootMessage(e), e);
                }
            }
        }
        for (SourceHandle s : sources) {
            try {
                s.source.open();
            } catch (Exception e) {
                abortStart();
                throw new ConfigException("source '" + s.name + "' could not start: " + rootMessage(e), e);
            }
        }
        transforms.start();

        deadLetterPool.execute(deadLetters);
        for (int i = 0; i < channels.size(); i++) {
            SinkChannel c = channels.get(i);
            for (SinkChannel.Lane l : c.lanes) {
                sinkPools.get(i).execute(new SinkWorker(c, l, listener, deadLetters));
            }
        }
        for (int i = 0; i < lanes.size(); i++) {
            int worker = i;
            workersAlive.incrementAndGet();
            workerPool.execute(() -> runLane(worker));
        }
        for (SourceHandle s : sources) {
            sourcePool.execute(s::runSource);
        }
        Thread.ofPlatform().daemon().name("conduit-completion").start(() -> {
            try {
                sourcesDone.await();
                if (!stopping.get()) {
                    log.info("all sources finished; draining");
                    stop(config.shutdownTimeout());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        log.info("pipeline started: {} sources, {} workers, sinks {}", sources.size(), lanes.size(),
                channels.stream().map(c -> c.name + "(" + c.type + " x" + c.lanes.size() + ")").toList());
    }

    /** A processing worker: drains its lane in chunks, one lock acquisition per chunk. */
    private void runLane(int worker) {
        ArrayBlockingQueue<Event> q = lanes.get(worker);
        List<Event> chunk = new ArrayList<>(LANE_DRAIN);
        IdleStrategy idle = new IdleStrategy();
        try {
            while (true) {
                if (q.drainTo(chunk, LANE_DRAIN) == 0) {
                    if (!idle.shouldBlock()) {
                        idle.idle();
                        continue;
                    }
                    chunk.add(q.take());
                }
                idle.reset();
                for (Event e : chunk) {
                    if (e == Lanes.POISON) {
                        return; // producers put it last, so nothing follows it
                    }
                    processor.process(e, worker);
                }
                chunk.clear();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            workersAlive.decrementAndGet();
        }
    }

    /** Blocks until the pipeline has drained, after the sources finish or {@link #stop} is called. */
    public ShutdownReport awaitCompletion() throws InterruptedException {
        try {
            return completion.get();
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException("shutdown failed", e.getCause());
        }
    }

    /**
     * Graceful shutdown (FR-12). Idempotent: concurrent callers all receive the same report. Every
     * step shares one deadline; a stage that misses it is interrupted and the report says so.
     */
    public ShutdownReport stop(Duration timeout) {
        if (!started.get()) {
            throw new IllegalStateException("pipeline was never started");
        }
        if (!stopping.compareAndSet(false, true)) {
            return completion.join();
        }
        try {
            ShutdownReport report = drain(timeout);
            completion.complete(report);
            return report;
        } catch (RuntimeException e) {
            completion.completeExceptionally(e);
            throw e;
        }
    }

    private ShutdownReport drain(Duration timeout) {
        long start = System.nanoTime();
        long deadline = start + timeout.toNanos();
        boolean forced = false;

        for (SourceHandle s : sources) {
            try {
                s.source.stop();
            } catch (RuntimeException e) {
                log.warn("stopping source '{}' failed", s.name, e);
            }
        }
        sourcePool.shutdown();
        forced |= !await(sourcePool, deadline, "sources");

        for (ArrayBlockingQueue<Event> q : lanes) {
            forced |= !poison(q, deadline);
        }
        workerPool.shutdown();
        forced |= !await(workerPool, deadline, "processing workers");

        for (int i = 0; i < channels.size(); i++) {
            for (SinkChannel.Lane l : channels.get(i).lanes) {
                for (ArrayBlockingQueue<Event> input : l.inputs) {
                    forced |= !poison(input, deadline);
                }
            }
            sinkPools.get(i).shutdown();
        }
        for (int i = 0; i < channels.size(); i++) {
            forced |= !await(sinkPools.get(i), deadline, "sink '" + channels.get(i).name + "'");
        }

        try {
            deadLetters.poison();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        deadLetterPool.shutdown();
        forced |= !await(deadLetterPool, deadline, "dead-letter writer");

        sources.forEach(s -> closeQuietly(s.source));
        transforms.close();
        for (SinkChannel c : channels) {
            c.lanes.forEach(l -> closeQuietly(l.sink));
        }

        ShutdownReport report = new ShutdownReport(accounting.snapshot(), Duration.ofNanos(System.nanoTime() - start),
                forced, inFlight());
        if (report.ok()) {
            log.info("{}", report.summary());
        } else {
            log.error("{}", report.summary());
        }
        return report;
    }

    private static boolean poison(BlockingQueue<Event> q, long deadline) {
        try {
            return q.offer(Lanes.POISON, Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static boolean await(ExecutorService pool, long deadline, String what) {
        try {
            if (pool.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.error("{} did not finish before the shutdown deadline; interrupting", what);
        pool.shutdownNow();
        try {
            pool.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    private Map<String, Integer> inFlight() {
        Map<String, Integer> left = new LinkedHashMap<>();
        for (int i = 0; i < lanes.size(); i++) {
            int n = (int) lanes.get(i).stream().filter(e -> e != Lanes.POISON).count();
            if (n > 0) {
                left.put("lane-" + i, n);
            }
        }
        for (SinkChannel c : channels) {
            for (SinkChannel.Lane l : c.lanes) {
                int n = (int) l.inputs.stream().flatMap(BlockingQueue::stream).filter(e -> e != Lanes.POISON).count();
                if (n > 0) {
                    left.put(c.name + "-" + l.index, n);
                }
            }
        }
        if (!deadLetters.queue().isEmpty()) {
            left.put("dead-letter", deadLetters.queue().size());
        }
        return left;
    }

    private void abortStart() {
        for (SinkChannel c : channels) {
            c.lanes.forEach(l -> closeQuietly(l.sink));
        }
        sources.forEach(s -> closeQuietly(s.source));
        transforms.close();
        sourcePool.shutdownNow();
        workerPool.shutdownNow();
        sinkPools.forEach(ExecutorService::shutdownNow);
        deadLetterPool.shutdownNow();
    }

    public Health health() {
        Map<String, Object> details = new LinkedHashMap<>();
        boolean degraded = false;

        Map<String, Object> src = new LinkedHashMap<>();
        int failed = 0;
        for (SourceHandle s : sources) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("state", s.state);
            if (s.error != null) {
                m.put("error", s.error);
            }
            src.put(s.name, m);
            if ("FAILED".equals(s.state)) {
                failed++;
            }
        }
        degraded |= failed > 0;
        details.put("sources", src);

        List<Integer> laneDepths = lanes.stream().map(BlockingQueue::size).toList();
        details.put("processing", Map.of("workers", lanes.size(), "alive", workersAlive.get(),
                "queueDepths", laneDepths, "queueCapacity", config.queueCapacity()));

        Map<String, Object> sinks = new LinkedHashMap<>();
        for (SinkChannel c : channels) {
            Map<String, Object> m = new LinkedHashMap<>();
            List<Map<String, Object>> laneInfo = new ArrayList<>();
            for (SinkChannel.Lane l : c.lanes) {
                String circuit = l.breaker == null ? "DISABLED" : l.breaker.state().name();
                degraded |= l.saturated || !(circuit.equals("CLOSED") || circuit.equals("DISABLED"));
                laneInfo.add(Map.of("queueDepth", l.depth(), "circuit", circuit, "shedding", l.saturated));
            }
            m.put("type", c.type);
            m.put("lanes", laneInfo);
            m.put("queueCapacity", c.settings.queueCapacity());
            if (c.lastSuccessMillis > 0) {
                m.put("lastSuccessAgoMs", System.currentTimeMillis() - c.lastSuccessMillis);
            }
            if (c.lastError != null) {
                m.put("lastError", c.lastError);
            }
            sinks.put(c.name, m);
        }
        details.put("sinks", sinks);
        details.put("deadLetterQueueDepth", deadLetters.queue().size());
        details.put("accounting", accounting.snapshot().asMap());

        Health.Status status;
        if (!started.get() || stopping.get() || (!sources.isEmpty() && failed == sources.size())
                || workersAlive.get() < lanes.size()) {
            status = Health.Status.DOWN;
        } else {
            status = degraded ? Health.Status.DEGRADED : Health.Status.UP;
        }
        return new Health(status, details);
    }

    public Accounting accounting() {
        return accounting;
    }

    public MetricsListener metrics() {
        return metrics;
    }

    public MeterRegistry registry() {
        return registry;
    }

    public ConduitConfig config() {
        return config;
    }

    /** The source instance with this name, e.g. to read a port it bound at startup. */
    public Source source(String name) {
        return sources.stream().filter(s -> s.name.equals(name)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no source named " + name)).source;
    }

    private static void closeQuietly(AutoCloseable c) {
        try {
            c.close();
        } catch (Exception e) {
            log.warn("close failed for {}", c.getClass().getSimpleName(), e);
        }
    }

    private static String rootMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String m = root.getMessage();
        return root.getClass().getSimpleName() + (m == null ? "" : ": " + m);
    }

    /** One source's view of the pipeline, and the bookkeeping for the thread that runs it. */
    private final class SourceHandle implements SourceContext {
        final String name;
        final Source source;
        final String keyField;
        volatile String state = "CREATED";
        volatile String error;

        SourceHandle(String name, Source source, String keyField) {
            this.name = name;
            this.source = source;
            this.keyField = keyField;
        }

        void runSource() {
            state = "RUNNING";
            try {
                source.run(this);
                state = stopping.get() ? "STOPPED" : "FINISHED";
            } catch (InterruptedException e) {
                state = "STOPPED";
                Thread.currentThread().interrupt();
            } catch (Throwable e) {
                state = "FAILED";
                error = rootMessage(e);
                log.error("source '{}' failed", name, e);
            } finally {
                sourcesDone.countDown();
            }
        }

        @Override
        public String sourceName() {
            return name;
        }

        @Override
        public String keyField() {
            return keyField;
        }

        @Override
        public EventParser parser() {
            return parser;
        }

        @Override
        public void emit(Event event) throws InterruptedException {
            lanes.get(Lanes.index(event.key(), lanes.size())).put(event);
            listener.onIngested(name);
        }

        @Override
        public boolean offer(Event event, Duration timeout) throws InterruptedException {
            if (lanes.get(Lanes.index(event.key(), lanes.size())).offer(event, timeout.toNanos(),
                    TimeUnit.NANOSECONDS)) {
                listener.onIngested(name);
                return true;
            }
            listener.onRejected(name);
            return false;
        }

        @Override
        public void deadLetter(String raw, String reason) {
            listener.onIngested(name);
            deadLetters.submit(DeadLetter.ofRaw(DeadLetter.Stage.PARSE, name, reason, raw));
        }
    }
}
