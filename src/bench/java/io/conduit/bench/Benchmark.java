package io.conduit.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.conduit.config.ConfigLoader;
import io.conduit.core.Json;
import io.conduit.pipeline.Pipeline;
import io.conduit.pipeline.ShutdownReport;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Measures the claims in the README. Every number it prints comes from a run on the machine it ran
 * on; nothing is estimated.
 *
 * <pre>
 * ./gradlew bench                                  # everything, results in bench/results/
 * ./gradlew bench -PbenchArgs="scaling --runs 3 --events 3000000 --workers 1,2,4,8,16"
 * ./gradlew bench -PbenchArgs="latency --rate 200000 --seconds 20"
 * ./gradlew bench -PbenchArgs="loss --events 10000000"
 * ./gradlew bench -PbenchArgs="overload --seconds 600"
 * ./gradlew bench -PbenchArgs="isolation"
 * ./gradlew benchSmoke                             # CI gate against bench/baseline.json
 * </pre>
 */
public final class Benchmark {
    private static final Path RESULTS = Path.of("bench", "results");
    private static final ObjectWriter JSON = Json.MAPPER.writerWithDefaultPrettyPrinter();

    private final Args args;
    private final Path work;

    private Benchmark(Args args) throws IOException {
        this.args = args;
        this.work = Files.createTempDirectory("conduit-bench");
    }

    public static void main(String[] argv) throws Exception {
        Args a = Args.parse(argv);
        Files.createDirectories(RESULTS);
        Benchmark b = new Benchmark(a);
        int status = 0;
        try {
            switch (a.scenario) {
                case "scaling" -> b.scaling();
                case "latency" -> b.latency();
                case "loss" -> b.loss();
                case "overload" -> b.overload();
                case "isolation" -> b.isolation();
                case "smoke" -> status = b.smoke();
                case "all" -> b.all();
                case "report" -> Report.write(RESULTS);
                default -> throw new IllegalArgumentException("unknown scenario " + a.scenario
                        + "; use scaling, latency, loss, overload, isolation, smoke, report or all");
            }
        } finally {
            deleteTree(b.work);
        }
        System.exit(status);
    }

    // ---- scenarios -----------------------------------------------------------------------------

    private void all() throws Exception {
        Map<String, Object> scaling = scaling();
        double peak = (double) scaling.get("peakEventsPerSec");
        int peakWorkers = (int) scaling.get("peakWorkers");
        latencySweep(new double[] {100_000, 250_000, Math.round(peak / 2)}, peakWorkers, args.intOpt("seconds", 20));
        loss();
        overload();
        isolationAt(Math.min(200_000, peak / 4));
        Report.write(RESULTS);
    }

    Map<String, Object> scaling() throws Exception {
        int[] workerCounts = args.intList("workers", new int[] {1, 2, 4, 8, 16});
        int runs = args.intOpt("runs", 3);
        long events = args.longOpt("events", 3_000_000);
        String sink = args.opt("sink", "file");
        int sinkLanes = args.intOpt("sinkWorkers", 4);

        log("warmup");
        runThroughput(4, Math.min(events, 1_000_000), sink, sinkLanes);

        List<Map<String, Object>> rows = new ArrayList<>();
        double peak = 0;
        int peakWorkers = 1;
        for (int w : workerCounts) {
            double[] rates = new double[runs];
            double cores = 0;
            double cpuPerEvent = 0;
            for (int r = 0; r < runs; r++) {
                RunResult res = runThroughput(w, events, sink, sinkLanes);
                rates[r] = res.eventsPerSec;
                cores += res.coresUsed / runs;
                cpuPerEvent += res.cpuMicrosPerEvent / runs;
                log("workers=%d run=%d  %,.0f events/s  (%.2fs, %.1f cores busy, %.2f cpu-us/event, ok=%s)", w, r + 1,
                        res.eventsPerSec, res.seconds, res.coresUsed, res.cpuMicrosPerEvent, res.ok);
                if (!res.ok) {
                    throw new IllegalStateException("accounting check failed during scaling run:\n" + res.summary);
                }
            }
            Arrays.sort(rates);
            double median = rates[runs / 2];
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("workers", w);
            row.put("runs", Arrays.stream(rates).boxed().toList());
            row.put("median", median);
            row.put("min", rates[0]);
            row.put("max", rates[runs - 1]);
            row.put("coresUsed", cores);
            row.put("cpuMicrosPerEvent", cpuPerEvent);
            rows.add(row);
            if (median > peak) {
                peak = median;
                peakWorkers = w;
            }
        }
        double base = (double) rows.get(0).get("median");
        for (Map<String, Object> row : rows) {
            row.put("speedup", (double) row.get("median") / base);
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("scenario", "scaling");
        out.put("machine", Machine.describe());
        out.put("eventsPerRun", events);
        out.put("sink", sink);
        out.put("sinkWorkers", sinkLanes);
        out.put("sources", args.intOpt("sources", 2));
        out.put("results", rows);
        out.put("peakEventsPerSec", peak);
        out.put("peakWorkers", peakWorkers);
        out.put("peakSpeedup", peak / base);
        save("scaling", out);
        return out;
    }

    private void latency() throws Exception {
        int[] rates = args.intList("rates", new int[0]);
        if (rates.length == 0) {
            JsonNode s = readResult("scaling");
            if (s == null) {
                throw new IllegalArgumentException("pass --rates, or run the scaling scenario first");
            }
            rates = new int[] {100_000, 250_000, (int) (s.get("peakEventsPerSec").asDouble() / 2)};
        }
        latencySweep(Arrays.stream(rates).asDoubleStream().toArray(), args.intOpt("workers", 4),
                args.intOpt("seconds", 20));
    }

    /** One fixed-rate run per rate, after a JIT warmup, so the tail is not dominated by compilation. */
    private void latencySweep(double[] rates, int workers, int seconds) throws Exception {
        log("latency: warming up the JIT first");
        runThroughput(workers, 2_000_000, "file", 4);
        List<Map<String, Object>> runs = new ArrayList<>();
        for (double rate : rates) {
            runs.add(latencyAt(rate, workers, seconds));
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("scenario", "latency");
        out.put("machine", Machine.describe());
        out.put("workers", workers);
        out.put("secondsPerRun", seconds);
        out.put("lingerMs", 5);
        out.put("gc", "G1, -Xmx1g, -XX:MaxGCPauseMillis=20");
        out.put("runs", runs);
        save("latency", out);
    }

    private Map<String, Object> latencyAt(double rate, int workers, int seconds) throws Exception {
        long count = (long) (rate * seconds);
        SimpleMeterRegistry reg = new SimpleMeterRegistry();
        log("latency: %,.0f events/s for %ds with %d workers", rate, seconds, workers);
        Pipeline p = Pipeline.create(ConfigLoader.parse(throughputYaml(workers, (long) rate, count, "file", 4)), reg);
        long start = System.nanoTime();
        p.start();
        ShutdownReport report = p.awaitCompletion();
        double secs = (System.nanoTime() - start) / 1e9;
        HistogramSnapshot snap = p.metrics().latency("out").takeSnapshot();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("targetEventsPerSec", rate);
        out.put("achievedEventsPerSec", report.accounting().ingested() / secs);
        out.put("events", report.accounting().published());
        for (ValueAtPercentile v : snap.percentileValues()) {
            out.put("p" + Math.round(v.percentile() * 100) + "Ms", v.value(TimeUnit.MILLISECONDS));
        }
        out.put("meanMs", snap.mean(TimeUnit.MILLISECONDS));
        out.put("maxMs", snap.max(TimeUnit.MILLISECONDS));
        out.put("ok", report.ok());
        log("latency: p50 %.2f ms, p95 %.2f ms, p99 %.2f ms, max %.1f ms", out.get("p50Ms"), out.get("p95Ms"),
                out.get("p99Ms"), out.get("maxMs"));
        return out;
    }

    /** Ten million events into a healthy file sink and a flaky, intermittently dead fault sink. */
    private void loss() throws Exception {
        long events = args.longOpt("events", 10_000_000);
        String yaml = """
                pipeline: { workers: 4, queueCapacity: 8192, shutdownTimeout: 120s }
                metrics: { enabled: false }
                deadLetter: { dir: '%s/dlq' }
                sources:
                  - { name: synth, type: synthetic, rate: 0, count: %d, seed: 99, malformedRate: 0.0005 }
                transforms:
                  - { type: redact, fields: [email], salt: bench }
                routes:
                  rules:
                    - { name: both, when: { always: true }, to: [archive, flaky] }
                  default: [archive]
                sinks:
                  - { name: archive, type: blackhole, workers: 4, batchSize: 1000, linger: 5ms }
                  - name: flaky
                    type: fault
                    failureRate: 0.05
                    outageAfter: 3s
                    outageFor: 2s
                    workers: 2
                    batchSize: 1000
                    retry: { maxAttempts: 3, initialBackoff: 1ms, maxBackoff: 10ms }
                    circuitBreaker: { failureThreshold: 3, openFor: 500ms }
                """.formatted(work, events);
        log("loss: %,d events with injected sink failures", events);
        long start = System.nanoTime();
        Pipeline p = Pipeline.create(ConfigLoader.parse(yaml), new SimpleMeterRegistry());
        p.start();
        ShutdownReport r = p.awaitCompletion();
        double secs = (System.nanoTime() - start) / 1e9;
        long dlqLines = countLines(work.resolve("dlq"));

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("scenario", "loss");
        out.put("machine", Machine.describe());
        out.put("events", events);
        out.put("seconds", secs);
        out.put("accounting", r.accounting().asMap());
        out.put("deadLetterFileLines", dlqLines);
        out.put("invariantHolds", r.ok());
        out.put("filesMatchCounters", dlqLines == r.accounting().deadLetterWritten());
        log("loss: ok=%s files-match=%s in %.1fs%n%s", r.ok(), out.get("filesMatchCounters"), secs, r.summary());
        save("loss", out);
        clean("dlq");
        if (!r.ok()) {
            throw new IllegalStateException("zero-loss check failed");
        }
    }

    /** An unthrottled source against the pipeline for a long time; heap must stay flat. */
    private void overload() throws Exception {
        int seconds = args.intOpt("seconds", 60);
        log("overload: unthrottled source for %ds, sampling heap every second", seconds);
        Pipeline p = Pipeline.create(ConfigLoader.parse(throughputYaml(4, 0, 0, "blackhole", 2)),
                new SimpleMeterRegistry());
        MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
        p.start();
        List<Long> heapMb = new ArrayList<>();
        List<Long> ingested = new ArrayList<>();
        for (int i = 0; i < seconds; i++) {
            Thread.sleep(1000);
            heapMb.add(mem.getHeapMemoryUsage().getUsed() >> 20);
            ingested.add(p.accounting().snapshot().ingested());
        }
        ShutdownReport r = p.stop(Duration.ofSeconds(60));
        int q = Math.max(1, heapMb.size() / 4);
        double firstQuarter = heapMb.subList(0, q).stream().mapToLong(Long::longValue).average().orElse(0);
        double lastQuarter = heapMb.subList(heapMb.size() - q, heapMb.size()).stream().mapToLong(Long::longValue)
                .average().orElse(0);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("scenario", "overload");
        out.put("machine", Machine.describe());
        out.put("seconds", seconds);
        out.put("maxHeapMb", Runtime.getRuntime().maxMemory() >> 20);
        out.put("heapUsedMbPerSecond", heapMb);
        out.put("heapUsedMbMax", heapMb.stream().mapToLong(Long::longValue).max().orElse(0));
        out.put("heapUsedMbFirstQuarterAvg", firstQuarter);
        out.put("heapUsedMbLastQuarterAvg", lastQuarter);
        out.put("eventsIngested", r.accounting().ingested());
        out.put("avgEventsPerSec", r.accounting().ingested() / (double) seconds);
        out.put("ok", r.ok());
        log("overload: heap avg %.0f MB (first quarter) -> %.0f MB (last quarter), max %d MB of %d MB; %,d events; ok=%s",
                firstQuarter, lastQuarter, out.get("heapUsedMbMax"), out.get("maxHeapMb"), r.accounting().ingested(),
                r.ok());
        save("overload", out);
    }

    private void isolation() throws Exception {
        isolationAt(args.doubleOpt("rate", 100_000));
    }

    /**
     * A healthy file sink and a sink that hangs from t=5s to t=11s, both receiving every event at a
     * fixed rate. The healthy sink's throughput is sampled every 500 ms.
     */
    private void isolationAt(double rate) throws Exception {
        int seconds = args.intOpt("isolationSeconds", 16);
        String yaml = """
                pipeline: { workers: 4, shutdownTimeout: 60s }
                metrics: { enabled: false }
                deadLetter: { dir: '%s/dlq' }
                sources:
                  - { name: synth, type: synthetic, rate: %d, count: %d, seed: 5 }
                routes:
                  rules:
                    - { name: both, when: { always: true }, to: [healthy, failing] }
                  default: [healthy]
                sinks:
                  - { name: healthy, type: file, dir: '%s/healthy', workers: 2, batchSize: 1000, linger: 5ms }
                  - name: failing
                    type: fault
                    outageAfter: 5s
                    outageFor: 6s
                    outageMode: hang
                    batchSize: 1000
                    writeTimeout: 1s
                    offerTimeout: 100ms
                    retry: { maxAttempts: 2, initialBackoff: 50ms, maxBackoff: 200ms }
                    circuitBreaker: { failureThreshold: 2, openFor: 2s }
                """.formatted(work, (long) rate, (long) rate * seconds, work);
        log("isolation: %,.0f events/s; 'failing' sink hangs from t=5s to t=11s", rate);
        SimpleMeterRegistry reg = new SimpleMeterRegistry();
        Pipeline p = Pipeline.create(ConfigLoader.parse(yaml), reg);
        p.start();
        List<Map<String, Object>> samples = new ArrayList<>();
        long t0 = System.nanoTime();
        double prevHealthy = 0;
        double prevFailing = 0;
        while (p.health().status() != io.conduit.pipeline.Health.Status.DOWN
                && System.nanoTime() - t0 < Duration.ofSeconds(seconds + 10).toNanos()) {
            Thread.sleep(500);
            double healthy = published(reg, "healthy");
            double failing = published(reg, "failing");
            Map<String, Object> s = new LinkedHashMap<>();
            s.put("t", Math.round((System.nanoTime() - t0) / 1e8) / 10.0);
            s.put("healthyPerSec", (healthy - prevHealthy) * 2);
            s.put("failingPerSec", (failing - prevFailing) * 2);
            samples.add(s);
            prevHealthy = healthy;
            prevFailing = failing;
        }
        ShutdownReport r = p.awaitCompletion();

        double before = avg(samples, "healthyPerSec", 1.0, 5.0);
        double during = avg(samples, "healthyPerSec", 5.5, 11.0);
        double after = avg(samples, "healthyPerSec", 11.5, seconds - 0.5);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("scenario", "isolation");
        out.put("machine", Machine.describe());
        out.put("rate", rate);
        out.put("healthyPerSecBefore", before);
        out.put("healthyPerSecDuringOutage", during);
        out.put("healthyPerSecAfter", after);
        out.put("duringVsBefore", during / before);
        out.put("samples", samples);
        out.put("accounting", r.accounting().asMap());
        out.put("ok", r.ok());
        log("isolation: healthy sink %,.0f/s before, %,.0f/s during outage (%.1f%%), %,.0f/s after; ok=%s",
                before, during, 100 * during / before, after, r.ok());
        save("isolation", out);
        clean("healthy", "dlq");
    }

    private int smoke() throws Exception {
        Path baselineFile = Path.of(args.opt("baseline", "bench/baseline.json"));
        long events = args.longOpt("events", 1_000_000);
        runThroughput(4, events / 3, "file", 4);
        double best = 0;
        for (int i = 0; i < 3; i++) {
            best = Math.max(best, runThroughput(4, events, "file", 4).eventsPerSec);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("scenario", "smoke");
        out.put("machine", Machine.describe());
        out.put("eventsPerSec", best);
        out.put("at", Instant.now().toString());
        save("smoke", out);

        if (args.flag("update-baseline")) {
            JSON.writeValue(baselineFile.toFile(), Map.of("eventsPerSec", Math.round(best), "maxDrop", 0.5,
                    "machine", Machine.describe().get("cpu"), "recorded", Instant.now().toString()));
            log("smoke: baseline %s updated to %,.0f events/s", baselineFile, best);
            return 0;
        }
        if (!Files.exists(baselineFile)) {
            log("smoke: %,.0f events/s (no baseline at %s; pass --update-baseline to record one)", best, baselineFile);
            return 0;
        }
        JsonNode base = Json.MAPPER.readTree(baselineFile.toFile());
        double floor = base.get("eventsPerSec").asDouble() * (1 - base.get("maxDrop").asDouble());
        boolean pass = best >= floor;
        log("smoke: %,.0f events/s vs baseline %,.0f (floor %,.0f): %s", best, base.get("eventsPerSec").asDouble(),
                floor, pass ? "PASS" : "FAIL - throughput dropped sharply");
        return pass ? 0 : 1;
    }

    // ---- plumbing ------------------------------------------------------------------------------

    record RunResult(double eventsPerSec, double seconds, double coresUsed, double cpuMicrosPerEvent, boolean ok,
                     String summary) {
    }

    private static long processCpuNanos() {
        return ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
                .getProcessCpuTime();
    }

    private RunResult runThroughput(int workers, long events, String sink, int sinkLanes) throws Exception {
        System.gc();
        Pipeline p = Pipeline.create(ConfigLoader.parse(throughputYaml(workers, 0, events, sink, sinkLanes)),
                new SimpleMeterRegistry());
        long cpu0 = processCpuNanos();
        long start = System.nanoTime();
        p.start();
        ShutdownReport r = p.awaitCompletion();
        long wall = System.nanoTime() - start;
        long cpu = processCpuNanos() - cpu0;
        clean("out", "dlq");
        long n = r.accounting().ingested();
        return new RunResult(n / (wall / 1e9), wall / 1e9, (double) cpu / wall, cpu / 1e3 / n, r.ok(), r.summary());
    }

    /**
     * The benchmark pipeline: synthetic source, heartbeat filter, the full transform chain (UTC
     * normalization, HMAC-SHA256 email redaction, CIDR enrichment of two fields), into one sink.
     */
    private String throughputYaml(int workers, long rate, long count, String sink, int sinkLanes) {
        String sinkLine = sink.equals("file")
                ? "{ name: out, type: file, dir: '%s/out', workers: %d, batchSize: 1000, linger: 5ms }"
                .formatted(work, sinkLanes)
                : "{ name: out, type: %s, workers: %d, batchSize: 1000, linger: 5ms }".formatted(sink, sinkLanes);
        return """
                pipeline: { workers: %d, queueCapacity: 8192, shutdownTimeout: 120s }
                metrics: { enabled: false }
                deadLetter: { dir: '%s/dlq' }
                sources:
                %s
                filter:
                  drop: { type: heartbeat }
                transforms:
                  - { type: timestamp }
                  - { type: redact, fields: [email], detectEmails: true, salt: bench }
                  - { type: enrich-ip, fields: [src_ip, dst_ip], table: config/ip-table.csv, reloadInterval: 0ms }
                sinks:
                  - %s
                """.formatted(workers, work, sourceLines(rate, count), sinkLine);
    }

    /** {@code --sources K} splits the load over K synthetic source threads. */
    private String sourceLines(long rate, long count) {
        int k = args.intOpt("sources", 2);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < k; i++) {
            long c = count == 0 ? 0 : count / k + (i < count % k ? 1 : 0);
            sb.append(i == 0 ? "" : "\n").append("  - { name: synth%d, type: synthetic, rate: %d, count: %d, keys: 100000, seed: %d }"
                    .formatted(i, rate == 0 ? 0 : Math.max(1, rate / k), c, i + 1));
        }
        return sb.toString();
    }

    private static double published(SimpleMeterRegistry reg, String sink) {
        Counter c = reg.find("conduit.sink.published").tag("sink", sink).counter();
        return c == null ? 0 : c.count();
    }

    private static double avg(List<Map<String, Object>> samples, String key, double from, double to) {
        return samples.stream().filter(s -> (double) s.get("t") >= from && (double) s.get("t") <= to)
                .mapToDouble(s -> (double) s.get(key)).average().orElse(0);
    }

    private void save(String name, Map<String, Object> result) throws IOException {
        Path f = RESULTS.resolve(name + ".json");
        JSON.writeValue(f.toFile(), result);
        log("wrote %s", f);
    }

    static JsonNode readResult(String name) throws IOException {
        Path f = RESULTS.resolve(name + ".json");
        return Files.exists(f) ? Json.MAPPER.readTree(f.toFile()) : null;
    }

    private void clean(String... dirs) throws IOException {
        for (String d : dirs) {
            deleteTree(work.resolve(d));
        }
    }

    private static long countLines(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return 0;
        }
        long n = 0;
        try (Stream<Path> files = Files.list(dir)) {
            for (Path f : files.toList()) {
                try (Stream<String> lines = Files.lines(f)) {
                    n += lines.filter(l -> !l.isBlank()).count();
                }
            }
        }
        return n;
    }

    private static void deleteTree(Path p) throws IOException {
        if (!Files.exists(p)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(p)) {
            for (Path x : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(x);
            }
        }
    }

    static void log(String fmt, Object... a) {
        System.out.printf("[bench] " + fmt + "%n", a);
    }

    /** {@code scenario --key value --flag}. */
    static final class Args {
        final String scenario;
        final Map<String, String> opts = new LinkedHashMap<>();

        private Args(String scenario) {
            this.scenario = scenario;
        }

        static Args parse(String[] argv) {
            Args a = new Args(argv.length == 0 ? "all" : argv[0]);
            for (int i = 1; i < argv.length; i++) {
                if (!argv[i].startsWith("--")) {
                    throw new IllegalArgumentException("unexpected argument " + argv[i]);
                }
                String key = argv[i].substring(2);
                boolean hasValue = i + 1 < argv.length && !argv[i + 1].startsWith("--");
                a.opts.put(key, hasValue ? argv[++i] : "true");
            }
            return a;
        }

        String opt(String k, String def) {
            return opts.getOrDefault(k, def);
        }

        int intOpt(String k, int def) {
            return opts.containsKey(k) ? Integer.parseInt(opts.get(k)) : def;
        }

        long longOpt(String k, long def) {
            return opts.containsKey(k) ? Long.parseLong(opts.get(k).replace("_", "")) : def;
        }

        double doubleOpt(String k, double def) {
            return opts.containsKey(k) ? Double.parseDouble(opts.get(k)) : def;
        }

        boolean flag(String k) {
            return "true".equals(opts.get(k));
        }

        int[] intList(String k, int[] def) {
            return opts.containsKey(k) ? Arrays.stream(opts.get(k).split(",")).mapToInt(Integer::parseInt).toArray()
                    : def;
        }
    }
}
