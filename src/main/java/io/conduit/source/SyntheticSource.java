package io.conduit.source;

import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import io.conduit.core.Event;
import io.conduit.core.Severity;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.locks.LockSupport;

/**
 * Generates auth, network-flow, process and heartbeat events at a set rate, for benchmarks and demos
 * (FR-2). Value pools are built up front so generation stays cheap enough that the pipeline, not the
 * generator, is what a benchmark measures.
 *
 * <pre>
 * - name: synth
 *   type: synthetic
 *   rate: 50000          # events per second; 0 = as fast as backpressure allows
 *   count: 1000000       # stop after this many; 0 = run until stopped
 *   keys: 10000          # distinct users (and so partition keys)
 *   malformedRate: 0.001 # share of records sent as broken JSON, to exercise dead-lettering
 *   mix: { auth: 0.35, network: 0.4, process: 0.2, heartbeat: 0.05 }
 * </pre>
 */
public final class SyntheticSource implements Source {

    public static final class Provider implements SourceProvider {
        @Override
        public String type() {
            return "synthetic";
        }

        @Override
        public Source create(Props p, ComponentContext ctx) {
            Props mix = p.childOrEmpty("mix");
            double auth = mix.decimal("auth", 0.35, 0, 1);
            double network = mix.decimal("network", 0.40, 0, 1);
            double process = mix.decimal("process", 0.20, 0, 1);
            double heartbeat = mix.decimal("heartbeat", 0.05, 0, 1);
            mix.ensureAllUsed();
            double total = auth + network + process + heartbeat;
            if (total <= 0) {
                throw mix.invalid("auth", "the mix must have at least one non-zero share");
            }
            return new SyntheticSource(ctx.name(), ctx.clock(),
                    p.longValue("rate", 1000, 0),
                    p.longValue("count", 0, 0),
                    p.integer("keys", 10_000, 1),
                    p.longValue("seed", System.nanoTime(), Long.MIN_VALUE),
                    p.decimal("malformedRate", 0.0, 0, 1),
                    new double[] {auth / total, (auth + network) / total, (auth + network + process) / total});
        }
    }

    private static final String[] METHODS = {"password", "mfa", "kerberos", "sso"};
    private static final String[] PROCESSES = {"bash", "python3", "curl", "sshd", "powershell.exe", "java", "nginx"};
    private static final String[] PROTOCOLS = {"tcp", "udp"};
    private static final int[] PORTS = {22, 53, 80, 443, 3389, 5432, 8080, 9200};

    private final String name;
    private final Clock clock;
    private final long rate;
    private final long count;
    private final double malformedRate;
    private final double[] mixThresholds;
    private final SplittableRandom random;
    private final String[] users;
    private final String[] emails;
    private final String[] hosts;
    private final String[] internalIps;
    private final String[] externalIps;
    private volatile boolean running = true;

    SyntheticSource(String name, Clock clock, long rate, long count, int keys, long seed, double malformedRate,
                    double[] mixThresholds) {
        this.name = name;
        this.clock = clock;
        this.rate = rate;
        this.count = count;
        this.malformedRate = malformedRate;
        this.mixThresholds = mixThresholds;
        this.random = new SplittableRandom(seed);
        this.users = new String[keys];
        this.emails = new String[keys];
        for (int i = 0; i < keys; i++) {
            users[i] = "user-" + i;
            emails[i] = "user" + i + "@corp.example.com";
        }
        this.hosts = new String[Math.min(keys, 2000)];
        for (int i = 0; i < hosts.length; i++) {
            hosts[i] = "host-" + i + ".corp.example.com";
        }
        this.internalIps = new String[4096];
        this.externalIps = new String[4096];
        for (int i = 0; i < 4096; i++) {
            internalIps[i] = "10." + (i >> 8) + "." + (i & 0xff) + "." + (1 + random.nextInt(250));
            externalIps[i] = (1 + random.nextInt(223)) + "." + random.nextInt(256) + "." + random.nextInt(256) + "."
                    + (1 + random.nextInt(250));
        }
    }

    @Override
    public void run(SourceContext ctx) throws InterruptedException {
        long intervalNanos = rate > 0 ? 1_000_000_000L / rate : 0;
        long start = System.nanoTime();
        for (long i = 0; running && (count == 0 || i < count); i++) {
            if (intervalNanos > 0 && (i & 63) == 0) {
                long due = start + i * intervalNanos;
                long wait = due - System.nanoTime();
                if (wait > 0) {
                    LockSupport.parkNanos(wait);
                }
            }
            if (malformedRate > 0 && random.nextDouble() < malformedRate) {
                ctx.emitJson("{\"type\": \"auth\", \"user\": \"truncated-" + i);
                continue;
            }
            ctx.emit(next(i));
        }
    }

    Event next(long seq) {
        OffsetDateTime ts = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
        double r = random.nextDouble();
        int u = random.nextInt(users.length);
        Map<String, Object> a = new LinkedHashMap<>(12);
        String type;
        String key;
        Severity sev;
        if (r < mixThresholds[0]) {
            type = "auth";
            boolean failed = random.nextInt(10) == 0;
            boolean external = random.nextInt(5) == 0;
            a.put("user", users[u]);
            a.put("email", emails[u]);
            a.put("src_ip", external ? externalIps[random.nextInt(4096)] : internalIps[u & 4095]);
            a.put("host", hosts[u % hosts.length]);
            a.put("method", METHODS[random.nextInt(METHODS.length)]);
            a.put("outcome", failed ? "failure" : "success");
            sev = failed ? (external ? Severity.HIGH : Severity.MEDIUM) : Severity.INFO;
            key = users[u];
        } else if (r < mixThresholds[1]) {
            type = "network";
            boolean deny = random.nextInt(20) == 0;
            String src = internalIps[random.nextInt(4096)];
            a.put("src_ip", src);
            a.put("dst_ip", random.nextBoolean() ? externalIps[random.nextInt(4096)] : internalIps[random.nextInt(4096)]);
            a.put("dst_port", PORTS[random.nextInt(PORTS.length)]);
            a.put("protocol", PROTOCOLS[random.nextInt(2)]);
            a.put("bytes", random.nextInt(1_000_000));
            a.put("action", deny ? "deny" : "allow");
            sev = deny ? Severity.LOW : Severity.INFO;
            key = src;
        } else if (r < mixThresholds[2]) {
            type = "process";
            String proc = PROCESSES[random.nextInt(PROCESSES.length)];
            a.put("host", hosts[u % hosts.length]);
            a.put("user", users[u]);
            a.put("process", proc);
            a.put("pid", 1000 + random.nextInt(60_000));
            a.put("cmdline", random.nextInt(8) == 0
                    ? proc + " --notify " + emails[random.nextInt(emails.length)]
                    : proc + " --config /etc/" + proc + ".conf");
            int roll = random.nextInt(100);
            sev = roll < 2 ? Severity.CRITICAL : roll < 8 ? Severity.HIGH : roll < 20 ? Severity.MEDIUM : Severity.LOW;
            key = users[u];
        } else {
            type = "heartbeat";
            String host = hosts[u % hosts.length];
            a.put("host", host);
            sev = Severity.INFO;
            key = host;
        }
        return new Event(name + "-" + seq, ts, name, type, sev, key, a, System.nanoTime());
    }

    @Override
    public void stop() {
        running = false;
    }
}
