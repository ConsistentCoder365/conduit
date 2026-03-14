package io.conduit;

import io.conduit.config.ConduitConfig;
import io.conduit.config.ConfigException;
import io.conduit.config.ConfigLoader;
import io.conduit.observe.OpsServer;
import io.conduit.pipeline.Pipeline;
import io.conduit.pipeline.ShutdownReport;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code conduit [--validate] <config.yaml>}
 *
 * <p>Exit codes: 0 clean run, 1 accounting check failed or shutdown timed out, 78 bad configuration
 * or unreachable sink (EX_CONFIG). On SIGTERM the shutdown hook drains the pipeline; if the check
 * fails there, the JVM is halted with status 1 so supervisors see the failure.
 */
public final class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final int EX_CONFIG = 78;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        boolean validateOnly = false;
        Path configPath = null;
        for (String a : args) {
            if (a.equals("--validate")) {
                validateOnly = true;
            } else if (a.equals("-h") || a.equals("--help")) {
                System.out.println("usage: conduit [--validate] <config.yaml>");
                return;
            } else {
                configPath = Path.of(a);
            }
        }
        if (configPath == null) {
            configPath = Path.of(System.getenv().getOrDefault("CONDUIT_CONFIG", "config/conduit.yaml"));
        }

        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);

        Pipeline pipeline;
        try {
            ConduitConfig config = ConfigLoader.load(configPath);
            pipeline = Pipeline.create(config, registry);
            if (validateOnly) {
                System.out.println("config OK: " + configPath);
                return;
            }
            pipeline.start();
        } catch (ConfigException e) {
            log.error("refusing to start: {}", e.getMessage());
            System.exit(EX_CONFIG);
            return;
        }

        ConduitConfig config = pipeline.config();
        OpsServer ops = config.metricsEnabled() ? new OpsServer(config.metricsPort(), registry, pipeline::health) : null;
        if (ops != null) {
            log.info("metrics on http://localhost:{}/metrics, health on /health", ops.port());
        }

        AtomicReference<Boolean> naturalExit = new AtomicReference<>(false);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (naturalExit.get()) {
                return;
            }
            log.info("SIGTERM received; draining (timeout {})", config.shutdownTimeout());
            ShutdownReport r = pipeline.stop(config.shutdownTimeout());
            if (ops != null) {
                ops.close();
            }
            if (!r.ok()) {
                Runtime.getRuntime().halt(1);
            }
        }, "shutdown-hook"));

        ShutdownReport report = pipeline.awaitCompletion();
        naturalExit.set(true);
        if (ops != null) {
            ops.close();
        }
        System.exit(report.ok() ? 0 : 1);
    }
}
