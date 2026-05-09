package io.conduit.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import io.conduit.config.ConfigLoader;
import io.conduit.pipeline.Pipeline;
import io.conduit.pipeline.ShutdownReport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Builds pipelines from inline YAML for tests. {@code ${DLQ}} is replaced with a temp directory. */
public final class TestPipelines {
    private TestPipelines() {
    }

    public static Pipeline create(String yaml, Path tmp) {
        String withDefaults = yaml.contains("deadLetter:") ? yaml
                : yaml + "\ndeadLetter:\n  dir: ${DLQ}\n";
        String metrics = withDefaults.contains("metrics:") ? withDefaults
                : withDefaults + "\nmetrics:\n  enabled: false\n";
        Map<String, String> env = Map.of("DLQ", tmp.resolve("dlq").toString(), "TMP", tmp.toString());
        return Pipeline.create(ConfigLoader.parse(metrics, env::get), new SimpleMeterRegistry());
    }

    /** Starts the pipeline and waits for it to drain on its own (finite sources). */
    public static ShutdownReport runToCompletion(Pipeline p, Duration max) throws Exception {
        p.start();
        return CompletableFuture.supplyAsync(() -> {
            try {
                return p.awaitCompletion();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }).get(max.toMillis(), TimeUnit.MILLISECONDS);
    }

    public static ShutdownReport runClean(String yaml, Path tmp) throws Exception {
        ShutdownReport r = runToCompletion(create(yaml, tmp), Duration.ofSeconds(60));
        assertThat(r.ok()).as(r.summary()).isTrue();
        return r;
    }

    public static void resetAll() {
        CollectingSink.clearAll();
        GateSink.clearAll();
        BurstSource.clearAll();
    }
}
