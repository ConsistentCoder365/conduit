package io.conduit.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import io.conduit.core.Event;
import io.conduit.testkit.BurstSource;
import io.conduit.testkit.CollectingSink;
import io.conduit.testkit.TestPipelines;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

/**
 * Many producer threads hit the pipeline at the same instant (released together by one latch),
 * with small queues so producers and consumers constantly block on each other. Repeated to shake out
 * rare interleavings. Checks: nothing lost, nothing duplicated, and per-key order preserved end to
 * end through both the processing lanes and multi-lane sinks.
 */
class ConcurrencyStressTest {
    private static final int SOURCES = 8;
    private static final int PER_SOURCE = 5_000;

    @TempDir
    Path tmp;

    @AfterEach
    void reset() {
        TestPipelines.resetAll();
    }

    @RepeatedTest(15)
    void concurrentProducersLoseNothingAndKeepPerKeyOrder() throws Exception {
        String latch = "stress-" + System.nanoTime();
        String sources = IntStream.range(0, SOURCES)
                .mapToObj(i -> "  - { name: p%d, type: burst, count: %d, keys: 37, latch: %s }"
                        .formatted(i, PER_SOURCE, latch))
                .collect(Collectors.joining("\n"));
        Pipeline p = TestPipelines.create("""
                pipeline: { workers: 6, queueCapacity: 64 }
                sources:
                %s
                routes:
                  rules:
                    - { name: hot, when: { severity: { gte: HIGH } }, to: [hot, all] }
                  default: [all]
                sinks:
                  - { name: all, type: collect, workers: 3, queueCapacity: 32, batchSize: 16, linger: 1ms }
                  - { name: hot, type: collect, workers: 2, queueCapacity: 16, batchSize: 8, linger: 1ms,
                      overflow: block }
                """.formatted(sources), tmp);
        p.start();
        BurstSource.latch(latch).countDown();
        ShutdownReport r = p.awaitCompletion();

        assertThat(r.ok()).as(r.summary()).isTrue();
        long total = (long) SOURCES * PER_SOURCE;
        assertThat(r.accounting().ingested()).isEqualTo(total);
        assertThat(r.accounting().sinkOverflow()).as("no shedding under contention alone").isZero();

        List<Event> all = CollectingSink.events("all");
        assertThat(all).hasSize((int) total);
        assertThat(all.stream().map(Event::id).distinct().count()).as("no duplicates").isEqualTo(total);
        assertPerKeyOrder(all);

        List<Event> hot = CollectingSink.events("hot");
        assertThat(hot).hasSize((int) (total * 2 / 5));
        assertPerKeyOrder(hot);
    }

    /** The same check many more times. Opt in with {@code ./gradlew test -PwithStress}. */
    @Tag("stress-long")
    @RepeatedTest(200)
    void concurrentProducersLongRun() throws Exception {
        concurrentProducersLoseNothingAndKeepPerKeyOrder();
        TestPipelines.resetAll();
    }

    /**
     * Within a lane, CollectingSink appends batches in write order. Events of one key always share a
     * lane, so filtering the collected stream by key must yield strictly increasing seq numbers.
     */
    private static void assertPerKeyOrder(List<Event> events) {
        Map<String, Long> last = new HashMap<>();
        for (Event e : events) {
            long seq = (Long) e.attributes().get("seq");
            Long prev = last.put(e.key(), seq);
            if (prev != null) {
                assertThat(seq).as("order for key %s", e.key()).isGreaterThan(prev);
            }
        }
    }

    @RepeatedTest(5)
    void stopUnderFullLoadDrainsEverything() throws Exception {
        Pipeline p = TestPipelines.create("""
                pipeline: { workers: 4, queueCapacity: 256, shutdownTimeout: 20s }
                sources:
                  - { name: a, type: synthetic, rate: 0, seed: 1 }
                  - { name: b, type: synthetic, rate: 0, seed: 2 }
                  - { name: c, type: burst, count: 0, keys: 50 }
                sinks:
                  - { name: out, type: collect, workers: 2 }
                  - { name: files, type: file, dir: '${TMP}/out', workers: 2 }
                """, tmp);
        p.start();
        Thread.sleep(300);
        ShutdownReport r = p.stop(Duration.ofSeconds(20));
        assertThat(r.ok()).as(r.summary()).isTrue();
        assertThat(r.accounting().ingested()).isGreaterThan(10_000);
        assertThat(CollectingSink.events("out")).hasSize((int) r.accounting().routed());
    }
}
