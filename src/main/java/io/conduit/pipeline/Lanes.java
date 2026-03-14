package io.conduit.pipeline;

import io.conduit.core.Event;
import io.conduit.core.Severity;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Partitioning, the shutdown sentinel, and thread naming shared by the pipeline's stages. */
final class Lanes {
    private static final Logger log = LoggerFactory.getLogger(Lanes.class);

    /**
     * Poison pill. Put at the end of a queue once every producer of that queue has finished, so the
     * consumer knows nothing follows it. Compared by identity.
     */
    static final Event POISON = new Event("__poison__", OffsetDateTime.MIN, "", "", Severity.INFO, "", Map.of(), 0);

    private Lanes() {
    }

    /** Same key, same lane: this is what keeps per-key order through a multi-threaded stage. */
    static int index(String key, int lanes) {
        if (lanes == 1) {
            return 0;
        }
        int h = key.hashCode();
        h ^= h >>> 16;
        return (h & 0x7fffffff) % lanes;
    }

    static ThreadFactory named(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + n.getAndIncrement());
            t.setUncaughtExceptionHandler((th, e) -> log.error("thread {} died", th.getName(), e));
            return t;
        };
    }
}
