package io.conduit.observe;

import io.conduit.core.DeadLetter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs what an operator needs during an incident: dead-letter reasons, sink errors and circuit
 * transitions. Repeated messages are rate-limited per key so a dead sink cannot flood the log.
 */
public final class LoggingListener implements PipelineListener {
    private static final Logger log = LoggerFactory.getLogger("conduit.events");
    private static final long INTERVAL_NANOS = 10_000_000_000L;

    private final Map<String, AtomicLong> lastLogged = new ConcurrentHashMap<>();

    @Override
    public void onDeadLettered(DeadLetter.Stage stage, String origin, String reason, int count) {
        if (shouldLog("dlq|" + stage + "|" + origin)) {
            log.warn("dead-lettered {} at {} ({}): {}", count, stage.name().toLowerCase(), origin, reason);
        }
    }

    @Override
    public void onSinkError(String sink, Throwable error) {
        if (shouldLog("err|" + sink)) {
            log.warn("sink '{}' write failed: {}", sink, describe(error));
        }
    }

    @Override
    public void onCircuitStateChange(String sink, int lane, String from, String to) {
        log.warn("sink '{}' lane {} circuit {} -> {}", sink, lane, from, to);
    }

    private boolean shouldLog(String key) {
        long now = System.nanoTime();
        AtomicLong last = lastLogged.computeIfAbsent(key, k -> new AtomicLong(now - INTERVAL_NANOS));
        long prev = last.get();
        return now - prev >= INTERVAL_NANOS && last.compareAndSet(prev, now);
    }

    static String describe(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String msg = root.getMessage();
        return root.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }
}
