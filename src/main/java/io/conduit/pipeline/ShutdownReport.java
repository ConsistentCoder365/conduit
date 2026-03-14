package io.conduit.pipeline;

import io.conduit.observe.Accounting;
import java.time.Duration;
import java.util.Map;

/**
 * How a run ended. {@link #ok()} is the zero-loss verdict: every queue drained, nothing had to be
 * interrupted, and the accounting invariant holds.
 *
 * @param forced   some stage missed the shutdown deadline and was interrupted
 * @param inFlight queues that still held events after shutdown, by name (empty when clean)
 */
public record ShutdownReport(Accounting.Snapshot accounting, Duration took, boolean forced,
                             Map<String, Integer> inFlight) {

    public ShutdownReport {
        inFlight = Map.copyOf(inFlight);
    }

    public boolean ok() {
        return !forced && inFlight.isEmpty() && accounting.balanced();
    }

    public String summary() {
        StringBuilder sb = new StringBuilder();
        sb.append(ok() ? "clean shutdown" : "SHUTDOWN CHECK FAILED")
                .append(" in ").append(took.toMillis()).append(" ms");
        if (forced) {
            sb.append(" (timeout exceeded, stages interrupted)");
        }
        sb.append('\n').append(accounting.explain());
        if (!inFlight.isEmpty()) {
            sb.append("\nstill queued: ").append(inFlight);
        }
        return sb.toString();
    }
}
