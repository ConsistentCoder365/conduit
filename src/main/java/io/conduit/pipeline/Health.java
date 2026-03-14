package io.conduit.pipeline;

import java.util.Map;

/** What {@code /health} reports: an overall status and the per-component detail behind it. */
public record Health(Status status, Map<String, Object> details) {

    public enum Status {
        /** Everything running, every circuit closed. */
        UP,
        /** Still moving events, but a source failed, a circuit is open, or a sink lane is shedding. */
        DEGRADED,
        /** Not running, shutting down, a processing worker died, or every source failed. */
        DOWN
    }
}
