package io.conduit.core;

import java.time.Instant;

/**
 * A record or event the pipeline could not deliver, with where and why. Exactly one of
 * {@code raw} (unparsed input) and {@code event} (a parsed event) is set.
 */
public record DeadLetter(Instant time, Stage stage, String origin, String reason, String raw, Event event,
                         int attempts) {

    public enum Stage {
        /** The record never became an event. Origin is the source. */
        PARSE,
        /** A transform step threw. Origin is the step. */
        TRANSFORM,
        /** The sink's queue stayed full past its offer timeout, so the event was shed. Origin is the sink. */
        SINK_OVERFLOW,
        /** The sink write failed after retries, or its circuit was open. Origin is the sink. */
        SINK_WRITE
    }

    public static DeadLetter ofRaw(Stage stage, String origin, String reason, String raw) {
        return new DeadLetter(Instant.now(), stage, origin, reason, raw, null, 0);
    }

    public static DeadLetter ofEvent(Stage stage, String origin, String reason, Event event, int attempts) {
        return new DeadLetter(Instant.now(), stage, origin, reason, null, event, attempts);
    }
}
