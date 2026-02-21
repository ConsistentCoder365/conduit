package io.conduit.filter;

import io.conduit.core.Event;

/**
 * Keeps an event when it matches {@code keep} (if set) and does not match {@code drop} (if set).
 * With neither set, every event passes.
 */
public record FilterStage(Rule keep, Rule drop) {

    public static final FilterStage PASS_ALL = new FilterStage(null, null);

    public boolean accept(Event event) {
        return (keep == null || keep.test(event)) && (drop == null || !drop.test(event));
    }
}
