package io.conduit.core;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;

/**
 * What the core hands a component when it builds one: its configured name, the shared meter
 * registry for component-specific metrics, a clock, and — for sinks — which lane it serves.
 */
public record ComponentContext(String name, MeterRegistry meters, Clock clock, int lane, int lanes) {

    public static ComponentContext of(String name, MeterRegistry meters, Clock clock) {
        return new ComponentContext(name, meters, clock, 0, 1);
    }

    public ComponentContext forLane(int lane, int lanes) {
        return new ComponentContext(name, meters, clock, lane, lanes);
    }
}
