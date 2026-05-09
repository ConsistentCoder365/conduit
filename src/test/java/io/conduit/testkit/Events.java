package io.conduit.testkit;

import io.conduit.core.Event;
import io.conduit.core.Severity;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/** Terse event construction for unit tests. */
public final class Events {
    private Events() {
    }

    public static Event of(String type, Severity severity, Object... kv) {
        Map<String, Object> attrs = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            attrs.put((String) kv[i], kv[i + 1]);
        }
        return new Event("id-1", OffsetDateTime.parse("2026-10-25T12:00:00Z"), "test", type, severity, "k", attrs,
                System.nanoTime());
    }

    public static Event of(String type, Object... kv) {
        return of(type, Severity.INFO, kv);
    }
}
