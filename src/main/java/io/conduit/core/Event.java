package io.conduit.core;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The single model every source parses into.
 *
 * <p>Events are immutable and shared between threads: after routing, the same instance can sit in
 * several sink queues at once. Transforms never mutate an event; they build a new one with the
 * {@code with*} methods. {@code ingestNanos} is the {@link System#nanoTime()} reading taken when the
 * record entered the pipeline and is used only for end-to-end latency.
 */
public record Event(
        String id,
        OffsetDateTime timestamp,
        String source,
        String type,
        Severity severity,
        String key,
        Map<String, Object> attributes,
        long ingestNanos) {

    public Event {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(severity, "severity");
        Objects.requireNonNull(key, "key");
        attributes = attributes == null ? Map.of() : Collections.unmodifiableMap(attributes);
    }

    public Event withTimestamp(OffsetDateTime ts) {
        return new Event(id, ts, source, type, severity, key, attributes, ingestNanos);
    }

    public Event withKey(String newKey) {
        return new Event(id, timestamp, source, type, severity, newKey, attributes, ingestNanos);
    }

    /** The caller hands over ownership of {@code attrs} and must not modify it afterwards. */
    public Event withAttributes(Map<String, Object> attrs) {
        return new Event(id, timestamp, source, type, severity, key, attrs, ingestNanos);
    }

    /** Reads an attribute by dotted path, e.g. {@code geo.country}. Returns null when absent. */
    public Object attribute(String path) {
        return resolve(attributes, path);
    }

    /** Reads a top-level field or, failing that, an attribute path. Used by rules and key extraction. */
    public Object field(String path) {
        return switch (path) {
            case "id" -> id;
            case "source" -> source;
            case "type" -> type;
            case "severity" -> severity.name();
            case "key" -> key;
            case "timestamp" -> timestamp;
            default -> attribute(path.startsWith("attributes.") ? path.substring(11) : path);
        };
    }

    static Object resolve(Map<String, Object> root, String path) {
        Object direct = root.get(path);
        if (direct != null || path.indexOf('.') < 0) {
            return direct;
        }
        Object current = root;
        for (String part : path.split("\\.")) {
            if (current instanceof Map<?, ?> m) {
                current = m.get(part);
            } else if (current instanceof List<?> l && isIndex(part) && Integer.parseInt(part) < l.size()) {
                current = l.get(Integer.parseInt(part));
            } else {
                return null;
            }
        }
        return current;
    }

    private static boolean isIndex(String s) {
        if (s.isEmpty() || s.length() > 6) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
