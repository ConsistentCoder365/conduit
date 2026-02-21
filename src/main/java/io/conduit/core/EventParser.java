package io.conduit.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Turns raw records into {@link Event}s. Thread-safe.
 *
 * <p>Known top-level fields map onto the model; every other top-level field is folded into the
 * attributes, alongside anything under an explicit {@code attributes} object. {@code type} is the
 * only required field. Anything that cannot be parsed raises {@link MalformedRecordException} with a
 * reason that ends up on the dead-letter record.
 */
public final class EventParser {
    private static final Set<String> TIMESTAMP_FIELDS = Set.of("timestamp", "@timestamp", "ts", "time");
    private static final Set<String> RESERVED = Set.of(
            "id", "timestamp", "@timestamp", "ts", "time", "source", "type", "severity", "key", "attributes");
    private static final double MILLIS_THRESHOLD = 1e11;

    private final Clock clock;

    public EventParser(Clock clock) {
        this.clock = clock;
    }

    public EventParser() {
        this(Clock.systemUTC());
    }

    public Event parseJson(String line, String sourceName, String keyField) throws MalformedRecordException {
        if (line == null || line.isBlank()) {
            throw new MalformedRecordException("empty record");
        }
        Map<String, Object> record;
        try {
            record = Json.MAPPER.readValue(line, Json.MAP);
        } catch (JsonProcessingException e) {
            throw new MalformedRecordException("invalid JSON: " + e.getOriginalMessage());
        }
        if (record == null) {
            throw new MalformedRecordException("record is not a JSON object");
        }
        return fromMap(record, sourceName, keyField);
    }

    public Event fromMap(Map<String, Object> record, String sourceName, String keyField)
            throws MalformedRecordException {
        long ingest = System.nanoTime();
        Object type = record.get("type");
        if (!(type instanceof String t) || t.isBlank()) {
            throw new MalformedRecordException("missing required field 'type'");
        }

        Map<String, Object> attrs = new LinkedHashMap<>();
        Object nested = record.get("attributes");
        if (nested instanceof Map<?, ?> m) {
            for (Map.Entry<?, ?> e : m.entrySet()) {
                attrs.put(String.valueOf(e.getKey()), e.getValue());
            }
        } else if (nested != null) {
            throw new MalformedRecordException("'attributes' must be an object");
        }
        for (Map.Entry<String, Object> e : record.entrySet()) {
            if (!RESERVED.contains(e.getKey())) {
                attrs.put(e.getKey(), e.getValue());
            }
        }
        Object originalSource = record.get("source");
        if (originalSource != null && !originalSource.equals(sourceName)) {
            attrs.putIfAbsent("origin", originalSource);
        }

        Severity severity;
        try {
            severity = Severity.parse(record.get("severity"));
        } catch (IllegalArgumentException e) {
            throw new MalformedRecordException(e.getMessage());
        }

        OffsetDateTime ts = parseTimestamp(firstTimestamp(record));
        Object rawId = record.get("id");
        String id = rawId != null ? rawId.toString() : Ids.next();
        String key = resolveKey(record, attrs, keyField, id);
        return new Event(id, ts, sourceName, t, severity, key, attrs, ingest);
    }

    private static Object firstTimestamp(Map<String, Object> record) {
        for (String f : TIMESTAMP_FIELDS) {
            Object v = record.get(f);
            if (v != null) {
                return v;
            }
        }
        return null;
    }

    private static String resolveKey(Map<String, Object> record, Map<String, Object> attrs, String keyField,
                                     String id) {
        Object explicit = record.get("key");
        if (explicit != null) {
            return explicit.toString();
        }
        if (keyField != null) {
            Object v = Event.resolve(attrs, keyField);
            if (v != null) {
                return v.toString();
            }
        }
        return id;
    }

    /**
     * Accepts ISO-8601 with an offset, ISO-8601 local time (taken as UTC), or epoch seconds/millis as
     * a number or numeric string. The original offset is kept; the timestamp transform normalizes it.
     */
    public OffsetDateTime parseTimestamp(Object raw) throws MalformedRecordException {
        if (raw == null) {
            return OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
        }
        if (raw instanceof Number n) {
            return fromEpoch(n.doubleValue());
        }
        String s = raw.toString().trim();
        try {
            return OffsetDateTime.parse(s);
        } catch (DateTimeParseException ignored) {
            // fall through to the other accepted shapes
        }
        try {
            return LocalDateTime.parse(s).atOffset(ZoneOffset.UTC);
        } catch (DateTimeParseException ignored) {
            // fall through
        }
        try {
            return fromEpoch(new BigDecimal(s).doubleValue());
        } catch (NumberFormatException e) {
            throw new MalformedRecordException("unparseable timestamp: " + s);
        }
    }

    private static OffsetDateTime fromEpoch(double v) throws MalformedRecordException {
        if (Double.isNaN(v) || Double.isInfinite(v) || v < 0) {
            throw new MalformedRecordException("invalid epoch timestamp: " + v);
        }
        long millis = v >= MILLIS_THRESHOLD ? (long) v : (long) (v * 1000);
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }
}
