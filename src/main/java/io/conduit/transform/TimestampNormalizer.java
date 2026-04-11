package io.conduit.transform;

import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import io.conduit.core.Event;
import io.conduit.core.EventParser;
import io.conduit.core.MalformedRecordException;
import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Normalizes timestamps to UTC (FR-5). The event timestamp is shifted to offset {@code Z}; listed
 * attribute fields holding a timestamp in any accepted shape (ISO-8601, epoch seconds or millis) are
 * rewritten as ISO-8601 UTC strings. With {@code maxFutureSkew}, a timestamp too far in the future
 * (a sender with a broken clock) is replaced by the ingest time, and the original is kept in
 * {@code original_timestamp}.
 *
 * <pre>
 * - type: timestamp
 *   fields: [login_time]
 *   maxFutureSkew: 5m
 * </pre>
 */
public final class TimestampNormalizer implements Transform {

    public static final class Provider implements TransformProvider {
        @Override
        public String type() {
            return "timestamp";
        }

        @Override
        public Transform create(Props p, ComponentContext ctx) {
            return new TimestampNormalizer(p.stringList("fields", List.of()), p.duration("maxFutureSkew", null),
                    ctx.clock());
        }
    }

    private final List<String> fields;
    private final Duration maxFutureSkew;
    private final Clock clock;
    private final EventParser parser;

    public TimestampNormalizer(List<String> fields, Duration maxFutureSkew, Clock clock) {
        this.fields = List.copyOf(fields);
        this.maxFutureSkew = maxFutureSkew;
        this.clock = clock;
        this.parser = new EventParser(clock);
    }

    @Override
    public Event apply(Event event, Chain next) {
        Event e = event;
        OffsetDateTime ts = e.timestamp();
        Map<String, Object> attrs = null;
        if (maxFutureSkew != null && ts.toInstant().isAfter(clock.instant().plus(maxFutureSkew))) {
            attrs = new LinkedHashMap<>(e.attributes());
            attrs.put("original_timestamp", DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ts));
            ts = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
        }
        if (!ZoneOffset.UTC.equals(ts.getOffset())) {
            ts = ts.withOffsetSameInstant(ZoneOffset.UTC);
        }
        for (String f : fields) {
            Object v = e.attributes().get(f);
            if (v == null) {
                continue;
            }
            try {
                String normalized = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
                        parser.parseTimestamp(v).withOffsetSameInstant(ZoneOffset.UTC));
                if (!normalized.equals(v)) {
                    if (attrs == null) {
                        attrs = new LinkedHashMap<>(e.attributes());
                    }
                    attrs.put(f, normalized);
                }
            } catch (MalformedRecordException ignored) {
                // Leave a value we cannot read as it is; the event is still worth keeping.
            }
        }
        if (ts != e.timestamp()) {
            e = e.withTimestamp(ts);
        }
        if (attrs != null) {
            e = e.withAttributes(attrs);
        }
        return next.proceed(e);
    }
}
