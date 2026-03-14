package io.conduit.observe;

import io.conduit.core.DeadLetter;
import io.conduit.core.Event;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * The ledger behind the zero-loss claim. Every record that enters must leave through exactly one
 * door, and every delivery the router promises must be acknowledged or dead-lettered:
 *
 * <pre>
 *   ingested   = filtered + routed + dead-lettered before routing (parse, transform)
 *   deliveries = published + dead-lettered at a sink (overflow, write failure)
 *   dead-letter records written = all dead-lettered
 * </pre>
 *
 * With one target per event, deliveries equal routed events and the first two lines collapse into
 * the PRD's "in = published + filtered + dead-lettered". Fan-out makes one event several deliveries,
 * so the ledger keeps the two levels apart. Counters are {@link LongAdder}s: many threads add, and
 * only the final check reads.
 */
public final class Accounting implements PipelineListener {
    private final LongAdder ingested = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder filtered = new LongAdder();
    private final LongAdder routed = new LongAdder();
    private final LongAdder deliveries = new LongAdder();
    private final LongAdder published = new LongAdder();
    private final LongAdder parseFailed = new LongAdder();
    private final LongAdder transformFailed = new LongAdder();
    private final LongAdder sinkOverflow = new LongAdder();
    private final LongAdder sinkFailed = new LongAdder();
    private final LongAdder deadLetterWritten = new LongAdder();

    @Override
    public void onIngested(String source) {
        ingested.increment();
    }

    @Override
    public void onRejected(String source) {
        rejected.increment();
    }

    @Override
    public void onFiltered(Event event) {
        filtered.increment();
    }

    @Override
    public void onRouted(Event event, int targets) {
        routed.increment();
        deliveries.add(targets);
    }

    @Override
    public void onPublished(String sink, List<Event> batch, long ackNanos) {
        published.add(batch.size());
    }

    @Override
    public void onDeadLettered(DeadLetter.Stage stage, String origin, String reason, int count) {
        switch (stage) {
            case PARSE -> parseFailed.add(count);
            case TRANSFORM -> transformFailed.add(count);
            case SINK_OVERFLOW -> sinkOverflow.add(count);
            case SINK_WRITE -> sinkFailed.add(count);
        }
    }

    @Override
    public void onDeadLetterWritten(int count) {
        deadLetterWritten.add(count);
    }

    public Snapshot snapshot() {
        return new Snapshot(ingested.sum(), rejected.sum(), filtered.sum(), routed.sum(), deliveries.sum(),
                published.sum(), parseFailed.sum(), transformFailed.sum(), sinkOverflow.sum(), sinkFailed.sum(),
                deadLetterWritten.sum());
    }

    public record Snapshot(long ingested, long rejected, long filtered, long routed, long deliveries, long published,
                           long parseFailed, long transformFailed, long sinkOverflow, long sinkFailed,
                           long deadLetterWritten) {

        public long deadLettered() {
            return parseFailed + transformFailed + sinkOverflow + sinkFailed;
        }

        public boolean eventsBalance() {
            return ingested == filtered + routed + parseFailed + transformFailed;
        }

        public boolean deliveriesBalance() {
            return deliveries == published + sinkOverflow + sinkFailed;
        }

        public boolean deadLettersWritten() {
            return deadLetterWritten == deadLettered();
        }

        public boolean balanced() {
            return eventsBalance() && deliveriesBalance() && deadLettersWritten();
        }

        public Map<String, Long> asMap() {
            Map<String, Long> m = new LinkedHashMap<>();
            m.put("ingested", ingested);
            m.put("rejected_backpressure", rejected);
            m.put("filtered", filtered);
            m.put("routed", routed);
            m.put("deliveries", deliveries);
            m.put("published", published);
            m.put("dead_lettered_parse", parseFailed);
            m.put("dead_lettered_transform", transformFailed);
            m.put("dead_lettered_sink_overflow", sinkOverflow);
            m.put("dead_lettered_sink_write", sinkFailed);
            m.put("dead_letter_records_written", deadLetterWritten);
            return m;
        }

        public String explain() {
            return String.format(
                    "events:     ingested %,d %s filtered %,d + routed %,d + dead-lettered %,d (parse %,d, transform %,d)%n"
                            + "deliveries: routed-to %,d %s published %,d + dead-lettered %,d (overflow %,d, write %,d)%n"
                            + "dead-letter file: %,d records written %s %,d dead-lettered",
                    ingested, eventsBalance() ? "=" : "≠", filtered, routed, parseFailed + transformFailed,
                    parseFailed, transformFailed,
                    deliveries, deliveriesBalance() ? "=" : "≠", published, sinkOverflow + sinkFailed, sinkOverflow,
                    sinkFailed,
                    deadLetterWritten, deadLettersWritten() ? "=" : "≠", deadLettered());
        }
    }
}
