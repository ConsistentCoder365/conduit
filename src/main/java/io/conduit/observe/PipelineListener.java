package io.conduit.observe;

import io.conduit.core.DeadLetter;
import io.conduit.core.Event;
import java.util.List;

/**
 * Observer for what happens to events. Stages report facts here and never decide what gets
 * counted, logged or exported; listeners do. Callbacks run on pipeline threads, many at once, so
 * implementations must be thread-safe and cheap.
 */
public interface PipelineListener {

    /** A record entered the pipeline: emitted to a lane, or dead-lettered as malformed. */
    default void onIngested(String source) {
    }

    /** A source could not hand over an event because its lane stayed full (HTTP answered 429). */
    default void onRejected(String source) {
    }

    /** The filter (or a transform returning null) dropped the event. */
    default void onFiltered(Event event) {
    }

    /** The router picked {@code targets} destinations for the event. */
    default void onRouted(Event event, int targets) {
    }

    /** A sink acknowledged a batch. {@code ackNanos} is the {@link System#nanoTime()} of the ack. */
    default void onPublished(String sink, List<Event> batch, long ackNanos) {
    }

    /** {@code count} records or deliveries were handed to the dead-letter queue. */
    default void onDeadLettered(DeadLetter.Stage stage, String origin, String reason, int count) {
    }

    /** The dead-letter writer durably wrote {@code count} records. */
    default void onDeadLetterWritten(int count) {
    }

    /** A sink write attempt failed. It may still succeed on retry. */
    default void onSinkError(String sink, Throwable error) {
    }

    default void onCircuitStateChange(String sink, int lane, String from, String to) {
    }

    /** Many listeners, notified in order. Built once, so the hot path iterates a plain array. */
    static PipelineListener composite(List<PipelineListener> listeners) {
        PipelineListener[] all = listeners.toArray(PipelineListener[]::new);
        return new PipelineListener() {
            public void onIngested(String source) {
                for (PipelineListener l : all) {
                    l.onIngested(source);
                }
            }

            public void onRejected(String source) {
                for (PipelineListener l : all) {
                    l.onRejected(source);
                }
            }

            public void onFiltered(Event event) {
                for (PipelineListener l : all) {
                    l.onFiltered(event);
                }
            }

            public void onRouted(Event event, int targets) {
                for (PipelineListener l : all) {
                    l.onRouted(event, targets);
                }
            }

            public void onPublished(String sink, List<Event> batch, long ackNanos) {
                for (PipelineListener l : all) {
                    l.onPublished(sink, batch, ackNanos);
                }
            }

            public void onDeadLettered(DeadLetter.Stage stage, String origin, String reason, int count) {
                for (PipelineListener l : all) {
                    l.onDeadLettered(stage, origin, reason, count);
                }
            }

            public void onDeadLetterWritten(int count) {
                for (PipelineListener l : all) {
                    l.onDeadLetterWritten(count);
                }
            }

            public void onSinkError(String sink, Throwable error) {
                for (PipelineListener l : all) {
                    l.onSinkError(sink, error);
                }
            }

            public void onCircuitStateChange(String sink, int lane, String from, String to) {
                for (PipelineListener l : all) {
                    l.onCircuitStateChange(sink, lane, from, to);
                }
            }
        };
    }
}
