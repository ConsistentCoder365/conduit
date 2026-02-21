package io.conduit.source;

import io.conduit.core.Event;
import io.conduit.core.EventParser;
import io.conduit.core.MalformedRecordException;
import java.time.Duration;
import java.util.Map;

/** The pipeline as a source sees it: somewhere to put events, with backpressure. */
public interface SourceContext {

    String sourceName();

    /** Attribute path used as the partition key when a record has no explicit {@code key}. May be null. */
    String keyField();

    EventParser parser();

    /** Hands an event to the pipeline, blocking while its lane is full. This is the backpressure. */
    void emit(Event event) throws InterruptedException;

    /**
     * Like {@link #emit} but gives up after {@code timeout}. Returns false if the lane stayed full;
     * the event was not accepted and the caller should tell its client to retry (HTTP 429).
     */
    boolean offer(Event event, Duration timeout) throws InterruptedException;

    /** Records a malformed input record with the reason. It counts as ingested and dead-lettered. */
    void deadLetter(String raw, String reason);

    /** Parses one JSON line and emits it, or dead-letters it with the parse error. */
    default void emitJson(String line) throws InterruptedException {
        Event event;
        try {
            event = parser().parseJson(line, sourceName(), keyField());
        } catch (MalformedRecordException e) {
            deadLetter(line, e.getMessage());
            return;
        }
        emit(event);
    }

    /** Builds an event from an already-decoded record and emits it, or dead-letters {@code raw}. */
    default void emitRecord(Map<String, Object> record, String raw) throws InterruptedException {
        Event event;
        try {
            event = parser().fromMap(record, sourceName(), keyField());
        } catch (MalformedRecordException e) {
            deadLetter(raw, e.getMessage());
            return;
        }
        emit(event);
    }
}
