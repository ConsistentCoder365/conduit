package io.conduit.core;

/** A record that cannot become an {@link Event}. The message is the dead-letter reason. */
public class MalformedRecordException extends Exception {
    public MalformedRecordException(String reason) {
        super(reason);
    }
}
