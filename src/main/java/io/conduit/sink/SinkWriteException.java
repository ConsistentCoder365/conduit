package io.conduit.sink;

/** A batch the sink stack gave up on. {@code attempts} is how many writes were tried. */
public class SinkWriteException extends Exception {
    private final int attempts;

    public SinkWriteException(String message, int attempts, Throwable cause) {
        super(message, cause);
        this.attempts = attempts;
    }

    public int attempts() {
        return attempts;
    }
}
