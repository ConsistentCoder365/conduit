package io.conduit.transform;

/** Wraps an exception thrown by a transform step, naming the step for the dead-letter record. */
public class TransformException extends RuntimeException {
    private final String step;

    public TransformException(String step, Throwable cause) {
        super(step + ": " + cause.getMessage(), cause);
        this.step = step;
    }

    public String step() {
        return step;
    }
}
