package io.conduit.sink;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/** Exponential backoff with full jitter, capped at {@code maxBackoff}. */
public record RetryPolicy(int maxAttempts, Duration initialBackoff, Duration maxBackoff, double multiplier) {

    public static final RetryPolicy NONE = new RetryPolicy(1, Duration.ZERO, Duration.ZERO, 1.0);

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be at least 1.0");
        }
    }

    /**
     * How long to wait before attempt number {@code attempt + 1}, where {@code attempt} is the number of
     * attempts already made (1-based). Full jitter spreads retries from many lanes so they do not hit a
     * recovering sink in lockstep.
     */
    public long backoffMillis(int attempt) {
        double ceiling = initialBackoff.toMillis() * Math.pow(multiplier, attempt - 1);
        long capped = (long) Math.min(ceiling, maxBackoff.toMillis());
        return capped <= 0 ? 0 : ThreadLocalRandom.current().nextLong(capped / 2, capped + 1);
    }
}
