package io.conduit.sink;

import io.conduit.core.Event;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Retries a failed batch with exponential backoff (FR-9). Gives up early on errors the sink calls
 * non-retryable. When attempts run out it throws {@link SinkWriteException}, and the worker sends the
 * batch to dead-letter.
 */
public final class RetryingSink extends ForwardingSink {
    private final RetryPolicy policy;
    private final BiConsumer<Integer, Exception> onFailedAttempt;

    public RetryingSink(Sink delegate, RetryPolicy policy, BiConsumer<Integer, Exception> onFailedAttempt) {
        super(delegate);
        this.policy = policy;
        this.onFailedAttempt = onFailedAttempt;
    }

    @Override
    public void write(List<Event> batch) throws Exception {
        Exception last = null;
        for (int attempt = 1; attempt <= policy.maxAttempts(); attempt++) {
            try {
                delegate.write(batch);
                return;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                last = e;
                onFailedAttempt.accept(attempt, e);
                if (!delegate.isRetryable(e)) {
                    throw new SinkWriteException("non-retryable error: " + e.getMessage(), attempt, e);
                }
                if (attempt < policy.maxAttempts()) {
                    Thread.sleep(policy.backoffMillis(attempt));
                }
            }
        }
        throw new SinkWriteException("gave up after " + policy.maxAttempts() + " attempts: " + last.getMessage(),
                policy.maxAttempts(), last);
    }
}
