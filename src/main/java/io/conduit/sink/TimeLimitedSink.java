package io.conduit.sink;

import io.conduit.core.Event;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Bounds how long one write attempt may take, so a sink that hangs (a stuck socket, a lock wait)
 * shows up as a failure the retry and breaker layers can handle, instead of freezing its lane.
 *
 * <p>The write runs on a virtual thread. On timeout that thread is interrupted and abandoned, and
 * {@link Sink#reset()} tells the sink to drop whatever the stuck call holds.
 */
public final class TimeLimitedSink extends ForwardingSink {
    private final long timeoutNanos;
    private final ExecutorService executor;

    public TimeLimitedSink(Sink delegate, Duration timeout, String threadName) {
        super(delegate);
        this.timeoutNanos = timeout.toNanos();
        this.executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name(threadName + "-write-", 0).factory());
    }

    @Override
    public void write(List<Event> batch) throws Exception {
        Future<?> f = executor.submit(() -> {
            delegate.write(batch);
            return null;
        });
        try {
            f.get(timeoutNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            f.cancel(true);
            delegate.reset();
            throw new TimeoutException("write timed out after " + Duration.ofNanos(timeoutNanos).toMillis() + " ms");
        } catch (InterruptedException e) {
            f.cancel(true);
            throw e;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        executor.shutdownNow();
        super.close();
    }
}
