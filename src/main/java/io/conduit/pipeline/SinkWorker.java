package io.conduit.pipeline;

import io.conduit.core.DeadLetter;
import io.conduit.core.Event;
import io.conduit.observe.PipelineListener;
import io.conduit.sink.CircuitBreakerSink;
import io.conduit.sink.SinkWriteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drains one sink lane in batches: up to {@code batchSize} events, or whatever arrived within
 * {@code linger} of the first one, whichever comes first. The lane has one input per processing
 * worker; they are drained round-robin. A batch the sink stack rejects (retries exhausted, circuit
 * open, non-retryable error) goes to dead-letter whole, so every delivery is either acknowledged or
 * dead-lettered.
 *
 * <p>The worker finishes once every input has delivered its poison pill.
 */
final class SinkWorker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SinkWorker.class);
    private static final long LINGER_NAP_NANOS = 200_000;

    private final SinkChannel channel;
    private final SinkChannel.Lane lane;
    private final PipelineListener listener;
    private final DeadLetterQueue deadLetters;
    private final int batchSize;
    private final long lingerNanos;
    private final List<ArrayBlockingQueue<Event>> inputs;
    private final boolean[] finished;
    private int finishedCount;
    private int next;

    SinkWorker(SinkChannel channel, SinkChannel.Lane lane, PipelineListener listener, DeadLetterQueue deadLetters) {
        this.channel = channel;
        this.lane = lane;
        this.listener = listener;
        this.deadLetters = deadLetters;
        this.batchSize = channel.settings.batchSize();
        this.lingerNanos = channel.settings.linger().toNanos();
        this.inputs = lane.inputs;
        this.finished = new boolean[inputs.size()];
    }

    @Override
    public void run() {
        List<Event> batch = new ArrayList<>(batchSize);
        IdleStrategy idle = new IdleStrategy();
        try {
            while (finishedCount < inputs.size()) {
                if (drain(batch) == 0) {
                    idle.idle();
                    continue;
                }
                idle.reset();
                fill(batch);
                if (!batch.isEmpty()) {
                    publish(batch);
                    batch.clear();
                }
                lane.clearSaturationIfDrained();
            }
        } catch (InterruptedException e) {
            // Forced shutdown: anything still queued is reported as in-flight by the shutdown report.
            if (!batch.isEmpty()) {
                deadLetters.submitBatch(batch, DeadLetter.Stage.SINK_WRITE, channel.name,
                        "interrupted by forced shutdown", 0);
            }
            Thread.currentThread().interrupt();
        } finally {
            try {
                lane.sink.flush();
            } catch (Exception e) {
                log.error("flushing sink '{}' lane {} failed", channel.name, lane.index, e);
            }
        }
    }

    /**
     * One round-robin pass over the inputs, starting at a rotating offset so no input is favoured.
     * Returns how many events were added. A poison pill is always the last element an input ever
     * holds, so it can only appear at the end of that input's drain.
     */
    private int drain(List<Event> batch) {
        int added = 0;
        int n = inputs.size();
        for (int k = 0; k < n && batch.size() < batchSize; k++) {
            int i = (next + k) % n;
            if (finished[i]) {
                continue;
            }
            int got = inputs.get(i).drainTo(batch, batchSize - batch.size());
            if (got > 0 && batch.get(batch.size() - 1) == Lanes.POISON) {
                batch.remove(batch.size() - 1);
                finished[i] = true;
                finishedCount++;
                got--;
            }
            added += got;
        }
        next = (next + 1) % n;
        return added;
    }

    /**
     * Tops the batch up until it is full, {@code linger} has passed, or every input is finished.
     * Waits in short naps rather than on a queue, for the reason given in {@link IdleStrategy}.
     */
    private void fill(List<Event> batch) throws InterruptedException {
        long deadline = System.nanoTime() + lingerNanos;
        while (batch.size() < batchSize && finishedCount < inputs.size()) {
            if (drain(batch) > 0) {
                continue;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            LockSupport.parkNanos(Math.min(remaining, LINGER_NAP_NANOS));
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
    }

    private void publish(List<Event> batch) throws InterruptedException {
        // A copy, because a write abandoned on timeout may still be reading it after we move on.
        List<Event> toWrite = List.copyOf(batch);
        try {
            lane.sink.write(toWrite);
        } catch (InterruptedException e) {
            throw e; // run() dead-letters the batch
        } catch (Exception e) {
            String reason = reason(e);
            int attempts = e instanceof SinkWriteException s ? s.attempts() : 0;
            channel.lastError = reason;
            deadLetters.submitBatch(toWrite, DeadLetter.Stage.SINK_WRITE, channel.name, reason, attempts);
            return;
        }
        long ack = System.nanoTime();
        channel.lastSuccessMillis = System.currentTimeMillis();
        listener.onPublished(channel.name, toWrite, ack);
    }

    private static String reason(Exception e) {
        if (e instanceof CircuitBreakerSink.OpenException) {
            return "circuit open";
        }
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String prefix = e instanceof SinkWriteException ? e.getMessage().split(":", 2)[0] + ": " : "";
        String msg = root.getMessage();
        return prefix + root.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }
}
