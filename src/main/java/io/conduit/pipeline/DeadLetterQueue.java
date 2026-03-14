package io.conduit.pipeline;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;
import io.conduit.config.ConduitConfig.DeadLetterSettings;
import io.conduit.core.DeadLetter;
import io.conduit.core.Event;
import io.conduit.core.Json;
import io.conduit.observe.PipelineListener;
import io.conduit.sink.RollingFileWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The last-resort destination. Anything the pipeline cannot deliver comes here with its reason and
 * is written as JSON lines, ready to inspect or replay. Submitting blocks rather than drops: losing a
 * dead letter would break the zero-loss guarantee, and a local file append is the one sink assumed to
 * keep up.
 */
final class DeadLetterQueue implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueue.class);
    private static final DeadLetter POISON = DeadLetter.ofRaw(DeadLetter.Stage.PARSE, "", "", "");
    private static final int MAX_DRAIN = 1024;

    private final ArrayBlockingQueue<DeadLetter> queue;
    private final RollingFileWriter writer;
    private final PipelineListener listener;

    DeadLetterQueue(DeadLetterSettings settings, PipelineListener listener) {
        this.queue = new ArrayBlockingQueue<>(settings.queueCapacity());
        this.writer = new RollingFileWriter(settings.dir(), "dead-letter", settings.maxFileBytes(), null, false);
        this.listener = listener;
    }

    void open() throws IOException {
        writer.open();
    }

    ArrayBlockingQueue<DeadLetter> queue() {
        return queue;
    }

    void submit(DeadLetter letter) {
        putUninterruptibly(letter);
        String origin = letter.origin();
        listener.onDeadLettered(letter.stage(), origin, letter.reason(), 1);
    }

    void submitBatch(List<Event> events, DeadLetter.Stage stage, String origin, String reason, int attempts) {
        for (Event e : events) {
            putUninterruptibly(DeadLetter.ofEvent(stage, origin, reason, e, attempts));
        }
        listener.onDeadLettered(stage, origin, reason, events.size());
    }

    /**
     * Even a forced shutdown must not leave a counted record unqueued, so an interrupt is remembered
     * and restored afterwards instead of abandoning the put.
     */
    private void putUninterruptibly(DeadLetter letter) {
        boolean interrupted = Thread.interrupted();
        try {
            while (true) {
                try {
                    queue.put(letter);
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    void poison() throws InterruptedException {
        queue.put(POISON);
    }

    @Override
    public void run() {
        List<DeadLetter> batch = new ArrayList<>(MAX_DRAIN);
        ByteArrayOutputStream buf = new ByteArrayOutputStream(64 * 1024);
        boolean done = false;
        try {
            while (!done) {
                batch.add(queue.take());
                queue.drainTo(batch, MAX_DRAIN - 1);
                int last = batch.size() - 1;
                if (batch.get(last) == POISON) {
                    batch.remove(last);
                    done = true;
                }
                if (!batch.isEmpty()) {
                    write(batch, buf);
                    listener.onDeadLetterWritten(batch.size());
                    batch.clear();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                log.error("closing dead-letter file failed", e);
            }
        }
    }

    private void write(List<DeadLetter> batch, ByteArrayOutputStream buf) {
        buf.reset();
        try (JsonGenerator g = Json.FACTORY.createGenerator(buf)) {
            g.setRootValueSeparator(new SerializedString("\n"));
            for (DeadLetter d : batch) {
                g.writeStartObject();
                g.writeStringField("time", d.time().toString());
                g.writeStringField("stage", d.stage().name().toLowerCase());
                g.writeStringField("origin", d.origin());
                g.writeStringField("reason", d.reason());
                if (d.attempts() > 0) {
                    g.writeNumberField("attempts", d.attempts());
                }
                if (d.event() != null) {
                    g.writeFieldName("event");
                    Json.writeEvent(g, d.event());
                } else {
                    g.writeStringField("raw", d.raw());
                }
                g.writeEndObject();
            }
            g.writeRaw('\n');
        } catch (IOException e) {
            throw new IllegalStateException("serializing dead letters failed", e);
        }
        // A failed append here has nowhere further to go. Keep retrying: blocking upstream is safer
        // than silently losing records, and the stall is visible in queue-depth metrics.
        long backoff = 100;
        while (true) {
            try {
                writer.write(buf.toByteArray(), 0, buf.size());
                return;
            } catch (IOException e) {
                log.error("writing dead-letter file failed; retrying in {} ms", backoff, e);
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while dead-letter file was failing", e);
                }
                backoff = Math.min(backoff * 2, 5000);
            }
        }
    }
}
