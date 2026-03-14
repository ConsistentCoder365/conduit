package io.conduit.sink;

import io.conduit.core.Event;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.List;

/**
 * Times every write attempt and records batch sizes, tagged by outcome, without the sink knowing.
 * Sits inside the retry layer, so each attempt is measured on its own.
 */
public final class MeteredSink extends ForwardingSink {
    private final Timer success;
    private final Timer failure;
    private final DistributionSummary batchSize;

    public MeteredSink(Sink delegate, MeterRegistry registry, String sinkName) {
        super(delegate);
        this.success = Timer.builder("conduit.sink.write").description("Duration of one sink write attempt")
                .tag("sink", sinkName).tag("outcome", "success").register(registry);
        this.failure = Timer.builder("conduit.sink.write").description("Duration of one sink write attempt")
                .tag("sink", sinkName).tag("outcome", "failure").register(registry);
        this.batchSize = DistributionSummary.builder("conduit.sink.batch.size").description("Events per write")
                .tag("sink", sinkName).register(registry);
    }

    @Override
    public void write(List<Event> batch) throws Exception {
        long start = System.nanoTime();
        try {
            delegate.write(batch);
            success.record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS);
            batchSize.record(batch.size());
        } catch (Exception e) {
            failure.record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS);
            throw e;
        }
    }
}
