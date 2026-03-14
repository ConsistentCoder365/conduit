package io.conduit.pipeline;

import io.conduit.core.DeadLetter;
import io.conduit.core.Event;
import io.conduit.filter.FilterStage;
import io.conduit.observe.PipelineListener;
import io.conduit.route.Router;
import io.conduit.transform.TransformChain;
import io.conduit.transform.TransformException;
import java.util.List;

/**
 * Filter, transform, route, hand off: the work a processing worker does per event. Stateless and
 * shared by all workers. An exception from a filter rule, transform step or route never escapes;
 * the event is dead-lettered with the failing step's name instead.
 */
final class EventProcessor {
    private final FilterStage filter;
    private final TransformChain transforms;
    private final Router<SinkChannel> router;
    private final PipelineListener listener;
    private final DeadLetterQueue deadLetters;

    EventProcessor(FilterStage filter, TransformChain transforms, Router<SinkChannel> router,
                   PipelineListener listener, DeadLetterQueue deadLetters) {
        this.filter = filter;
        this.transforms = transforms;
        this.router = router;
        this.listener = listener;
        this.deadLetters = deadLetters;
    }

    /** {@code worker} is the calling processing worker's index; it picks that worker's sink inputs. */
    void process(Event event, int worker) throws InterruptedException {
        String step = "filter";
        List<SinkChannel> targets;
        Event out;
        try {
            if (!filter.accept(event)) {
                listener.onFiltered(event);
                return;
            }
            step = "transform";
            out = transforms.apply(event);
            if (out == null) {
                listener.onFiltered(event);
                return;
            }
            step = "router";
            targets = router.route(out);
        } catch (TransformException e) {
            deadLetters.submit(DeadLetter.ofEvent(DeadLetter.Stage.TRANSFORM, "transform:" + e.step(),
                    describe(e.getCause()), event, 0));
            return;
        } catch (RuntimeException e) {
            deadLetters.submit(DeadLetter.ofEvent(DeadLetter.Stage.TRANSFORM, step, describe(e), event, 0));
            return;
        }
        listener.onRouted(out, targets.size());
        for (SinkChannel c : targets) {
            c.offer(out, worker);
        }
    }

    private static String describe(Throwable t) {
        String msg = t.getMessage();
        return t.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }
}
