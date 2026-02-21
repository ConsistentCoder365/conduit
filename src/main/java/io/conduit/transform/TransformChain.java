package io.conduit.transform;

import io.conduit.core.Event;
import java.util.List;

/**
 * Links the configured steps once at build time. Each link is stateless, so one chain is shared by
 * all workers and applying it allocates nothing beyond what the steps themselves create. A step that
 * throws is reported as a {@link TransformException} naming that step.
 */
public final class TransformChain implements AutoCloseable {

    public record Step(String name, Transform transform) {
    }

    private final List<Step> steps;
    private final Transform.Chain head;

    public TransformChain(List<Step> steps) {
        this.steps = List.copyOf(steps);
        Transform.Chain next = event -> event;
        for (int i = this.steps.size() - 1; i >= 0; i--) {
            Step step = this.steps.get(i);
            Transform.Chain after = next;
            next = event -> {
                try {
                    return step.transform().apply(event, after);
                } catch (TransformException e) {
                    throw e;
                } catch (RuntimeException e) {
                    throw new TransformException(step.name(), e);
                }
            };
        }
        this.head = next;
    }

    public static TransformChain empty() {
        return new TransformChain(List.of());
    }

    /** Returns the transformed event, or null if a step dropped it. */
    public Event apply(Event event) {
        return head.proceed(event);
    }

    public void start() {
        steps.forEach(s -> s.transform().start());
    }

    @Override
    public void close() {
        steps.forEach(s -> s.transform().close());
    }
}
