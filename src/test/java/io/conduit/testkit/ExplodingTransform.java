package io.conduit.testkit;

import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import io.conduit.core.Event;
import io.conduit.transform.Transform;
import io.conduit.transform.TransformProvider;

/** Throws for every event whose {@code seq} attribute is a multiple of {@code everyNth}. */
public final class ExplodingTransform implements Transform {

    public static final class Provider implements TransformProvider {
        @Override
        public String type() {
            return "explode";
        }

        @Override
        public Transform create(Props p, ComponentContext ctx) {
            return new ExplodingTransform(p.integer("everyNth", 1, 1));
        }
    }

    private final int everyNth;

    ExplodingTransform(int everyNth) {
        this.everyNth = everyNth;
    }

    @Override
    public Event apply(Event event, Chain next) {
        if (event.attributes().get("seq") instanceof Long seq && seq % everyNth == 0) {
            throw new IllegalStateException("boom");
        }
        return next.proceed(event);
    }
}
