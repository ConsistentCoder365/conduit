package io.conduit.filter;

import io.conduit.core.Event;
import java.util.List;
import java.util.function.Predicate;

/**
 * A condition on an event. Filter stages and routes are both configured with rules, so one small
 * language covers "drop heartbeats" and "send auth events to Neo4j" alike.
 *
 * <p>Rules are immutable and thread-safe; every worker shares the same instances.
 */
public interface Rule {

    boolean test(Event event);

    Rule ALWAYS = new Leaf("always", e -> true);

    record And(List<Rule> rules) implements Rule {
        public boolean test(Event event) {
            for (Rule r : rules) {
                if (!r.test(event)) {
                    return false;
                }
            }
            return true;
        }
    }

    record Or(List<Rule> rules) implements Rule {
        public boolean test(Event event) {
            for (Rule r : rules) {
                if (r.test(event)) {
                    return true;
                }
            }
            return false;
        }
    }

    record Not(Rule rule) implements Rule {
        public boolean test(Event event) {
            return !rule.test(event);
        }
    }

    /** A leaf condition with a readable description for logs and error messages. */
    record Leaf(String description, Predicate<Event> predicate) implements Rule {
        public boolean test(Event event) {
            return predicate.test(event);
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
