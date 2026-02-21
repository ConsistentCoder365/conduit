package io.conduit.route;

import io.conduit.core.Event;
import io.conduit.filter.Rule;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Decides which sinks get each event. The routing policy is a strategy: {@code ALL_MATCHING} fans
 * an event out to the union of every matching route's targets, {@code FIRST_MATCH} stops at the
 * first route that matches. Unmatched events go to the default targets, so every event that reaches
 * the router leaves with at least one destination.
 *
 * @param <T> the target type; sink channels in the pipeline, plain names in tests
 */
public final class Router<T> {

    public enum Policy { ALL_MATCHING, FIRST_MATCH }

    public record Route<T>(String name, Rule when, List<T> targets) {
        public Route {
            targets = List.copyOf(targets);
        }
    }

    private final Policy policy;
    private final List<Route<T>> routes;
    private final List<T> defaults;

    public Router(Policy policy, List<Route<T>> routes, List<T> defaults) {
        if (defaults.isEmpty()) {
            throw new IllegalArgumentException("router needs at least one default target");
        }
        this.policy = policy;
        this.routes = List.copyOf(routes);
        this.defaults = List.copyOf(defaults);
    }

    /** Builds a router over different targets, e.g. from sink names to the live channels. */
    public <U> Router<U> map(Function<T, U> resolve) {
        List<Route<U>> mapped = new ArrayList<>(routes.size());
        for (Route<T> r : routes) {
            mapped.add(new Route<>(r.name(), r.when(), r.targets().stream().map(resolve).toList()));
        }
        return new Router<>(policy, mapped, defaults.stream().map(resolve).toList());
    }

    /** The distinct targets for this event, in route order. Never empty. */
    public List<T> route(Event event) {
        List<T> first = null;
        List<T> union = null;
        for (Route<T> r : routes) {
            if (!r.when().test(event)) {
                continue;
            }
            if (policy == Policy.FIRST_MATCH) {
                return r.targets();
            }
            if (first == null) {
                first = r.targets();
            } else {
                if (union == null) {
                    union = new ArrayList<>(first);
                }
                for (T t : r.targets()) {
                    if (!union.contains(t)) {
                        union.add(t);
                    }
                }
            }
        }
        if (union != null) {
            return union;
        }
        return first != null ? first : defaults;
    }

    public List<Route<T>> routes() {
        return routes;
    }

    public List<T> defaults() {
        return defaults;
    }

    public Policy policy() {
        return policy;
    }
}
