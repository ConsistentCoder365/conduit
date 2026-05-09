package io.conduit.route;

import static org.assertj.core.api.Assertions.assertThat;

import io.conduit.core.Severity;
import io.conduit.filter.Rule;
import io.conduit.testkit.Events;
import java.util.List;
import org.junit.jupiter.api.Test;

class RouterTest {
    private static final Rule AUTH = new Rule.Leaf("auth", e -> e.type().equals("auth"));
    private static final Rule HIGH = new Rule.Leaf("high", e -> e.severity().compareTo(Severity.HIGH) >= 0);

    private final List<Router.Route<String>> routes = List.of(
            new Router.Route<>("auth", AUTH, List.of("neo4j", "postgres")),
            new Router.Route<>("alerts", HIGH, List.of("postgres", "pager")));

    @Test
    void allMatchingFansOutToTheDistinctUnionInRouteOrder() {
        Router<String> r = new Router<>(Router.Policy.ALL_MATCHING, routes, List.of("archive"));
        assertThat(r.route(Events.of("auth", Severity.HIGH))).containsExactly("neo4j", "postgres", "pager");
        assertThat(r.route(Events.of("auth", Severity.INFO))).containsExactly("neo4j", "postgres");
        assertThat(r.route(Events.of("process", Severity.CRITICAL))).containsExactly("postgres", "pager");
    }

    @Test
    void firstMatchStopsAtTheFirstRoute() {
        Router<String> r = new Router<>(Router.Policy.FIRST_MATCH, routes, List.of("archive"));
        assertThat(r.route(Events.of("auth", Severity.HIGH))).containsExactly("neo4j", "postgres");
    }

    @Test
    void unmatchedEventsTakeTheDefaultRoute() {
        Router<String> r = new Router<>(Router.Policy.ALL_MATCHING, routes, List.of("archive"));
        assertThat(r.route(Events.of("network", Severity.INFO))).containsExactly("archive");
    }

    @Test
    void mapResolvesTargets() {
        Router<Integer> r = new Router<>(Router.Policy.ALL_MATCHING, routes, List.of("archive")).map(String::length);
        assertThat(r.route(Events.of("auth"))).containsExactly(5, 8);
    }
}
