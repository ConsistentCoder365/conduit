package io.conduit.config;

import io.conduit.filter.FilterStage;
import io.conduit.route.Router;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The validated, read-only pipeline definition. Only {@link Builder#build()} creates one, and it
 * checks everything that can be checked without touching the network: names, references between
 * routes and sinks, ranges, and that every type has a registered provider. Workers receive this
 * object and never see a half-valid config.
 */
public final class ConduitConfig {

    public record SourceSpec(ComponentSpec component, String keyField) {
    }

    public record SinkSpec(ComponentSpec component, SinkSettings settings) {
        public String name() {
            return component.name();
        }
    }

    public record DeadLetterSettings(Path dir, long maxFileBytes, int queueCapacity) {
    }

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_.-]*");

    private final int workers;
    private final int queueCapacity;
    private final Duration shutdownTimeout;
    private final boolean metricsEnabled;
    private final int metricsPort;
    private final List<SourceSpec> sources;
    private final FilterStage filter;
    private final List<ComponentSpec> transforms;
    private final Router<String> router;
    private final List<SinkSpec> sinks;
    private final DeadLetterSettings deadLetter;

    private ConduitConfig(Builder b, Router<String> router) {
        this.workers = b.workers;
        this.queueCapacity = b.queueCapacity;
        this.shutdownTimeout = b.shutdownTimeout;
        this.metricsEnabled = b.metricsEnabled;
        this.metricsPort = b.metricsPort;
        this.sources = List.copyOf(b.sources);
        this.filter = b.filter;
        this.transforms = List.copyOf(b.transforms);
        this.router = router;
        this.sinks = List.copyOf(b.sinks);
        this.deadLetter = b.deadLetter;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int workers() {
        return workers;
    }

    public int queueCapacity() {
        return queueCapacity;
    }

    public Duration shutdownTimeout() {
        return shutdownTimeout;
    }

    public boolean metricsEnabled() {
        return metricsEnabled;
    }

    public int metricsPort() {
        return metricsPort;
    }

    public List<SourceSpec> sources() {
        return sources;
    }

    public FilterStage filter() {
        return filter;
    }

    public List<ComponentSpec> transforms() {
        return transforms;
    }

    public Router<String> router() {
        return router;
    }

    public List<SinkSpec> sinks() {
        return sinks;
    }

    public DeadLetterSettings deadLetter() {
        return deadLetter;
    }

    /** A copy with a different worker count, for benchmarks that sweep it. */
    public ConduitConfig withWorkers(int newWorkers) {
        Builder b = toBuilder();
        b.workers(newWorkers);
        return b.build();
    }

    public Builder toBuilder() {
        Builder b = new Builder()
                .workers(workers)
                .queueCapacity(queueCapacity)
                .shutdownTimeout(shutdownTimeout)
                .metrics(metricsEnabled, metricsPort)
                .filter(filter)
                .routing(router.policy(), router.defaults())
                .deadLetter(deadLetter)
                .registries(null);
        sources.forEach(b::source);
        transforms.forEach(b::transform);
        sinks.forEach(b::sink);
        router.routes().forEach(r -> b.route(r.name(), r.when(), r.targets()));
        return b;
    }

    public static final class Builder {
        private int workers = Runtime.getRuntime().availableProcessors();
        private int queueCapacity = 8192;
        private Duration shutdownTimeout = Duration.ofSeconds(30);
        private boolean metricsEnabled = true;
        private int metricsPort = 9400;
        private final List<SourceSpec> sources = new ArrayList<>();
        private FilterStage filter = FilterStage.PASS_ALL;
        private final List<ComponentSpec> transforms = new ArrayList<>();
        private final List<Router.Route<String>> routes = new ArrayList<>();
        private Router.Policy policy = Router.Policy.ALL_MATCHING;
        private List<String> defaultRoute;
        private final List<SinkSpec> sinks = new ArrayList<>();
        private DeadLetterSettings deadLetter = new DeadLetterSettings(Path.of("out", "dead-letter"), 128L << 20, 8192);
        private Registries registries = Registries.discover();

        private Builder() {
        }

        public Builder workers(int n) {
            this.workers = n;
            return this;
        }

        public Builder queueCapacity(int n) {
            this.queueCapacity = n;
            return this;
        }

        public Builder shutdownTimeout(Duration d) {
            this.shutdownTimeout = d;
            return this;
        }

        public Builder metrics(boolean enabled, int port) {
            this.metricsEnabled = enabled;
            this.metricsPort = port;
            return this;
        }

        public Builder source(SourceSpec spec) {
            sources.add(spec);
            return this;
        }

        public Builder filter(FilterStage f) {
            this.filter = Objects.requireNonNull(f);
            return this;
        }

        public Builder transform(ComponentSpec spec) {
            transforms.add(spec);
            return this;
        }

        public Builder route(String name, io.conduit.filter.Rule when, List<String> targets) {
            routes.add(new Router.Route<>(name, when, targets));
            return this;
        }

        /** {@code defaults} null means "unset": every sink if there are no routes, else required. */
        public Builder routing(Router.Policy p, List<String> defaults) {
            this.policy = p;
            this.defaultRoute = defaults;
            return this;
        }

        public Builder sink(SinkSpec spec) {
            sinks.add(spec);
            return this;
        }

        public Builder deadLetter(DeadLetterSettings d) {
            this.deadLetter = d;
            return this;
        }

        /** Null skips the provider-exists check; used when copying an already-validated config. */
        public Builder registries(Registries r) {
            this.registries = r;
            return this;
        }

        public ConduitConfig build() {
            List<String> errors = new ArrayList<>();
            if (workers < 1) {
                errors.add("pipeline.workers must be at least 1");
            }
            if (queueCapacity < 1) {
                errors.add("pipeline.queueCapacity must be at least 1");
            }
            if (shutdownTimeout.isNegative() || shutdownTimeout.isZero()) {
                errors.add("pipeline.shutdownTimeout must be positive");
            }
            if (sources.isEmpty()) {
                errors.add("at least one source is required");
            }
            if (sinks.isEmpty()) {
                errors.add("at least one sink is required");
            }

            checkNames("source", sources.stream().map(s -> s.component()).toList(), errors);
            checkNames("sink", sinks.stream().map(SinkSpec::component).toList(), errors);
            if (registries != null) {
                for (SourceSpec s : sources) {
                    checkType(() -> registries.sources().get(s.component().type(), s.component().path()), errors);
                }
                for (ComponentSpec t : transforms) {
                    checkType(() -> registries.transforms().get(t.type(), t.path()), errors);
                }
                for (SinkSpec s : sinks) {
                    checkType(() -> registries.sinks().get(s.component().type(), s.component().path()), errors);
                }
            }
            for (SinkSpec s : sinks) {
                SinkSettings st = s.settings();
                if (st.batchSize() > st.queueCapacity()) {
                    errors.add("sink '" + s.name() + "': batchSize (" + st.batchSize() + ") exceeds queueCapacity ("
                            + st.queueCapacity() + ")");
                }
            }

            Set<String> sinkNames = new LinkedHashSet<>();
            sinks.forEach(s -> sinkNames.add(s.name()));
            List<String> defaults = defaultRoute;
            if (defaults == null || defaults.isEmpty()) {
                if (routes.isEmpty() || sinkNames.size() == 1) {
                    defaults = List.copyOf(sinkNames);
                } else {
                    errors.add("routes.default is required when routes are configured and there is more than "
                            + "one sink");
                    defaults = List.copyOf(sinkNames);
                }
            }
            for (Router.Route<String> r : routes) {
                if (r.targets().isEmpty()) {
                    errors.add("route '" + r.name() + "' has no targets");
                }
                for (String t : r.targets()) {
                    if (!sinkNames.contains(t)) {
                        errors.add("route '" + r.name() + "' sends to unknown sink '" + t + "'; sinks are "
                                + sinkNames);
                    }
                }
            }
            for (String t : defaults) {
                if (!sinkNames.contains(t)) {
                    errors.add("default route sends to unknown sink '" + t + "'; sinks are " + sinkNames);
                }
            }

            if (!errors.isEmpty()) {
                throw new ConfigException(errors.size() == 1 ? errors.get(0)
                        : "invalid configuration:\n  - " + String.join("\n  - ", errors));
            }
            Router<String> router = new Router<>(policy, routes, defaults.isEmpty() ? List.copyOf(sinkNames)
                    : defaults);
            return new ConduitConfig(this, router);
        }

        private static void checkNames(String kind, List<ComponentSpec> specs, List<String> errors) {
            Set<String> seen = new HashSet<>();
            for (ComponentSpec c : specs) {
                if (!NAME.matcher(c.name()).matches()) {
                    errors.add(kind + " name '" + c.name() + "' at " + c.path()
                            + " may only use letters, digits, '.', '_' and '-'");
                }
                if (!seen.add(c.name())) {
                    errors.add("duplicate " + kind + " name '" + c.name() + "'");
                }
            }
        }

        private static void checkType(Runnable lookup, List<String> errors) {
            try {
                lookup.run();
            } catch (ConfigException e) {
                errors.add(e.getMessage());
            }
        }
    }
}
