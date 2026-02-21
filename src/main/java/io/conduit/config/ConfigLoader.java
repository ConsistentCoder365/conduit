package io.conduit.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.conduit.config.ConduitConfig.DeadLetterSettings;
import io.conduit.config.ConduitConfig.SinkSpec;
import io.conduit.config.ConduitConfig.SourceSpec;
import io.conduit.filter.FilterStage;
import io.conduit.filter.Rule;
import io.conduit.filter.Rules;
import io.conduit.route.Router;
import io.conduit.sink.RetryPolicy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads the pipeline YAML into a {@link ConduitConfig}. {@code ${VAR}} and {@code ${VAR:-default}}
 * are replaced from the environment first, so secrets and hostnames stay out of the file.
 */
public final class ConfigLoader {
    private static final Pattern ENV = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?}");
    private static final YAMLMapper YAML = new YAMLMapper();

    private ConfigLoader() {
    }

    public static ConduitConfig load(Path file) {
        String text;
        try {
            text = Files.readString(file);
        } catch (IOException e) {
            throw new ConfigException("cannot read config file " + file + ": " + e.getMessage(), e);
        }
        return parse(text, System::getenv);
    }

    public static ConduitConfig parse(String yaml) {
        return parse(yaml, System::getenv);
    }

    public static ConduitConfig parse(String yaml, Function<String, String> env) {
        Map<String, Object> tree;
        try {
            tree = YAML.readValue(substitute(yaml, env), io.conduit.core.Json.MAP);
        } catch (JsonProcessingException e) {
            throw new ConfigException("config is not valid YAML: " + e.getOriginalMessage(), e);
        }
        if (tree == null) {
            throw new ConfigException("config is empty");
        }
        return fromTree(new Props("config", tree));
    }

    static String substitute(String text, Function<String, String> env) {
        Matcher m = ENV.matcher(text);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String value = env.apply(m.group(1));
            if (value == null) {
                value = m.group(2);
            }
            if (value == null) {
                throw new ConfigException("environment variable " + m.group(1) + " is not set and has no default");
            }
            m.appendReplacement(out, Matcher.quoteReplacement(value));
        }
        m.appendTail(out);
        return out.toString();
    }

    private static ConduitConfig fromTree(Props root) {
        ConduitConfig.Builder b = ConduitConfig.builder();

        Props pipeline = root.childOrEmpty("pipeline");
        int queueCapacity = pipeline.integer("queueCapacity", 8192, 1);
        b.workers(pipeline.integer("workers", Runtime.getRuntime().availableProcessors(), 1))
                .queueCapacity(queueCapacity)
                .shutdownTimeout(pipeline.duration("shutdownTimeout", Duration.ofSeconds(30)));
        pipeline.ensureAllUsed();

        Props metrics = root.childOrEmpty("metrics");
        b.metrics(metrics.bool("enabled", true), metrics.integer("port", 9400, 0));
        metrics.ensureAllUsed();

        Props dlq = root.childOrEmpty("deadLetter");
        b.deadLetter(new DeadLetterSettings(Path.of(dlq.string("dir", "out/dead-letter")),
                dlq.bytes("maxFileBytes", 128L << 20), dlq.integer("queueCapacity", 8192, 1)));
        dlq.ensureAllUsed();

        List<Props> sources = list(root, "sources");
        for (Props s : sources) {
            String name = s.string("name");
            String type = s.string("type");
            String keyField = s.string("keyField", null);
            b.source(new SourceSpec(new ComponentSpec(name, type, s.path() + " (" + name + ")", s.remaining()),
                    keyField));
        }

        root.child("filter").ifPresent(f -> {
            Rule keep = f.has("keep") ? Rules.parse(f.raw("keep"), f.path() + ".keep") : null;
            Rule drop = f.has("drop") ? Rules.parse(f.raw("drop"), f.path() + ".drop") : null;
            f.ensureAllUsed();
            b.filter(new FilterStage(keep, drop));
        });

        for (Props t : list(root, "transforms")) {
            String type = t.string("type");
            String name = t.string("name", type);
            b.transform(new ComponentSpec(name, type, t.path() + " (" + type + ")", t.remaining()));
        }

        root.child("routes").ifPresentOrElse(r -> {
            Router.Policy policy = r.enumValue("policy", Router.Policy.class, Router.Policy.ALL_MATCHING);
            List<Props> rules = list(r, "rules");
            for (int i = 0; i < rules.size(); i++) {
                Props rule = rules.get(i);
                String name = rule.string("name", "route-" + i);
                if (!rule.has("when")) {
                    throw rule.missing("when");
                }
                Rule when = Rules.parse(rule.raw("when"), rule.path() + ".when");
                List<String> to = rule.stringList("to", null);
                if (to == null) {
                    throw rule.missing("to");
                }
                rule.ensureAllUsed();
                b.route(name, when, to);
            }
            b.routing(policy, r.stringList("default", null));
            r.ensureAllUsed();
        }, () -> b.routing(Router.Policy.ALL_MATCHING, null));

        for (Props s : list(root, "sinks")) {
            String name = s.string("name");
            String type = s.string("type");
            SinkSettings settings = sinkSettings(s, queueCapacity);
            b.sink(new SinkSpec(new ComponentSpec(name, type, s.path() + " (" + name + ")", s.remaining()), settings));
        }

        root.ensureAllUsed();
        return b.build();
    }

    private static SinkSettings sinkSettings(Props s, int defaultQueueCapacity) {
        int workers = s.integer("workers", 1, 1);
        int queueCapacity = s.integer("queueCapacity", defaultQueueCapacity, 1);
        int batchSize = s.integer("batchSize", Math.min(500, queueCapacity), 1);
        Duration linger = s.duration("linger", Duration.ofMillis(20));
        Duration offerTimeout = s.duration("offerTimeout", Duration.ofMillis(500));
        SinkSettings.Overflow overflow = s.enumValue("overflow", SinkSettings.Overflow.class,
                SinkSettings.Overflow.DEAD_LETTER);
        Duration writeTimeout = s.duration("writeTimeout", Duration.ofSeconds(30));

        Props retry = s.childOrEmpty("retry");
        RetryPolicy retryPolicy = new RetryPolicy(retry.integer("maxAttempts", 5, 1),
                retry.duration("initialBackoff", Duration.ofMillis(100)),
                retry.duration("maxBackoff", Duration.ofSeconds(5)),
                retry.decimal("multiplier", 2.0, 1.0, 10.0));
        retry.ensureAllUsed();

        Props cb = s.childOrEmpty("circuitBreaker");
        SinkSettings.Breaker breaker = new SinkSettings.Breaker(cb.bool("enabled", true),
                cb.integer("failureThreshold", 3, 1), cb.duration("openFor", Duration.ofSeconds(10)));
        cb.ensureAllUsed();

        return new SinkSettings(workers, queueCapacity, batchSize, linger, offerTimeout, overflow, writeTimeout,
                retryPolicy, breaker);
    }

    private static List<Props> list(Props parent, String key) {
        Object v = parent.raw(key);
        if (v == null) {
            return List.of();
        }
        if (!(v instanceof List<?> items)) {
            throw new ConfigException("expected a list at " + parent.path() + "." + key + ", got " + Props.describe(v));
        }
        return java.util.stream.IntStream.range(0, items.size())
                .mapToObj(i -> {
                    String path = parent.path() + "." + key + "[" + i + "]";
                    return new Props(path, Props.asMap(items.get(i), path));
                })
                .toList();
    }
}
