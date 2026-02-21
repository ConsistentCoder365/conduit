package io.conduit.config;

import io.conduit.core.Provider;
import io.conduit.sink.SinkProvider;
import io.conduit.source.SourceProvider;
import io.conduit.transform.TransformProvider;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;

/**
 * The factory registry: maps YAML type names to providers found on the classpath. Adding a type
 * means adding a provider, never editing this class.
 */
public final class Registries {

    public static final class Registry<P extends Provider<?>> {
        private final String kind;
        private final Map<String, P> byType;

        Registry(String kind, Class<P> type) {
            this.kind = kind;
            this.byType = new TreeMap<>();
            for (P p : ServiceLoader.load(type, Registries.class.getClassLoader())) {
                P previous = byType.put(p.type(), p);
                if (previous != null) {
                    throw new IllegalStateException("two " + kind + " providers registered for type '" + p.type()
                            + "': " + previous.getClass().getName() + " and " + p.getClass().getName());
                }
            }
        }

        public P get(String type, String path) {
            P p = byType.get(type);
            if (p == null) {
                throw new ConfigException("unknown " + kind + " type '" + type + "' at " + path + "; known types: "
                        + byType.keySet());
            }
            return p;
        }

        public boolean contains(String type) {
            return byType.containsKey(type);
        }
    }

    private static volatile Registries discovered;

    private final Registry<SourceProvider> sources;
    private final Registry<TransformProvider> transforms;
    private final Registry<SinkProvider> sinks;

    private Registries() {
        sources = new Registry<>("source", SourceProvider.class);
        transforms = new Registry<>("transform", TransformProvider.class);
        sinks = new Registry<>("sink", SinkProvider.class);
    }

    public static Registries discover() {
        Registries r = discovered;
        if (r == null) {
            r = new Registries();
            discovered = r;
        }
        return r;
    }

    public Registry<SourceProvider> sources() {
        return sources;
    }

    public Registry<TransformProvider> transforms() {
        return transforms;
    }

    public Registry<SinkProvider> sinks() {
        return sinks;
    }
}
