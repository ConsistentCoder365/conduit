package io.conduit.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A source, transform or sink as written in YAML: its type name, its instance name, and the fields
 * that belong to the component itself (common fields the core handles are already removed).
 */
public record ComponentSpec(String name, String type, String path, Map<String, Object> fields) {

    public ComponentSpec {
        fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

    /** A fresh view of the fields. Each component build gets its own, so unknown-field checks are exact. */
    public Props props() {
        return new Props(path, fields);
    }
}
