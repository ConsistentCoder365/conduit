package io.conduit.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Typed, path-aware access to one YAML mapping. Every read marks the key as used, so
 * {@link #ensureAllUsed()} can reject typos and unknown fields instead of silently ignoring them.
 * Not thread-safe; used only while the pipeline is being built.
 */
public final class Props {
    private static final Pattern DURATION = Pattern.compile("(\\d+)\\s*(ms|s|m|h)");
    private static final Pattern SIZE = Pattern.compile("(\\d+)\\s*(b|kb|mb|gb)?");

    private final String path;
    private final Map<String, Object> values;
    private final Set<String> used = new LinkedHashSet<>();

    public Props(String path, Map<String, Object> values) {
        this.path = path;
        this.values = values == null ? Map.of() : values;
    }

    public String path() {
        return path;
    }

    public boolean has(String key) {
        return values.containsKey(key);
    }

    public Object raw(String key) {
        used.add(key);
        return values.get(key);
    }

    public String string(String key) {
        Object v = raw(key);
        if (v == null || v.toString().isBlank()) {
            throw missing(key);
        }
        return v.toString();
    }

    public String string(String key, String def) {
        Object v = raw(key);
        return v == null ? def : v.toString();
    }

    public int integer(String key, int def, int min) {
        Object v = raw(key);
        if (v == null) {
            return def;
        }
        int i;
        if (v instanceof Number n && n.doubleValue() == n.intValue()) {
            i = n.intValue();
        } else {
            try {
                i = Integer.parseInt(v.toString().trim());
            } catch (NumberFormatException e) {
                throw invalid(key, "expected an integer, got '" + v + "'");
            }
        }
        if (i < min) {
            throw invalid(key, "must be at least " + min + ", got " + i);
        }
        return i;
    }

    public int integer(String key, int min) {
        if (values.get(key) == null) {
            raw(key);
            throw missing(key);
        }
        return integer(key, 0, min);
    }

    public long longValue(String key, long def, long min) {
        Object v = raw(key);
        if (v == null) {
            return def;
        }
        long l;
        try {
            l = v instanceof Number n ? n.longValue() : Long.parseLong(v.toString().trim().replace("_", ""));
        } catch (NumberFormatException e) {
            throw invalid(key, "expected an integer, got '" + v + "'");
        }
        if (l < min) {
            throw invalid(key, "must be at least " + min + ", got " + l);
        }
        return l;
    }

    public double decimal(String key, double def, double min, double max) {
        Object v = raw(key);
        if (v == null) {
            return def;
        }
        double d;
        try {
            d = v instanceof Number n ? n.doubleValue() : Double.parseDouble(v.toString().trim());
        } catch (NumberFormatException e) {
            throw invalid(key, "expected a number, got '" + v + "'");
        }
        if (d < min || d > max) {
            throw invalid(key, "must be between " + min + " and " + max + ", got " + d);
        }
        return d;
    }

    public boolean bool(String key, boolean def) {
        Object v = raw(key);
        if (v == null) {
            return def;
        }
        if (v instanceof Boolean b) {
            return b;
        }
        return switch (v.toString().trim().toLowerCase(Locale.ROOT)) {
            case "true", "yes", "on" -> true;
            case "false", "no", "off" -> false;
            default -> throw invalid(key, "expected true or false, got '" + v + "'");
        };
    }

    /** Accepts {@code 250ms}, {@code 5s}, {@code 2m}, {@code 1h}, or a bare number of milliseconds. */
    public Duration duration(String key, Duration def) {
        Object v = raw(key);
        if (v == null) {
            return def;
        }
        if (v instanceof Number n) {
            return Duration.ofMillis(n.longValue());
        }
        Matcher m = DURATION.matcher(v.toString().trim().toLowerCase(Locale.ROOT));
        if (!m.matches()) {
            throw invalid(key, "expected a duration like 250ms, 5s, 2m or 1h, got '" + v + "'");
        }
        long amount = Long.parseLong(m.group(1));
        return switch (m.group(2)) {
            case "ms" -> Duration.ofMillis(amount);
            case "s" -> Duration.ofSeconds(amount);
            case "m" -> Duration.ofMinutes(amount);
            default -> Duration.ofHours(amount);
        };
    }

    /** Accepts {@code 512KB}, {@code 64MB}, {@code 1GB} or a bare byte count. */
    public long bytes(String key, long def) {
        Object v = raw(key);
        if (v == null) {
            return def;
        }
        if (v instanceof Number n) {
            return n.longValue();
        }
        Matcher m = SIZE.matcher(v.toString().trim().toLowerCase(Locale.ROOT));
        if (!m.matches()) {
            throw invalid(key, "expected a size like 512KB, 64MB or 1GB, got '" + v + "'");
        }
        long amount = Long.parseLong(m.group(1));
        String unit = m.group(2) == null ? "b" : m.group(2);
        return switch (unit) {
            case "kb" -> amount << 10;
            case "mb" -> amount << 20;
            case "gb" -> amount << 30;
            default -> amount;
        };
    }

    public List<String> stringList(String key, List<String> def) {
        Object v = raw(key);
        if (v == null) {
            return def;
        }
        if (v instanceof List<?> list) {
            List<String> out = new ArrayList<>(list.size());
            for (Object o : list) {
                out.add(String.valueOf(o));
            }
            return List.copyOf(out);
        }
        return List.of(v.toString());
    }

    public <E extends Enum<E>> E enumValue(String key, Class<E> type, E def) {
        Object v = raw(key);
        if (v == null) {
            return def;
        }
        String name = v.toString().trim().toUpperCase(Locale.ROOT).replace('-', '_');
        try {
            return Enum.valueOf(type, name);
        } catch (IllegalArgumentException e) {
            List<String> allowed = new ArrayList<>();
            for (E c : type.getEnumConstants()) {
                allowed.add(c.name().toLowerCase(Locale.ROOT).replace('_', '-'));
            }
            throw invalid(key, "expected one of " + allowed + ", got '" + v + "'");
        }
    }

    public Optional<Props> child(String key) {
        Object v = raw(key);
        if (v == null) {
            return Optional.empty();
        }
        return Optional.of(new Props(path + "." + key, asMap(v, path + "." + key)));
    }

    public Props childOrEmpty(String key) {
        return child(key).orElseGet(() -> new Props(path + "." + key, Map.of()));
    }

    /** Everything not yet read, as a fresh mapping. Used to hand a component its own fields. */
    public Map<String, Object> remaining() {
        Map<String, Object> rest = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : values.entrySet()) {
            if (!used.contains(e.getKey())) {
                rest.put(e.getKey(), e.getValue());
            }
        }
        return Collections.unmodifiableMap(rest);
    }

    public void ensureAllUsed() {
        Set<String> unknown = new TreeSet<>(values.keySet());
        unknown.removeAll(used);
        if (!unknown.isEmpty()) {
            throw new ConfigException("unknown field(s) " + unknown + " at " + path);
        }
    }

    public ConfigException missing(String key) {
        return new ConfigException("missing required field '" + key + "' at " + path);
    }

    public ConfigException invalid(String key, String why) {
        return new ConfigException("invalid '" + key + "' at " + path + ": " + why);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> asMap(Object v, String where) {
        if (v instanceof Map<?, ?> m) {
            return (Map<String, Object>) m;
        }
        throw new ConfigException("expected a mapping at " + where + ", got " + describe(v));
    }

    public static String describe(Object v) {
        if (v == null) {
            return "nothing";
        }
        if (v instanceof List<?>) {
            return "a list";
        }
        if (v instanceof Map<?, ?>) {
            return "a mapping";
        }
        return "'" + v + "'";
    }
}
