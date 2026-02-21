package io.conduit.filter;

import io.conduit.config.ConfigException;
import io.conduit.config.Props;
import io.conduit.core.Event;
import io.conduit.core.Severity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Builds {@link Rule}s from YAML. A mapping with several keys means AND; a list means AND too.
 *
 * <pre>
 * and:  [rule, ...]            or: [rule, ...]            not: rule
 * severity: HIGH               severity: { gte: MEDIUM }  severity: { in: [LOW, HIGH] }
 * type: auth                   type: [auth, process]      source: { matches: "^edge-" }
 * key: { startsWith: svc- }
 * field: { name: dst_port, in: [22, 3389] }
 * field: { name: user, exists: true }
 * field: { name: bytes, gt: 1000000 }
 * </pre>
 */
public final class Rules {
    private static final Set<String> RULE_KEYS = Set.of("and", "or", "not", "severity", "type", "source", "key",
            "field", "always");
    private static final Set<String> STRING_OPS = Set.of("eq", "ne", "in", "notIn", "matches", "startsWith",
            "contains");
    private static final Set<String> NUMERIC_OPS = Set.of("gt", "gte", "lt", "lte");

    private Rules() {
    }

    public static Rule parse(Object node, String path) {
        if (node instanceof List<?> list) {
            return and(parseAll(list, path));
        }
        Map<String, Object> map = Props.asMap(node, path);
        if (map.isEmpty()) {
            throw new ConfigException("empty rule at " + path);
        }
        List<Rule> parts = new ArrayList<>();
        for (Map.Entry<String, Object> e : map.entrySet()) {
            parts.add(parseKey(e.getKey(), e.getValue(), path + "." + e.getKey()));
        }
        return and(parts);
    }

    private static Rule parseKey(String key, Object value, String path) {
        return switch (key) {
            case "and" -> and(parseAll(asList(value, path), path));
            case "or" -> {
                List<Rule> rules = parseAll(asList(value, path), path);
                yield rules.size() == 1 ? rules.get(0) : new Rule.Or(rules);
            }
            case "not" -> new Rule.Not(parse(value, path));
            case "always" -> Boolean.parseBoolean(String.valueOf(value)) ? Rule.ALWAYS : new Rule.Not(Rule.ALWAYS);
            case "severity" -> severity(value, path);
            case "type" -> stringRule("type", Event::type, value, path);
            case "source" -> stringRule("source", Event::source, value, path);
            case "key" -> stringRule("key", Event::key, value, path);
            case "field" -> field(value, path);
            default -> throw new ConfigException("unknown rule '" + key + "' at " + path + "; expected one of "
                    + RULE_KEYS);
        };
    }

    private static List<Rule> parseAll(List<?> nodes, String path) {
        if (nodes.isEmpty()) {
            throw new ConfigException("empty rule list at " + path);
        }
        List<Rule> rules = new ArrayList<>(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            rules.add(parse(nodes.get(i), path + "[" + i + "]"));
        }
        return rules;
    }

    private static Rule and(List<Rule> rules) {
        return rules.size() == 1 ? rules.get(0) : new Rule.And(List.copyOf(rules));
    }

    // ---- severity ---------------------------------------------------------------------------

    private static Rule severity(Object value, String path) {
        Map<String, Object> ops = value instanceof Map<?, ?> ? Props.asMap(value, path)
                : value instanceof List<?> ? Map.of("in", value) : Map.of("eq", value);
        List<Rule> parts = new ArrayList<>();
        for (Map.Entry<String, Object> op : ops.entrySet()) {
            String desc = "severity " + op.getKey() + " " + op.getValue();
            if (op.getKey().equals("in")) {
                Set<Severity> allowed = new HashSet<>();
                for (Object o : asList(op.getValue(), path + ".in")) {
                    allowed.add(sev(o, path));
                }
                parts.add(new Rule.Leaf(desc, e -> allowed.contains(e.severity())));
                continue;
            }
            int target = sev(op.getValue(), path).ordinal();
            Predicate<Event> p = switch (op.getKey()) {
                case "eq" -> e -> e.severity().ordinal() == target;
                case "ne" -> e -> e.severity().ordinal() != target;
                case "gt" -> e -> e.severity().ordinal() > target;
                case "gte" -> e -> e.severity().ordinal() >= target;
                case "lt" -> e -> e.severity().ordinal() < target;
                case "lte" -> e -> e.severity().ordinal() <= target;
                default -> throw new ConfigException("unknown severity operator '" + op.getKey() + "' at " + path
                        + "; expected eq, ne, gt, gte, lt, lte or in");
            };
            parts.add(new Rule.Leaf(desc, p));
        }
        return and(parts);
    }

    private static Severity sev(Object v, String path) {
        try {
            return Severity.parse(v);
        } catch (IllegalArgumentException e) {
            throw new ConfigException(e.getMessage() + " at " + path);
        }
    }

    // ---- strings and fields -----------------------------------------------------------------

    private static Rule stringRule(String name, Function<Event, String> getter, Object value, String path) {
        Map<String, Object> ops = value instanceof Map<?, ?> ? Props.asMap(value, path)
                : value instanceof List<?> ? Map.of("in", value) : Map.of("eq", value);
        List<Rule> parts = new ArrayList<>();
        for (Map.Entry<String, Object> op : ops.entrySet()) {
            Predicate<Object> test = stringOp(op.getKey(), op.getValue(), path);
            parts.add(new Rule.Leaf(name + " " + op.getKey() + " " + op.getValue(), e -> test.test(getter.apply(e))));
        }
        return and(parts);
    }

    private static Rule field(Object value, String path) {
        Map<String, Object> spec = new LinkedHashMap<>(Props.asMap(value, path));
        Object name = spec.remove("name");
        if (name == null) {
            throw new ConfigException("missing required field 'name' at " + path);
        }
        String fieldPath = name.toString();
        if (spec.isEmpty()) {
            throw new ConfigException("field rule at " + path + " needs an operator such as eq, in, matches, "
                    + "exists or gt");
        }
        List<Rule> parts = new ArrayList<>();
        for (Map.Entry<String, Object> op : spec.entrySet()) {
            String desc = fieldPath + " " + op.getKey() + " " + op.getValue();
            Predicate<Object> test;
            if (op.getKey().equals("exists")) {
                boolean want = Boolean.parseBoolean(String.valueOf(op.getValue()));
                test = v -> (v != null) == want;
            } else if (NUMERIC_OPS.contains(op.getKey())) {
                test = numericOp(op.getKey(), op.getValue(), path);
            } else {
                test = stringOp(op.getKey(), op.getValue(), path);
            }
            parts.add(new Rule.Leaf(desc, e -> test.test(e.field(fieldPath))));
        }
        return and(parts);
    }

    private static Predicate<Object> stringOp(String op, Object arg, String path) {
        return switch (op) {
            case "eq" -> {
                String s = String.valueOf(arg);
                yield v -> v != null && s.equals(v.toString());
            }
            case "ne" -> {
                String s = String.valueOf(arg);
                yield v -> v == null || !s.equals(v.toString());
            }
            case "in" -> {
                Set<String> set = stringSet(asList(arg, path + ".in"));
                yield v -> v != null && set.contains(v.toString());
            }
            case "notIn" -> {
                Set<String> set = stringSet(asList(arg, path + ".notIn"));
                yield v -> v == null || !set.contains(v.toString());
            }
            case "matches" -> {
                Pattern p = compile(String.valueOf(arg), path);
                yield v -> v != null && p.matcher(v.toString()).find();
            }
            case "startsWith" -> {
                String s = String.valueOf(arg);
                yield v -> v != null && v.toString().startsWith(s);
            }
            case "contains" -> {
                String s = String.valueOf(arg);
                yield v -> v != null && v.toString().contains(s);
            }
            default -> throw new ConfigException("unknown operator '" + op + "' at " + path + "; expected one of "
                    + STRING_OPS + (path.contains("field") ? ", exists or " + NUMERIC_OPS : ""));
        };
    }

    private static Predicate<Object> numericOp(String op, Object arg, String path) {
        double target;
        try {
            target = arg instanceof Number n ? n.doubleValue() : Double.parseDouble(String.valueOf(arg));
        } catch (NumberFormatException e) {
            throw new ConfigException("operator '" + op + "' at " + path + " needs a number, got '" + arg + "'");
        }
        return v -> {
            double d;
            if (v instanceof Number n) {
                d = n.doubleValue();
            } else if (v != null) {
                try {
                    d = Double.parseDouble(v.toString());
                } catch (NumberFormatException e) {
                    return false;
                }
            } else {
                return false;
            }
            return switch (op) {
                case "gt" -> d > target;
                case "gte" -> d >= target;
                case "lt" -> d < target;
                default -> d <= target;
            };
        };
    }

    private static Pattern compile(String regex, String path) {
        try {
            return Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new ConfigException("invalid regex at " + path + ": " + e.getDescription());
        }
    }

    private static Set<String> stringSet(Collection<?> values) {
        Set<String> set = new HashSet<>();
        for (Object o : values) {
            set.add(String.valueOf(o));
        }
        return Set.copyOf(set);
    }

    private static List<?> asList(Object v, String path) {
        if (v instanceof List<?> l) {
            return l;
        }
        throw new ConfigException("expected a list at " + path + ", got " + Props.describe(v));
    }
}
