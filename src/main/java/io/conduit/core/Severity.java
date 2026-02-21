package io.conduit.core;

import java.util.Locale;

/** Event severity, ordered from least to most severe so rules can compare with {@code gte}/{@code lt}. */
public enum Severity {
    INFO, LOW, MEDIUM, HIGH, CRITICAL;

    private static final Severity[] VALUES = values();

    /** Parses a name, a common alias (warn, error, fatal…) or an ordinal 0–4. Null means INFO. */
    public static Severity parse(Object raw) {
        if (raw == null) {
            return INFO;
        }
        if (raw instanceof Number n) {
            int i = n.intValue();
            if (i < 0 || i >= VALUES.length || n.doubleValue() != i) {
                throw new IllegalArgumentException("severity out of range 0-4: " + raw);
            }
            return VALUES[i];
        }
        return switch (raw.toString().trim().toLowerCase(Locale.ROOT)) {
            case "info", "informational", "notice", "debug", "trace", "0" -> INFO;
            case "low", "1" -> LOW;
            case "medium", "warn", "warning", "2" -> MEDIUM;
            case "high", "error", "err", "3" -> HIGH;
            case "critical", "crit", "fatal", "alert", "emergency", "emerg", "4" -> CRITICAL;
            default -> throw new IllegalArgumentException("unknown severity: " + raw);
        };
    }

    /** Maps a syslog severity code (0 = emergency … 7 = debug). */
    public static Severity fromSyslog(int code) {
        return switch (code) {
            case 0, 1, 2 -> CRITICAL;
            case 3 -> HIGH;
            case 4 -> MEDIUM;
            case 5 -> LOW;
            default -> INFO;
        };
    }
}
