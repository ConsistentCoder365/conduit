package io.conduit.transform;

import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import io.conduit.core.Event;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Redacts personal data (FR-5). Named fields are replaced whole; with {@code detectEmails} every
 * string value, at any depth, is also scanned for email addresses, and so is the partition key.
 *
 * <p>Modes: {@code hash} (default) replaces a value with a keyed HMAC-SHA256 token, so analysts can
 * still join on "same user" without seeing who it is; {@code mask} keeps the first character and
 * domain ({@code j***@corp.com}); {@code remove} deletes named fields.
 *
 * <pre>
 * - type: redact
 *   fields: [email, phone]
 *   detectEmails: true
 *   mode: hash
 *   salt: ${CONDUIT_REDACT_SALT:-change-me}
 * </pre>
 */
public final class PiiRedactor implements Transform {

    public enum Mode { HASH, MASK, REMOVE }

    public static final class Provider implements TransformProvider {
        @Override
        public String type() {
            return "redact";
        }

        @Override
        public Transform create(Props p, ComponentContext ctx) {
            Mode mode = p.enumValue("mode", Mode.class, Mode.HASH);
            String salt = p.string("salt", null);
            if (mode == Mode.HASH && (salt == null || salt.isBlank())) {
                throw p.missing("salt");
            }
            List<String> fields = p.stringList("fields", List.of());
            boolean detect = p.bool("detectEmails", true);
            if (fields.isEmpty() && !detect) {
                throw p.invalid("fields", "nothing to redact: list fields or enable detectEmails");
            }
            return new PiiRedactor(fields, detect, mode, salt);
        }
    }

    static final Pattern EMAIL = Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}");

    private final Set<String> fields;
    private final boolean detectEmails;
    private final Mode mode;
    private final ThreadLocal<Mac> mac;

    public PiiRedactor(List<String> fields, boolean detectEmails, Mode mode, String salt) {
        this.fields = Set.copyOf(fields);
        this.detectEmails = detectEmails;
        this.mode = mode;
        byte[] key = salt == null ? new byte[0] : salt.getBytes(StandardCharsets.UTF_8);
        // Mac instances are not thread-safe; one per worker thread avoids any locking.
        this.mac = ThreadLocal.withInitial(() -> {
            try {
                Mac m = Mac.getInstance("HmacSHA256");
                m.init(new SecretKeySpec(key.length == 0 ? new byte[1] : key, "HmacSHA256"));
                return m;
            } catch (GeneralSecurityException e) {
                throw new IllegalStateException("HmacSHA256 unavailable", e);
            }
        });
    }

    @Override
    public Event apply(Event event, Chain next) {
        Map<String, Object> attrs = event.attributes();
        Map<String, Object> out = null;
        for (Map.Entry<String, Object> entry : attrs.entrySet()) {
            Object v = entry.getValue();
            Object redacted;
            if (fields.contains(entry.getKey())) {
                redacted = mode == Mode.REMOVE ? null : redactWhole(v);
            } else if (detectEmails) {
                redacted = scan(v);
            } else {
                continue;
            }
            if (redacted != v) {
                if (out == null) {
                    out = new LinkedHashMap<>(attrs);
                }
                if (redacted == null) {
                    out.remove(entry.getKey());
                } else {
                    out.put(entry.getKey(), redacted);
                }
            }
        }
        Event e = out == null ? event : event.withAttributes(out);
        if (detectEmails && e.key().indexOf('@') >= 0) {
            String key = scanString(e.key());
            if (!key.equals(e.key())) {
                e = e.withKey(key);
            }
        }
        return next.proceed(e);
    }

    private Object redactWhole(Object v) {
        if (v == null) {
            return null;
        }
        return mode == Mode.MASK ? mask(v.toString()) : hash(v.toString());
    }

    /** Returns the same instance when nothing changed, so untouched events are not copied. */
    private Object scan(Object v) {
        if (v instanceof String s) {
            return s.indexOf('@') >= 0 ? scanString(s) : s;
        }
        if (v instanceof Map<?, ?> m) {
            Map<Object, Object> copy = null;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                Object r = scan(e.getValue());
                if (r != e.getValue()) {
                    if (copy == null) {
                        copy = new LinkedHashMap<>(m);
                    }
                    copy.put(e.getKey(), r);
                }
            }
            return copy == null ? v : copy;
        }
        if (v instanceof List<?> l) {
            List<Object> copy = null;
            for (int i = 0; i < l.size(); i++) {
                Object r = scan(l.get(i));
                if (r != l.get(i)) {
                    if (copy == null) {
                        copy = new ArrayList<>(l);
                    }
                    copy.set(i, r);
                }
            }
            return copy == null ? v : copy;
        }
        return v;
    }

    private String scanString(String s) {
        Matcher m = EMAIL.matcher(s);
        if (!m.find()) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s.length());
        do {
            String replacement = mode == Mode.MASK ? mask(m.group()) : hash(m.group());
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        } while (m.find());
        m.appendTail(sb);
        return sb.toString();
    }

    String hash(String value) {
        byte[] digest = mac.get().doFinal(value.getBytes(StandardCharsets.UTF_8));
        return "redacted:" + HexFormat.of().formatHex(digest, 0, 8);
    }

    static String mask(String value) {
        int at = value.indexOf('@');
        if (at > 0) {
            return value.charAt(0) + "***" + value.substring(at);
        }
        return value.isEmpty() ? value : value.charAt(0) + "***";
    }
}
