package io.conduit.source;

import io.conduit.core.Json;
import io.conduit.core.MalformedRecordException;
import io.conduit.core.Severity;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses one syslog-style line into a record map for {@link io.conduit.core.EventParser}.
 *
 * <ul>
 *   <li>An optional {@code <PRI>} prefix sets severity (from the syslog code) and facility.</li>
 *   <li>A JSON body after the prefix is used as-is: {@code <134>{"type":"auth",...}}.</li>
 *   <li>An RFC 5424 body ({@code 1 TIMESTAMP HOST APP PROCID MSGID [SD] MSG}) maps the header to
 *       fields and structured-data params to attributes; MSGID becomes the type.</li>
 *   <li>Anything else is free text: {@code key=value} pairs become attributes, and the type comes
 *       from a {@code type=} pair or defaults to {@code syslog}.</li>
 * </ul>
 */
final class SyslogLineParser {
    private static final Pattern KV = Pattern.compile("([A-Za-z_][\\w.-]*)=(\"([^\"]*)\"|[^\\s\"]+)");
    private static final Pattern SD_PARAM = Pattern.compile("([^\\s=\\]]+)=\"((?:\\\\.|[^\"\\\\])*)\"");

    private SyslogLineParser() {
    }

    static Map<String, Object> parse(String line) throws MalformedRecordException {
        String rest = line.strip();
        if (rest.isEmpty()) {
            throw new MalformedRecordException("empty line");
        }
        Integer pri = null;
        if (rest.charAt(0) == '<') {
            int close = rest.indexOf('>');
            if (close < 2 || close > 4) {
                throw new MalformedRecordException("bad syslog priority prefix");
            }
            try {
                pri = Integer.parseInt(rest.substring(1, close));
            } catch (NumberFormatException e) {
                throw new MalformedRecordException("bad syslog priority prefix");
            }
            if (pri > 191) {
                throw new MalformedRecordException("syslog priority out of range: " + pri);
            }
            rest = rest.substring(close + 1);
        }

        Map<String, Object> record;
        if (rest.startsWith("{")) {
            try {
                record = Json.MAPPER.readValue(rest, Json.MAP);
            } catch (JsonProcessingException e) {
                throw new MalformedRecordException("invalid JSON after syslog prefix: " + e.getOriginalMessage());
            }
        } else if (rest.startsWith("1 ")) {
            record = rfc5424(rest.substring(2));
        } else {
            record = new LinkedHashMap<>();
            record.put("message", rest);
            extractPairs(rest, record);
        }
        if (pri != null) {
            record.putIfAbsent("severity", Severity.fromSyslog(pri % 8).name());
            record.putIfAbsent("facility", pri / 8);
        }
        record.putIfAbsent("type", "syslog");
        return record;
    }

    private static Map<String, Object> rfc5424(String s) throws MalformedRecordException {
        String[] head = s.split(" ", 6);
        if (head.length < 6) {
            throw new MalformedRecordException("truncated RFC 5424 header");
        }
        Map<String, Object> record = new LinkedHashMap<>();
        putUnlessNil(record, "timestamp", head[0]);
        putUnlessNil(record, "host", head[1]);
        putUnlessNil(record, "app", head[2]);
        putUnlessNil(record, "procid", head[3]);
        putUnlessNil(record, "type", head[4]);
        String tail = head[5];
        if (tail.startsWith("-")) {
            tail = tail.length() > 1 ? tail.substring(2) : "";
        } else if (tail.startsWith("[")) {
            int end = structuredDataEnd(tail);
            Matcher m = SD_PARAM.matcher(tail.substring(0, end));
            while (m.find()) {
                record.put(m.group(1), m.group(2).replace("\\\"", "\"").replace("\\]", "]").replace("\\\\", "\\"));
            }
            tail = end < tail.length() ? tail.substring(end).strip() : "";
        } else {
            throw new MalformedRecordException("RFC 5424 structured data must be '-' or start with '['");
        }
        if (tail.startsWith("\uFEFF")) {
            tail = tail.substring(1);
        }
        if (!tail.isEmpty()) {
            record.put("message", tail);
            extractPairs(tail, record);
        }
        return record;
    }

    private static int structuredDataEnd(String s) throws MalformedRecordException {
        boolean inQuote = false;
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '\\' && inQuote) {
                i += 2;
                continue;
            }
            if (c == '"') {
                inQuote = !inQuote;
            } else if (c == ']' && !inQuote && (i + 1 == s.length() || s.charAt(i + 1) != '[')) {
                return i + 1;
            }
            i++;
        }
        throw new MalformedRecordException("unterminated structured data");
    }

    private static void putUnlessNil(Map<String, Object> m, String k, String v) {
        if (!"-".equals(v)) {
            m.put(k, v);
        }
    }

    private static void extractPairs(String text, Map<String, Object> record) {
        Matcher m = KV.matcher(text);
        while (m.find()) {
            record.putIfAbsent(m.group(1), m.group(3) != null ? m.group(3) : m.group(2));
        }
    }
}
