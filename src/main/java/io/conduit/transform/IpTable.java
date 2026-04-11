package io.conduit.transform;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An immutable IPv4 CIDR lookup table, loaded from CSV:
 *
 * <pre>
 * cidr,zone,country,asn,org
 * 10.0.0.0/8,internal,,,
 * 13.64.0.0/11,external,US,8075,Microsoft
 * </pre>
 *
 * The first column is the CIDR; the header names the attributes for the rest. Lookup is longest
 * prefix match: one probe per prefix length present, most specific first, into open-addressing maps
 * keyed by the masked address as a primitive int (no boxing on the hot path). Being immutable, a
 * table can be read by any number of threads while a reload builds its replacement.
 */
public final class IpTable {
    private final int[] prefixLengths;
    private final IntMap[] byPrefix = new IntMap[33];
    private final int size;

    private IpTable(Map<Integer, Map<Integer, Map<String, Object>>> rows, int size) {
        this.prefixLengths = rows.keySet().stream().sorted(Collections.reverseOrder()).mapToInt(i -> i).toArray();
        for (Map.Entry<Integer, Map<Integer, Map<String, Object>>> e : rows.entrySet()) {
            byPrefix[e.getKey()] = new IntMap(e.getValue());
        }
        this.size = size;
    }

    public static IpTable empty() {
        return new IpTable(Map.of(), 0);
    }

    public int size() {
        return size;
    }

    public static IpTable load(Path csv) throws IOException {
        List<String> lines = Files.readAllLines(csv);
        if (lines.isEmpty()) {
            throw new IOException(csv + " is empty; expected a header like cidr,zone,country");
        }
        String[] header = lines.get(0).split(",", -1);
        if (!header[0].trim().equalsIgnoreCase("cidr")) {
            throw new IOException(csv + ": first header column must be 'cidr'");
        }
        Map<Integer, Map<Integer, Map<String, Object>>> rows = new LinkedHashMap<>();
        int count = 0;
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            String[] cols = line.split(",", -1);
            String cidr = cols[0].trim();
            int slash = cidr.indexOf('/');
            Integer addr = parseIpv4(slash < 0 ? cidr : cidr.substring(0, slash));
            int prefix;
            try {
                prefix = slash < 0 ? 32 : Integer.parseInt(cidr.substring(slash + 1));
            } catch (NumberFormatException e) {
                prefix = -1;
            }
            if (addr == null || prefix < 0 || prefix > 32) {
                throw new IOException(csv + " line " + (i + 1) + ": invalid IPv4 CIDR '" + cidr + "'");
            }
            Map<String, Object> attrs = new LinkedHashMap<>();
            for (int c = 1; c < header.length && c < cols.length; c++) {
                String v = cols[c].trim();
                if (!v.isEmpty()) {
                    attrs.put(header[c].trim(), v);
                }
            }
            rows.computeIfAbsent(prefix, k -> new LinkedHashMap<>()).put(addr & mask(prefix),
                    Collections.unmodifiableMap(attrs));
            count++;
        }
        return new IpTable(rows, count);
    }

    /** The attributes of the most specific matching range, or null. */
    public Map<String, Object> lookup(String ip) {
        Integer parsed = parseIpv4(ip);
        if (parsed == null) {
            return null;
        }
        int addr = parsed;
        for (int prefix : prefixLengths) {
            Map<String, Object> hit = byPrefix[prefix].get(addr & mask(prefix));
            if (hit != null) {
                return hit;
            }
        }
        return null;
    }

    private static int mask(int prefix) {
        return prefix == 0 ? 0 : -1 << (32 - prefix);
    }

    /** Fast dotted-quad parser; null for anything that is not a valid IPv4 address. */
    static Integer parseIpv4(String s) {
        if (s == null || s.length() < 7 || s.length() > 15) {
            return null;
        }
        int result = 0;
        int octet = -1;
        int dots = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= '0' && c <= '9') {
                octet = (octet < 0 ? 0 : octet * 10) + (c - '0');
                if (octet > 255) {
                    return null;
                }
            } else if (c == '.' && octet >= 0 && dots < 3) {
                result = (result << 8) | octet;
                octet = -1;
                dots++;
            } else {
                return null;
            }
        }
        if (dots != 3 || octet < 0) {
            return null;
        }
        return (result << 8) | octet;
    }

    /** Read-only int-keyed map with linear probing, sized to stay at most half full. */
    private static final class IntMap {
        private final int[] keys;
        private final Object[] values;
        private final int mask;

        IntMap(Map<Integer, Map<String, Object>> source) {
            int cap = Integer.highestOneBit(Math.max(2, source.size() * 2 - 1)) << 1;
            keys = new int[cap];
            values = new Object[cap];
            mask = cap - 1;
            for (Map.Entry<Integer, Map<String, Object>> e : source.entrySet()) {
                int i = mix(e.getKey()) & mask;
                while (values[i] != null) {
                    i = (i + 1) & mask;
                }
                keys[i] = e.getKey();
                values[i] = e.getValue();
            }
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> get(int key) {
            int i = mix(key) & mask;
            Object v;
            while ((v = values[i]) != null) {
                if (keys[i] == key) {
                    return (Map<String, Object>) v;
                }
                i = (i + 1) & mask;
            }
            return null;
        }

        private static int mix(int k) {
            int h = k * 0x9E3779B9;
            return h ^ (h >>> 16);
        }
    }
}
