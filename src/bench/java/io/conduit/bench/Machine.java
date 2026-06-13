package io.conduit.bench;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** What the benchmark ran on, recorded next to every result. */
final class Machine {
    private Machine() {
    }

    static Map<String, Object> describe() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("cpu", cpu());
        m.put("cores", Runtime.getRuntime().availableProcessors());
        m.put("memoryGb", Math.round(physicalMemoryBytes() / (1024.0 * 1024 * 1024)));
        m.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version") + " "
                + System.getProperty("os.arch"));
        m.put("java", System.getProperty("java.vm.name") + " " + System.getProperty("java.version"));
        m.put("maxHeapMb", Runtime.getRuntime().maxMemory() >> 20);
        return m;
    }

    private static String cpu() {
        String os = System.getProperty("os.name").toLowerCase();
        try {
            if (os.contains("mac")) {
                return exec("sysctl", "-n", "machdep.cpu.brand_string");
            }
            Path info = Path.of("/proc/cpuinfo");
            if (Files.exists(info)) {
                for (String line : Files.readAllLines(info)) {
                    if (line.startsWith("model name")) {
                        return line.substring(line.indexOf(':') + 1).trim();
                    }
                }
            }
        } catch (Exception ignored) {
            // fall through
        }
        return System.getProperty("os.arch");
    }

    private static long physicalMemoryBytes() {
        java.lang.management.OperatingSystemMXBean os = java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.OperatingSystemMXBean sun) {
            return sun.getTotalMemorySize();
        }
        return 0;
    }

    private static String exec(String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String line = r.readLine();
            p.waitFor();
            return line == null ? "unknown" : line.trim();
        }
    }
}
