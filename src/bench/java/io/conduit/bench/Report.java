package io.conduit.bench;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Turns the JSON results into {@code bench/results/summary.md} and two SVG figures in {@code docs/}.
 * Figures carry their own light and dark styles, so they read correctly on either GitHub theme.
 */
final class Report {
    private static final Path DOCS = Path.of("docs");

    private static final String STYLE = """
            <style>
              .viz { --surface:#fcfcfb; --ink:#0b0b0b; --ink2:#52514e; --muted:#8a8984; --grid:#e6e5e0;
                     --s1:#2a78d6; --s2:#eb6834; --band:#f0efea; }
              @media (prefers-color-scheme: dark) {
                .viz { --surface:#1a1a19; --ink:#f0efec; --ink2:#c3c2b7; --muted:#8f8e86; --grid:#2e2e2c;
                       --s1:#3987e5; --s2:#d95926; --band:#252523; }
              }
              .viz text { font-family: -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; }
              .t { fill: var(--ink); font-size: 15px; font-weight: 600; }
              .st { fill: var(--ink2); font-size: 12px; }
              .ax { fill: var(--ink2); font-size: 11px; }
              .mu { fill: var(--muted); font-size: 11px; }
              .lb { fill: var(--ink); font-size: 11px; font-weight: 600; }
              .grid { stroke: var(--grid); stroke-width: 1; }
            </style>
            """;

    private Report() {
    }

    static void write(Path results) throws IOException {
        Files.createDirectories(DOCS);
        JsonNode scaling = Benchmark.readResult("scaling");
        JsonNode isolation = Benchmark.readResult("isolation");
        if (scaling != null) {
            Files.writeString(DOCS.resolve("throughput.svg"), throughputSvg(scaling));
        }
        if (isolation != null) {
            Files.writeString(DOCS.resolve("isolation.svg"), isolationSvg(isolation));
        }
        Files.writeString(results.resolve("summary.md"), markdown());
        Benchmark.log("wrote %s, docs/throughput.svg, docs/isolation.svg", results.resolve("summary.md"));
    }

    // ---- throughput vs workers ----------------------------------------------------------------

    static String throughputSvg(JsonNode s) {
        JsonNode rows = s.get("results");
        int n = rows.size();
        double base = rows.get(0).get("median").asDouble();
        double maxY = 0;
        for (JsonNode r : rows) {
            maxY = Math.max(maxY, r.get("max").asDouble());
        }
        maxY = Math.max(maxY, Math.min(base * 4.2, maxY * 1.6));
        double yMax = niceCeil(maxY);

        int w = 720;
        int h = 400;
        int left = 70;
        int right = 30;
        int top = 70;
        int bottom = 78;
        double pw = w - left - right;
        double ph = h - top - bottom;
        double step = pw / (n - 1);

        StringBuilder sb = header(w, h);
        sb.append(text(left - 50, 28, "t", "Throughput vs processing workers"));
        sb.append(text(left - 50, 48, "st", String.format(Locale.ROOT,
                "Median of %d runs, %,d events each, synthetic → filter → 3 transforms → %s sink. Whiskers: min–max.",
                rows.get(0).get("runs").size(), s.get("eventsPerRun").asLong(), s.get("sink").asText())));

        for (int i = 0; i <= 4; i++) {
            double v = yMax * i / 4;
            double y = top + ph - ph * v / yMax;
            sb.append(line(left, y, w - right, y, "grid"));
            sb.append(String.format(Locale.ROOT, "<text class=\"ax\" x=\"%d\" y=\"%.1f\" text-anchor=\"end\">%s</text>%n",
                    left - 8, y + 4, compact(v)));
        }
        sb.append(String.format(Locale.ROOT,
                "<text class=\"ax\" transform=\"translate(14,%.0f) rotate(-90)\" text-anchor=\"middle\">events / second</text>%n",
                top + ph / 2));

        // Reference: perfect linear scaling from the single-worker median, clipped to the plot.
        StringBuilder ideal = new StringBuilder();
        double lastIdealX = 0;
        double lastIdealY = 0;
        for (int i = 0; i < n; i++) {
            double v = base * rows.get(i).get("workers").asInt();
            if (v > yMax) {
                break;
            }
            lastIdealX = left + step * i;
            lastIdealY = top + ph - ph * v / yMax;
            ideal.append(i == 0 ? "M" : "L").append(fmt(lastIdealX)).append(',').append(fmt(lastIdealY));
        }
        sb.append("<path d=\"").append(ideal).append("\" fill=\"none\" stroke=\"var(--muted)\" stroke-width=\"1.5\" "
                + "stroke-dasharray=\"5 4\"/>\n");
        sb.append(String.format(Locale.ROOT, "<text class=\"mu\" x=\"%.1f\" y=\"%.1f\">linear scaling</text>%n",
                lastIdealX + 8, lastIdealY + 4));

        StringBuilder path = new StringBuilder();
        int peak = 0;
        for (int i = 0; i < n; i++) {
            if (rows.get(i).get("median").asDouble() > rows.get(peak).get("median").asDouble()) {
                peak = i;
            }
        }
        for (int i = 0; i < n; i++) {
            JsonNode r = rows.get(i);
            double x = left + step * i;
            double y = top + ph - ph * r.get("median").asDouble() / yMax;
            double yMin = top + ph - ph * r.get("min").asDouble() / yMax;
            double yHi = top + ph - ph * r.get("max").asDouble() / yMax;
            path.append(i == 0 ? "M" : "L").append(fmt(x)).append(',').append(fmt(y));
            sb.append(String.format(Locale.ROOT,
                    "<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"var(--s1)\" stroke-width=\"2\" "
                            + "stroke-linecap=\"round\" opacity=\"0.45\"/>%n", x, yMin, x, yHi));
            sb.append(String.format(Locale.ROOT, "<text class=\"ax\" x=\"%.1f\" y=\"%d\" text-anchor=\"middle\">%d</text>%n",
                    x, top + (int) ph + 20, r.get("workers").asInt()));
            if (r.has("coresUsed")) {
                sb.append(String.format(Locale.ROOT,
                        "<text class=\"mu\" x=\"%.1f\" y=\"%d\" text-anchor=\"middle\">%.1f cores</text>%n",
                        x, top + (int) ph + 36, r.get("coresUsed").asDouble()));
            }
        }
        sb.append(String.format(Locale.ROOT,
                "<text class=\"ax\" x=\"%.1f\" y=\"%d\" text-anchor=\"middle\">processing workers (CPU cores busy below)</text>%n",
                left + pw / 2, h - 12));
        sb.append("<path d=\"").append(path).append("\" fill=\"none\" stroke=\"var(--s1)\" stroke-width=\"2\" "
                + "stroke-linejoin=\"round\"/>\n");
        for (int i = 0; i < n; i++) {
            JsonNode r = rows.get(i);
            double x = left + step * i;
            double y = top + ph - ph * r.get("median").asDouble() / yMax;
            sb.append(String.format(Locale.ROOT, "<circle cx=\"%.1f\" cy=\"%.1f\" r=\"4.5\" fill=\"var(--s1)\" "
                    + "stroke=\"var(--surface)\" stroke-width=\"2\"><title>%d workers: %,.0f events/s</title></circle>%n",
                    x, y, r.get("workers").asInt(), r.get("median").asDouble()));
            if (i == 0 || i == peak) {
                String label = i == 0 ? compact(r.get("median").asDouble())
                        : String.format(Locale.ROOT, "%s  (%.2f×)", compact(r.get("median").asDouble()),
                        r.get("median").asDouble() / base);
                sb.append(String.format(Locale.ROOT, "<text class=\"lb\" x=\"%.1f\" y=\"%.1f\" text-anchor=\"%s\">%s</text>%n",
                        x + (i == 0 ? 8 : 0), i == 0 ? y + 20 : y - 12, i == 0 ? "start" : "middle", label));
            }
        }
        return sb.append("</svg>\n").toString();
    }

    // ---- isolation timeline -------------------------------------------------------------------

    static String isolationSvg(JsonNode s) {
        JsonNode samples = s.get("samples");
        double tMax = samples.get(samples.size() - 1).get("t").asDouble();
        double vMax = 0;
        for (JsonNode p : samples) {
            vMax = Math.max(vMax, Math.max(p.get("healthyPerSec").asDouble(), p.get("failingPerSec").asDouble()));
        }
        double yMax = niceCeil(vMax * 1.1);
        int w = 720;
        int h = 380;
        int left = 70;
        int right = 110;
        int top = 84;
        int bottom = 50;
        double pw = w - left - right;
        double ph = h - top - bottom;

        StringBuilder sb = header(w, h);
        sb.append(text(left - 50, 28, "t", "Healthy sink throughput while another sink hangs"));
        sb.append(text(left - 50, 48, "st", String.format(Locale.ROOT,
                "Both sinks get every event at %,.0f/s. Fault sink hangs 5–11 s; healthy averaged %.0f%% of normal.",
                s.get("rate").asDouble(),
                100 * s.get("duringVsBefore").asDouble())));

        double x5 = left + pw * 5 / tMax;
        double x11 = left + pw * Math.min(11, tMax) / tMax;
        sb.append(String.format(Locale.ROOT, "<rect x=\"%.1f\" y=\"%d\" width=\"%.1f\" height=\"%.1f\" fill=\"var(--band)\"/>%n",
                x5, top, x11 - x5, ph));
        sb.append(String.format(Locale.ROOT, "<text class=\"mu\" x=\"%.1f\" y=\"%d\" text-anchor=\"middle\">outage</text>%n",
                (x5 + x11) / 2, top + 14));

        for (int i = 0; i <= 4; i++) {
            double v = yMax * i / 4;
            double y = top + ph - ph * v / yMax;
            sb.append(line(left, y, left + pw, y, "grid"));
            sb.append(String.format(Locale.ROOT, "<text class=\"ax\" x=\"%d\" y=\"%.1f\" text-anchor=\"end\">%s</text>%n",
                    left - 8, y + 4, compact(v)));
        }
        for (int t = 0; t <= tMax; t += 2) {
            double x = left + pw * t / tMax;
            sb.append(String.format(Locale.ROOT, "<text class=\"ax\" x=\"%.1f\" y=\"%d\" text-anchor=\"middle\">%ds</text>%n",
                    x, top + (int) ph + 18, t));
        }
        sb.append(String.format(Locale.ROOT,
                "<text class=\"ax\" transform=\"translate(14,%.0f) rotate(-90)\" text-anchor=\"middle\">acknowledged events / second</text>%n",
                top + ph / 2));

        // Legend above the plot, plus direct labels at the line ends.
        sb.append(String.format(Locale.ROOT, "<line x1=\"%d\" y1=\"66\" x2=\"%d\" y2=\"66\" stroke=\"var(--s1)\" "
                + "stroke-width=\"2\"/><text class=\"ax\" x=\"%d\" y=\"70\">healthy file sink</text>%n", left, left + 18, left + 24));
        sb.append(String.format(Locale.ROOT, "<line x1=\"%d\" y1=\"66\" x2=\"%d\" y2=\"66\" stroke=\"var(--s2)\" "
                + "stroke-width=\"2\" stroke-dasharray=\"6 3\"/><text class=\"ax\" x=\"%d\" y=\"70\">fault sink</text>%n",
                left + 150, left + 168, left + 174));

        String[] keys = {"healthyPerSec", "failingPerSec"};
        String[] colors = {"var(--s1)", "var(--s2)"};
        String[] names = {"healthy", "fault"};
        String[] dash = {"", " stroke-dasharray=\"6 3\""};
        for (int k = 0; k < 2; k++) {
            StringBuilder path = new StringBuilder();
            double lx = 0;
            double ly = 0;
            for (int i = 0; i < samples.size(); i++) {
                JsonNode p = samples.get(i);
                lx = left + pw * p.get("t").asDouble() / tMax;
                ly = top + ph - ph * p.get(keys[k]).asDouble() / yMax;
                path.append(i == 0 ? "M" : "L").append(fmt(lx)).append(',').append(fmt(ly));
            }
            sb.append("<path d=\"").append(path).append("\" fill=\"none\" stroke=\"").append(colors[k])
                    .append("\" stroke-width=\"2\" stroke-linejoin=\"round\"").append(dash[k]).append("/>\n");
            sb.append(String.format(Locale.ROOT, "<text class=\"lb\" x=\"%.1f\" y=\"%.1f\">%s</text>%n", lx + 8,
                    ly + (k == 0 ? -4 : 12), names[k]));
        }
        return sb.append("</svg>\n").toString();
    }

    // ---- markdown -----------------------------------------------------------------------------

    static String markdown() throws IOException {
        StringBuilder md = new StringBuilder("# Benchmark results\n\n");
        JsonNode scaling = Benchmark.readResult("scaling");
        JsonNode any = scaling != null ? scaling : Benchmark.readResult("loss");
        if (any != null) {
            JsonNode m = any.get("machine");
            md.append(String.format(Locale.ROOT, "Machine: %s, %d cores, %d GB RAM, %s, %s, heap %d MB.%n%n",
                    m.get("cpu").asText(), m.get("cores").asInt(), m.get("memoryGb").asInt(), m.get("os").asText(),
                    m.get("java").asText(), m.get("maxHeapMb").asInt()));
        }
        if (scaling != null) {
            md.append(String.format(Locale.ROOT, "## Throughput scaling%n%n%,d events per run, %d runs per point, "
                            + "%d synthetic source thread(s), %s sink with %d lanes.%n%n",
                    scaling.get("eventsPerRun").asLong(), scaling.get("results").get(0).get("runs").size(),
                    scaling.path("sources").asInt(1), scaling.get("sink").asText(), scaling.get("sinkWorkers").asInt()));
            md.append("| Workers | Median events/s | Min | Max | Speedup | Cores busy | CPU µs/event |\n");
            md.append("|---:|---:|---:|---:|---:|---:|---:|\n");
            for (JsonNode r : scaling.get("results")) {
                md.append(String.format(Locale.ROOT, "| %d | %,.0f | %,.0f | %,.0f | %.2f× | %.1f | %.2f |%n",
                        r.get("workers").asInt(), r.get("median").asDouble(), r.get("min").asDouble(),
                        r.get("max").asDouble(), r.get("speedup").asDouble(), r.path("coresUsed").asDouble(),
                        r.path("cpuMicrosPerEvent").asDouble()));
            }
            md.append('\n');
        }
        JsonNode lat = Benchmark.readResult("latency");
        if (lat != null) {
            md.append(String.format(Locale.ROOT, "## End-to-end latency%n%nIngest to sink acknowledgement, %d s per rate "
                            + "after a JIT warmup, %d workers, %d ms sink linger, %s. Percentiles over a uniform sample "
                            + "of up to 64 events per batch.%n%n| Target events/s | Achieved | p50 | p95 | p99 | max |%n"
                            + "|---:|---:|---:|---:|---:|---:|%n",
                    lat.get("secondsPerRun").asInt(), lat.get("workers").asInt(), lat.get("lingerMs").asInt(),
                    lat.get("gc").asText()));
            for (JsonNode r : lat.get("runs")) {
                md.append(String.format(Locale.ROOT, "| %,.0f | %,.0f | %.1f ms | %.1f ms | %.1f ms | %.0f ms |%n",
                        r.get("targetEventsPerSec").asDouble(), r.get("achievedEventsPerSec").asDouble(),
                        r.get("p50Ms").asDouble(), r.get("p95Ms").asDouble(), r.get("p99Ms").asDouble(),
                        r.get("maxMs").asDouble()));
            }
            md.append('\n');
        }
        JsonNode loss = Benchmark.readResult("loss");
        if (loss != null) {
            JsonNode a = loss.get("accounting");
            md.append(String.format(Locale.ROOT, "## Zero loss under injected failures%n%n%,d events in %.1f s; "
                            + "every event to a healthy sink and to a fault sink failing 5%% of writes plus a 2 s outage.%n%n"
                            + "| Counter | Value |%n|---|---:|%n", loss.get("events").asLong(), loss.get("seconds").asDouble()));
            a.fields().forEachRemaining(e -> md.append(String.format(Locale.ROOT, "| %s | %,d |%n", e.getKey(),
                    e.getValue().asLong())));
            md.append(String.format(Locale.ROOT, "%nInvariant holds: **%s**. Files on disk match the counters "
                            + "(dead-letter file lines = dead-letter records counted): **%s**.%n%n",
                    loss.get("invariantHolds").asBoolean(), loss.get("filesMatchCounters").asBoolean()));
        }
        JsonNode over = Benchmark.readResult("overload");
        if (over != null) {
            md.append(String.format(Locale.ROOT, "## Bounded memory under overload%n%n%d s with an unthrottled source "
                            + "(%,.0f events/s average). Heap used: %.0f MB average in the first quarter, %.0f MB in the "
                            + "last quarter, %d MB peak, of %d MB max. Clean shutdown: **%s**.%n%n",
                    over.get("seconds").asInt(), over.get("avgEventsPerSec").asDouble(),
                    over.get("heapUsedMbFirstQuarterAvg").asDouble(), over.get("heapUsedMbLastQuarterAvg").asDouble(),
                    over.get("heapUsedMbMax").asInt(), over.get("maxHeapMb").asInt(), over.get("ok").asBoolean()));
        }
        JsonNode iso = Benchmark.readResult("isolation");
        if (iso != null) {
            md.append(String.format(Locale.ROOT, "## Failure isolation%n%nHealthy sink throughput: %,.0f/s before the "
                            + "outage, %,.0f/s during it (%.1f%%), %,.0f/s after. Clean shutdown: **%s**.%n%n",
                    iso.get("healthyPerSecBefore").asDouble(), iso.get("healthyPerSecDuringOutage").asDouble(),
                    100 * iso.get("duringVsBefore").asDouble(), iso.get("healthyPerSecAfter").asDouble(),
                    iso.get("ok").asBoolean()));
        }
        return md.toString();
    }

    // ---- svg helpers --------------------------------------------------------------------------

    private static StringBuilder header(int w, int h) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(Locale.ROOT, "<svg xmlns=\"http://www.w3.org/2000/svg\" class=\"viz\" viewBox=\"0 0 %d %d\" "
                + "width=\"%d\" height=\"%d\" role=\"img\">%n", w, h, w, h));
        sb.append(STYLE);
        sb.append(String.format(Locale.ROOT, "<rect width=\"%d\" height=\"%d\" rx=\"8\" fill=\"var(--surface)\"/>%n", w, h));
        return sb;
    }

    private static String text(int x, int y, String cls, String s) {
        return String.format(Locale.ROOT, "<text class=\"%s\" x=\"%d\" y=\"%d\">%s</text>%n", cls, x, y, escape(s));
    }

    private static String line(double x1, double y1, double x2, double y2, String cls) {
        return String.format(Locale.ROOT, "<line class=\"%s\" x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\"/>%n",
                cls, x1, y1, x2, y2);
    }

    private static String escape(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private static String fmt(double d) {
        return String.format(Locale.ROOT, "%.1f", d);
    }

    static String compact(double v) {
        if (v >= 1_000_000) {
            return String.format(Locale.ROOT, "%.2fM", v / 1_000_000).replace(".00M", "M");
        }
        if (v >= 1_000) {
            return String.format(Locale.ROOT, "%.0fk", v / 1_000);
        }
        return String.format(Locale.ROOT, "%.0f", v);
    }

    static double niceCeil(double v) {
        if (v <= 0) {
            return 1;
        }
        double mag = Math.pow(10, Math.floor(Math.log10(v)));
        for (double m : new double[] {1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10}) {
            if (m * mag >= v) {
                return m * mag;
            }
        }
        return 10 * mag;
    }
}
