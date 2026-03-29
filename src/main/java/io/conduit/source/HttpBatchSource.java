package io.conduit.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import io.conduit.core.Event;
import io.conduit.core.Json;
import io.conduit.core.MalformedRecordException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accepts batches over HTTP: {@code POST /ingest} with a JSON array of records or newline-delimited
 * JSON.
 *
 * <p>Backpressure is explicit here. Records are handed over in order, each waiting at most
 * {@code offerTimeout} for space. If the pipeline stays full the request ends with {@code 429 Too
 * Many Requests}, a {@code Retry-After} header, and a body saying how far it got:
 * <pre>{"accepted": 120, "deadLettered": 1, "rejected": 379, "resumeFrom": 121}</pre>
 * The client resends records from index {@code resumeFrom}. Malformed records are dead-lettered and
 * counted as handled, so they are never resent.
 */
public final class HttpBatchSource implements Source {
    private static final Logger log = LoggerFactory.getLogger(HttpBatchSource.class);

    public static final class Provider implements SourceProvider {
        @Override
        public String type() {
            return "http";
        }

        @Override
        public Source create(Props p, ComponentContext ctx) {
            return new HttpBatchSource(ctx.name(), p.string("host", "0.0.0.0"), p.integer("port", 0),
                    p.string("path", "/ingest"), p.duration("offerTimeout", Duration.ofMillis(100)),
                    p.duration("retryAfter", Duration.ofSeconds(1)), p.bytes("maxBodyBytes", 16L << 20),
                    p.integer("threads", 4, 1));
        }
    }

    private final String name;
    private final String host;
    private final int port;
    private final String path;
    private final Duration offerTimeout;
    private final long retryAfterSeconds;
    private final long maxBodyBytes;
    private final int threads;
    private final CountDownLatch stopped = new CountDownLatch(1);
    private volatile boolean running = true;
    private volatile SourceContext ctx;
    private HttpServer server;
    private ExecutorService executor;

    HttpBatchSource(String name, String host, int port, String path, Duration offerTimeout, Duration retryAfter,
                    long maxBodyBytes, int threads) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.path = path;
        this.offerTimeout = offerTimeout;
        this.retryAfterSeconds = Math.max(1, retryAfter.toSeconds());
        this.maxBodyBytes = maxBodyBytes;
        this.threads = threads;
    }

    @Override
    public void open() throws IOException {
        executor = Executors.newFixedThreadPool(threads, r -> new Thread(r, "source-" + name + "-http"));
        server = HttpServer.create(new InetSocketAddress(host, port), 128);
        server.setExecutor(executor);
        server.createContext(path, this::handle);
        log.info("source '{}' listening on http://{}:{}{}", name, host, server.getAddress().getPort(), path);
    }

    public int boundPort() {
        return server.getAddress().getPort();
    }

    @Override
    public void run(SourceContext context) throws InterruptedException {
        this.ctx = context;
        server.start();
        stopped.await();
        server.stop(1);
        // Handlers run on our executor; wait for them so no emit outlives run().
        executor.shutdown();
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private void handle(HttpExchange ex) throws IOException {
        try (ex) {
            if (!"POST".equals(ex.getRequestMethod())) {
                ex.getResponseHeaders().add("Allow", "POST");
                respond(ex, 405, Map.of("error", "use POST"));
                return;
            }
            if (!running) {
                respond(ex, 503, Map.of("error", "shutting down"));
                return;
            }
            byte[] body;
            try (InputStream in = ex.getRequestBody()) {
                body = in.readNBytes((int) Math.min(Integer.MAX_VALUE - 8, maxBodyBytes + 1));
            }
            if (body.length > maxBodyBytes) {
                respond(ex, 413, Map.of("error", "body larger than " + maxBodyBytes + " bytes"));
                return;
            }
            List<Item> items = split(new String(body, StandardCharsets.UTF_8));
            ingest(ex, items);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            respond(ex, 503, Map.of("error", "shutting down"));
        }
    }

    private void ingest(HttpExchange ex, List<Item> items) throws IOException, InterruptedException {
        int accepted = 0;
        int deadLettered = 0;
        for (int i = 0; i < items.size(); i++) {
            Item item = items.get(i);
            Event event = null;
            String error = item.error();
            if (error == null) {
                try {
                    event = ctx.parser().fromMap(item.record(), name, ctx.keyField());
                } catch (MalformedRecordException e) {
                    error = e.getMessage();
                }
            }
            if (error != null) {
                ctx.deadLetter(item.raw(), error);
                deadLettered++;
                continue;
            }
            if (!running || !ctx.offer(event, offerTimeout)) {
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("accepted", accepted);
                body.put("deadLettered", deadLettered);
                body.put("rejected", items.size() - i);
                body.put("resumeFrom", i);
                ex.getResponseHeaders().add("Retry-After", String.valueOf(retryAfterSeconds));
                respond(ex, running ? 429 : 503, body);
                return;
            }
            accepted++;
        }
        respond(ex, 202, Map.of("accepted", accepted, "deadLettered", deadLettered, "rejected", 0));
    }

    record Item(Map<String, Object> record, String raw, String error) {
    }

    /** A JSON array or NDJSON. Elements that are not objects become items carrying an error. */
    static List<Item> split(String body) {
        List<Item> items = new ArrayList<>();
        String trimmed = body.strip();
        if (trimmed.startsWith("[")) {
            JsonNode root;
            try {
                root = Json.MAPPER.readTree(trimmed);
            } catch (JsonProcessingException e) {
                items.add(new Item(null, abbreviate(trimmed), "invalid JSON array: " + e.getOriginalMessage()));
                return items;
            }
            for (JsonNode n : root) {
                if (n.isObject()) {
                    items.add(new Item(Json.MAPPER.convertValue(n, Json.MAP), n.toString(), null));
                } else {
                    items.add(new Item(null, n.toString(), "array element is not a JSON object"));
                }
            }
            return items;
        }
        for (String line : trimmed.split("\n")) {
            if (line.isBlank()) {
                continue;
            }
            try {
                Map<String, Object> m = Json.MAPPER.readValue(line, Json.MAP);
                items.add(m == null ? new Item(null, line, "record is not a JSON object") : new Item(m, line, null));
            } catch (JsonProcessingException e) {
                items.add(new Item(null, line, "invalid JSON: " + e.getOriginalMessage()));
            }
        }
        return items;
    }

    private static String abbreviate(String s) {
        return s.length() <= 1024 ? s : s.substring(0, 1024) + "…";
    }

    private static void respond(HttpExchange ex, int status, Map<String, ?> body) throws IOException {
        byte[] bytes = Json.MAPPER.writeValueAsBytes(body);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Override
    public void stop() {
        running = false;
        stopped.countDown();
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
