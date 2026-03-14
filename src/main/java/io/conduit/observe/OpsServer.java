package io.conduit.observe;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.conduit.core.Json;
import io.conduit.pipeline.Health;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Serves {@code GET /metrics} (Prometheus text) and {@code GET /health} (JSON, 200 when UP or
 * DEGRADED, 503 when DOWN) on its own small thread pool, apart from the pipeline's threads.
 */
public final class OpsServer implements AutoCloseable {
    private final HttpServer server;
    private final ExecutorService executor;

    public OpsServer(int port, PrometheusMeterRegistry registry, Supplier<Health> health) throws IOException {
        this.executor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "ops-http");
            t.setDaemon(true);
            return t;
        });
        this.server = HttpServer.create(new InetSocketAddress(port), 16);
        server.setExecutor(executor);
        server.createContext("/metrics", ex -> {
            try (ex) {
                send(ex, 200, "text/plain; version=0.0.4; charset=utf-8",
                        registry.scrape().getBytes(StandardCharsets.UTF_8));
            }
        });
        server.createContext("/health", ex -> {
            try (ex) {
                Health h = health.get();
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("status", h.status().name());
                body.putAll(h.details());
                send(ex, h.status() == Health.Status.DOWN ? 503 : 200, "application/json",
                        Json.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(body));
            }
        });
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private static void send(HttpExchange ex, int status, String contentType, byte[] body) throws IOException {
        ex.getResponseHeaders().set("Content-Type", contentType);
        ex.sendResponseHeaders(status, body.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(body);
        }
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }
}
