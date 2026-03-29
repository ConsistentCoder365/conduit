package io.conduit.source;

import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import io.conduit.core.MalformedRecordException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accepts newline-framed syslog-style lines over TCP. The source thread accepts connections; each
 * connection is read on its own virtual thread. Emits block when the pipeline is full, so the reader
 * stops reading, the socket buffer fills, and TCP flow control pushes back on the sender.
 *
 * <pre>
 * - name: syslog
 *   type: tcp
 *   port: 5140
 *   host: 0.0.0.0
 * </pre>
 */
public final class TcpSyslogSource implements Source {
    private static final Logger log = LoggerFactory.getLogger(TcpSyslogSource.class);

    public static final class Provider implements SourceProvider {
        @Override
        public String type() {
            return "tcp";
        }

        @Override
        public Source create(Props p, ComponentContext ctx) {
            return new TcpSyslogSource(ctx.name(), p.string("host", "0.0.0.0"), p.integer("port", 0),
                    (int) p.bytes("maxLineBytes", 64 << 10), p.integer("maxConnections", 1024, 1));
        }
    }

    private final String name;
    private final String host;
    private final int port;
    private final int maxLineBytes;
    private final int maxConnections;
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
    private final Set<Thread> readers = ConcurrentHashMap.newKeySet();
    private volatile boolean running = true;
    private ServerSocket server;

    TcpSyslogSource(String name, String host, int port, int maxLineBytes, int maxConnections) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.maxLineBytes = maxLineBytes;
        this.maxConnections = maxConnections;
    }

    @Override
    public void open() throws IOException {
        server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress(host, port));
        server.setSoTimeout(250);
        log.info("source '{}' listening on tcp {}:{}", name, host, server.getLocalPort());
    }

    /** The bound port; useful when configured with port 0. */
    public int boundPort() {
        return server.getLocalPort();
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        try {
            while (running) {
                Socket s;
                try {
                    s = server.accept();
                } catch (SocketTimeoutException e) {
                    continue;
                } catch (SocketException e) {
                    if (!running) {
                        break;
                    }
                    throw e;
                }
                if (clients.size() >= maxConnections) {
                    log.warn("source '{}' refusing connection: {} already open", name, maxConnections);
                    s.close();
                    continue;
                }
                clients.add(s);
                Thread t = Thread.ofVirtual().name("source-" + name + "-conn").unstarted(() -> serve(s, ctx));
                readers.add(t);
                t.start();
            }
        } finally {
            server.close();
            for (Socket s : clients) {
                closeQuietly(s);
            }
            // The contract: when run() returns, no emit from this source is still in flight.
            for (Thread t : readers) {
                t.join();
            }
        }
    }

    private void serve(Socket socket, SourceContext ctx) {
        try (socket; BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8), 16 * 1024)) {
            String line;
            while (running && (line = in.readLine()) != null) {
                if (line.length() > maxLineBytes) {
                    ctx.deadLetter(line.substring(0, 256) + "…", "line longer than " + maxLineBytes + " bytes");
                    continue;
                }
                if (line.isBlank()) {
                    continue;
                }
                Map<String, Object> record;
                try {
                    record = SyslogLineParser.parse(line);
                } catch (MalformedRecordException e) {
                    ctx.deadLetter(line, e.getMessage());
                    continue;
                }
                ctx.emitRecord(record, line);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            if (running) {
                log.debug("source '{}' connection closed: {}", name, e.getMessage());
            }
        } finally {
            clients.remove(socket);
            readers.remove(Thread.currentThread());
        }
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public void close() throws IOException {
        if (server != null) {
            server.close();
        }
    }

    private static void closeQuietly(Socket s) {
        try {
            s.shutdownInput();
        } catch (IOException ignored) {
            // already closed
        }
    }
}
