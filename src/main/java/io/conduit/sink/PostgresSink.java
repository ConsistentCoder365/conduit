package io.conduit.sink;

import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import io.conduit.core.Event;
import io.conduit.core.Json;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Batched inserts into PostgreSQL (FR-7). One connection per lane, one transaction per batch.
 *
 * <p>Inserts use {@code ON CONFLICT (id) DO NOTHING}: delivery is at-least-once, so a batch retried
 * after a timeout (where the first attempt may in fact have committed) cannot create duplicates.
 *
 * <pre>
 * - name: postgres
 *   type: postgres
 *   url: jdbc:postgresql://localhost:5432/conduit
 *   user: conduit
 *   password: ${PGPASSWORD}
 *   table: events
 *   createTable: true
 * </pre>
 */
public final class PostgresSink implements Sink {
    private static final Pattern IDENT = Pattern.compile("[a-z_][a-z0-9_]{0,62}(\\.[a-z_][a-z0-9_]{0,62})?");

    public static final class Provider implements SinkProvider {
        @Override
        public String type() {
            return "postgres";
        }

        @Override
        public Sink create(Props p, ComponentContext ctx) {
            String table = p.string("table", "events");
            if (!IDENT.matcher(table).matches()) {
                throw p.invalid("table", "must be a lower-case SQL identifier, optionally schema-qualified");
            }
            return new PostgresSink(p.string("url"), p.string("user", null), p.string("password", null), table,
                    p.bool("createTable", true), p.duration("queryTimeout", Duration.ofSeconds(15)));
        }
    }

    private final String url;
    private final Properties props = new Properties();
    private final String table;
    private final boolean createTable;
    private final int queryTimeoutSeconds;
    private final String insertSql;
    private volatile Connection connection;

    PostgresSink(String url, String user, String password, String table, boolean createTable, Duration queryTimeout) {
        this.url = url;
        if (user != null) {
            props.setProperty("user", user);
        }
        if (password != null) {
            props.setProperty("password", password);
        }
        props.setProperty("reWriteBatchedInserts", "true");
        props.setProperty("connectTimeout", "5");
        props.setProperty("ApplicationName", "conduit");
        this.table = table;
        this.createTable = createTable;
        this.queryTimeoutSeconds = (int) Math.max(1, queryTimeout.toSeconds());
        this.insertSql = "INSERT INTO " + table + " (id, ts, source, type, severity, key, attributes) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?::jsonb) ON CONFLICT (id) DO NOTHING";
    }

    @Override
    public void open() throws SQLException {
        Connection c = connection();
        if (createTable) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE IF NOT EXISTS " + table + " ("
                        + "id TEXT PRIMARY KEY, ts TIMESTAMPTZ NOT NULL, source TEXT NOT NULL, type TEXT NOT NULL, "
                        + "severity TEXT NOT NULL, key TEXT NOT NULL, attributes JSONB NOT NULL, "
                        + "ingested_at TIMESTAMPTZ NOT NULL DEFAULT now())");
                st.execute("CREATE INDEX IF NOT EXISTS " + table.replace('.', '_') + "_ts_idx ON " + table + " (ts)");
                c.commit();
            } catch (SQLException e) {
                c.rollback();
                throw e;
            }
        }
    }

    private Connection connection() throws SQLException {
        Connection c = connection;
        if (c == null || c.isClosed()) {
            c = DriverManager.getConnection(url, props);
            c.setAutoCommit(false);
            connection = c;
        }
        return c;
    }

    @Override
    public void write(List<Event> batch) throws SQLException {
        Connection c = connection();
        try (PreparedStatement ps = c.prepareStatement(insertSql)) {
            ps.setQueryTimeout(queryTimeoutSeconds);
            for (Event e : batch) {
                ps.setString(1, e.id());
                ps.setTimestamp(2, Timestamp.from(e.timestamp().toInstant()));
                ps.setString(3, e.source());
                ps.setString(4, e.type());
                ps.setString(5, e.severity().name());
                ps.setString(6, e.key());
                ps.setString(7, Json.attributesJson(e));
                ps.addBatch();
            }
            ps.executeBatch();
            c.commit();
        } catch (SQLException e) {
            reset();
            throw e;
        }
    }

    /** Data errors (class 22) and constraint violations (class 23) will fail every retry. */
    @Override
    public boolean isRetryable(Exception error) {
        if (error instanceof SQLException sql && sql.getSQLState() != null) {
            String cls = sql.getSQLState().substring(0, 2);
            return !cls.equals("22") && !cls.equals("23") && !cls.equals("42");
        }
        return true;
    }

    @Override
    public void reset() {
        Connection c = connection;
        connection = null;
        if (c != null) {
            try {
                c.close();
            } catch (SQLException ignored) {
                // the connection is being discarded either way
            }
        }
    }

    @Override
    public void close() {
        reset();
    }
}
