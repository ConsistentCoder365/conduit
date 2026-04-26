package io.conduit.sink;

import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import io.conduit.core.Event;
import io.micrometer.core.instrument.Counter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.ClientException;

/**
 * Grows an entity graph from authentication events (FR-8):
 *
 * <pre>
 *   (:User {name}) -[:AUTHENTICATED_FROM {count, failures, first_seen, last_seen}]-> (:IP {address})
 *   (:IP {address}) -[:ACCESSED {count, first_seen, last_seen}]-> (:Host {name})
 * </pre>
 *
 * Each batch is one {@code UNWIND ... MERGE} transaction, so the graph is upserted rather than
 * appended, and uniqueness constraints created at startup keep MERGE fast and free of duplicate
 * nodes. Events that are not auth events, or lack a user, IP or host, are acknowledged and skipped
 * (counted in {@code conduit.neo4j.skipped}); route only auth events here to avoid that.
 *
 * <p>Counts are incremented per delivery, so a batch retried after a lost acknowledgement can
 * over-count; nodes and relationships themselves are never duplicated.
 *
 * <pre>
 * - name: graph
 *   type: neo4j
 *   uri: bolt://localhost:7687
 *   user: neo4j
 *   password: ${NEO4J_PASSWORD}
 *   userField: user
 *   ipField: src_ip
 *   hostField: host
 * </pre>
 */
public final class Neo4jSink implements Sink {

    public static final class Provider implements SinkProvider {
        @Override
        public String type() {
            return "neo4j";
        }

        @Override
        public Sink create(Props p, ComponentContext ctx) {
            Counter skipped = Counter.builder("conduit.neo4j.skipped")
                    .description("Events acknowledged without a graph write (not auth, or missing fields)")
                    .tag("sink", ctx.name()).register(ctx.meters());
            return new Neo4jSink(p.string("uri"), p.string("user", "neo4j"), p.string("password"),
                    p.string("database", "neo4j"), p.string("eventType", "auth"), p.string("userField", "user"),
                    p.string("ipField", "src_ip"), p.string("hostField", "host"), p.string("outcomeField", "outcome"),
                    p.duration("transactionTimeout", Duration.ofSeconds(15)), skipped);
        }
    }

    static final String UPSERT = """
            UNWIND $rows AS row
            MERGE (u:User {name: row.user})
            MERGE (ip:IP {address: row.ip})
            MERGE (h:Host {name: row.host})
            MERGE (u)-[a:AUTHENTICATED_FROM]->(ip)
              ON CREATE SET a.first_seen = row.ts, a.last_seen = row.ts, a.count = 0, a.failures = 0
            SET a.count = a.count + 1,
                a.failures = a.failures + row.failed,
                a.last_seen = CASE WHEN row.ts > a.last_seen THEN row.ts ELSE a.last_seen END
            MERGE (ip)-[c:ACCESSED]->(h)
              ON CREATE SET c.first_seen = row.ts, c.last_seen = row.ts, c.count = 0
            SET c.count = c.count + 1,
                c.last_seen = CASE WHEN row.ts > c.last_seen THEN row.ts ELSE c.last_seen END
            """;

    private static final List<String> CONSTRAINTS = List.of(
            "CREATE CONSTRAINT conduit_user IF NOT EXISTS FOR (u:User) REQUIRE u.name IS UNIQUE",
            "CREATE CONSTRAINT conduit_ip IF NOT EXISTS FOR (i:IP) REQUIRE i.address IS UNIQUE",
            "CREATE CONSTRAINT conduit_host IF NOT EXISTS FOR (h:Host) REQUIRE h.name IS UNIQUE");

    private final String uri;
    private final String user;
    private final String password;
    private final String database;
    private final String eventType;
    private final String userField;
    private final String ipField;
    private final String hostField;
    private final String outcomeField;
    private final TransactionConfig txConfig;
    private final Counter skipped;
    private Driver driver;

    Neo4jSink(String uri, String user, String password, String database, String eventType, String userField,
              String ipField, String hostField, String outcomeField, Duration txTimeout, Counter skipped) {
        this.uri = uri;
        this.user = user;
        this.password = password;
        this.database = database;
        this.eventType = eventType;
        this.userField = userField;
        this.ipField = ipField;
        this.hostField = hostField;
        this.outcomeField = outcomeField;
        this.txConfig = TransactionConfig.builder().withTimeout(txTimeout).build();
        this.skipped = skipped;
    }

    @Override
    public void open() {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password), Config.builder()
                .withConnectionTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
                .withMaxTransactionRetryTime(0, java.util.concurrent.TimeUnit.SECONDS)
                .withoutEncryption()
                .build());
        driver.verifyConnectivity();
        try (Session s = session()) {
            for (String c : CONSTRAINTS) {
                s.run(c).consume();
            }
        }
    }

    private Session session() {
        return driver.session(SessionConfig.forDatabase(database));
    }

    @Override
    public void write(List<Event> batch) {
        List<Map<String, Object>> rows = new ArrayList<>(batch.size());
        int skip = 0;
        for (Event e : batch) {
            Map<String, Object> row = toRow(e);
            if (row == null) {
                skip++;
            } else {
                rows.add(row);
            }
        }
        if (skip > 0) {
            skipped.increment(skip);
        }
        if (rows.isEmpty()) {
            return;
        }
        try (Session s = session()) {
            s.executeWrite(tx -> tx.run(UPSERT, Map.of("rows", rows)).consume(), txConfig);
        }
    }

    Map<String, Object> toRow(Event e) {
        if (!eventType.equals(e.type())) {
            return null;
        }
        Object u = e.attribute(userField);
        Object ip = e.attribute(ipField);
        Object host = e.attribute(hostField);
        if (u == null || ip == null || host == null) {
            return null;
        }
        Map<String, Object> row = new HashMap<>(8);
        row.put("user", u.toString());
        row.put("ip", ip.toString());
        row.put("host", host.toString());
        row.put("ts", e.timestamp());
        Object outcome = e.attribute(outcomeField);
        row.put("failed", outcome != null && !"success".equalsIgnoreCase(outcome.toString()) ? 1 : 0);
        return row;
    }

    /** Syntax and constraint errors are permanent; connection and transient errors are worth retrying. */
    @Override
    public boolean isRetryable(Exception error) {
        return !(error instanceof ClientException ce) || ce.code() == null
                || !ce.code().startsWith("Neo.ClientError.Statement");
    }

    @Override
    public void close() {
        if (driver != null) {
            driver.close();
        }
    }
}
