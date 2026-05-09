package io.conduit.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.conduit.pipeline.Pipeline;
import io.conduit.pipeline.ShutdownReport;
import io.conduit.testkit.TestPipelines;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Real PostgreSQL and Neo4j in containers, a fixed event set through the whole pipeline, then
 * assertions on exactly what landed in each store. Skipped when Docker is not available.
 */
@Testcontainers(disabledWithoutDocker = true)
class DatabaseSinksE2ETest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static final Neo4jContainer<?> NEO4J = new Neo4jContainer<>("neo4j:5-community").withAdminPassword("conduit-test");

    @TempDir
    Path tmp;

    @BeforeEach
    void clean() throws Exception {
        try (Connection c = pg(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS events");
        }
        try (Driver d = neo(); Session s = d.session()) {
            s.run("MATCH (n) DETACH DELETE n").consume();
        }
    }

    @AfterEach
    void reset() {
        TestPipelines.resetAll();
    }

    private static final String EVENTS = """
            {"id":"a1","type":"auth","ts":"2026-10-25T09:00:00-04:00","user":"alice","email":"alice@contoso.com","src_ip":"185.220.101.7","host":"vpn-1","outcome":"failure","severity":"high"}
            {"id":"a2","type":"auth","ts":"2026-10-25T13:05:00Z","user":"alice","src_ip":"185.220.101.7","host":"vpn-1","outcome":"success"}
            {"id":"a3","type":"auth","ts":"2026-10-25T13:06:00Z","user":"alice","src_ip":"10.0.4.22","host":"mail-1","outcome":"success"}
            {"id":"a4","type":"auth","ts":"2026-10-25T13:07:00Z","user":"bob","src_ip":"10.0.4.22","host":"mail-1","outcome":"success"}
            {"id":"p1","type":"process","user":"svc","host":"db-1","cmdline":"backup --notify ops@contoso.com","severity":"critical"}
            {"id":"n1","type":"network","src_ip":"10.0.9.9","dst_ip":"20.42.1.1","severity":"low"}
            {"id":"h1","type":"heartbeat","host":"db-1"}
            {"type":"auth","user":"broken"
            """;

    @Test
    void fixedEventSetLandsInPostgresAndGrowsTheGraph() throws Exception {
        Path input = tmp.resolve("events.jsonl");
        Files.writeString(input, EVENTS);
        Path table = tmp.resolve("ip.csv");
        Files.writeString(table, "cidr,zone,country\n10.0.0.0/8,internal,\n185.220.100.0/22,tor,DE\n");

        ShutdownReport r = TestPipelines.runClean(config(input, table), tmp);

        assertThat(r.accounting().ingested()).isEqualTo(8);
        assertThat(r.accounting().parseFailed()).isEqualTo(1);
        assertThat(r.accounting().filtered()).isEqualTo(1);
        assertThat(r.accounting().routed()).isEqualTo(6);

        try (Connection c = pg(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT id, ts, attributes::text FROM events ORDER BY id");
            List<String> ids = new java.util.ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getString(1));
                assertThat(rs.getString(3)).doesNotContain("contoso.com");
                if (rs.getString(1).equals("a1")) {
                    assertThat(rs.getObject(2, java.time.OffsetDateTime.class).toInstant())
                            .isEqualTo(java.time.Instant.parse("2026-10-25T13:00:00Z"));
                    assertThat(rs.getString(3)).contains("\"zone\": \"tor\"").contains("redacted:");
                }
            }
            assertThat(ids).containsExactly("a1", "a2", "a3", "a4", "n1", "p1");
        }

        try (Driver d = neo(); Session s = d.session()) {
            Map<String, Object> counts = s.run("""
                    MATCH (u:User) WITH count(u) AS users
                    MATCH (i:IP) WITH users, count(i) AS ips
                    MATCH (h:Host) WITH users, ips, count(h) AS hosts
                    MATCH ()-[a:AUTHENTICATED_FROM]->() WITH users, ips, hosts, count(a) AS auths
                    MATCH ()-[x:ACCESSED]->()
                    RETURN users, ips, hosts, auths, count(x) AS accessed""").single().asMap();
            assertThat(counts).containsEntry("users", 2L).containsEntry("ips", 2L).containsEntry("hosts", 2L)
                    .containsEntry("auths", 3L).containsEntry("accessed", 2L);

            Record alice = s.run("""
                    MATCH (:User {name:'alice'})-[a:AUTHENTICATED_FROM]->(:IP {address:'185.220.101.7'})
                    RETURN a.count AS count, a.failures AS failures""").single();
            assertThat(alice.get("count").asLong()).isEqualTo(2);
            assertThat(alice.get("failures").asLong()).isEqualTo(1);

            List<String> sharedIp = s.run("""
                    MATCH (u:User)-[:AUTHENTICATED_FROM]->(:IP {address:'10.0.4.22'})
                    RETURN u.name AS name ORDER BY name""").list(rec -> rec.get("name").asString());
            assertThat(sharedIp).containsExactly("alice", "bob");
        }
    }

    @Test
    void replayingTheSameEventsDoesNotDuplicateRowsOrNodes() throws Exception {
        Path input = tmp.resolve("events.jsonl");
        Files.writeString(input, EVENTS);
        Path table = tmp.resolve("ip.csv");
        Files.writeString(table, "cidr,zone\n10.0.0.0/8,internal\n");

        TestPipelines.runClean(config(input, table), tmp);
        TestPipelines.runClean(config(input, table), tmp);

        try (Connection c = pg(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT count(*) FROM events");
            rs.next();
            assertThat(rs.getLong(1)).as("ON CONFLICT DO NOTHING makes retries idempotent").isEqualTo(6);
        }
        try (Driver d = neo(); Session s = d.session()) {
            assertThat(s.run("MATCH (u:User) RETURN count(u) AS n").single().get("n").asLong()).isEqualTo(2);
        }
    }

    @Test
    void neo4jGoingDownMidRunDoesNotStopPostgres() throws Exception {
        Pipeline p = TestPipelines.create("""
                pipeline: { workers: 4, shutdownTimeout: 30s }
                sources:
                  - { name: synth, type: synthetic, rate: 4000, count: 24000, seed: 42, mix: { auth: 1.0, network: 0, process: 0, heartbeat: 0 } }
                routes:
                  rules:
                    - { name: all, when: { type: auth }, to: [postgres, graph] }
                  default: [postgres]
                sinks:
                  - { name: postgres, type: postgres, url: '%s', user: '%s', password: '%s', batchSize: 200 }
                  - name: graph
                    type: neo4j
                    uri: '%s'
                    password: conduit-test
                    batchSize: 200
                    writeTimeout: 2s
                    offerTimeout: 100ms
                    retry: { maxAttempts: 2, initialBackoff: 50ms, maxBackoff: 200ms }
                    circuitBreaker: { failureThreshold: 2, openFor: 1s }
                """.formatted(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword(),
                NEO4J.getBoltUrl()), tmp);
        p.start();
        Thread.sleep(2000);
        NEO4J.getDockerClient().pauseContainerCmd(NEO4J.getContainerId()).exec();
        try {
            Thread.sleep(2500);
        } finally {
            NEO4J.getDockerClient().unpauseContainerCmd(NEO4J.getContainerId()).exec();
        }
        ShutdownReport r = p.awaitCompletion();

        assertThat(r.ok()).as(r.summary()).isTrue();
        try (Connection c = pg(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT count(*) FROM events");
            rs.next();
            assertThat(rs.getLong(1)).as("every event reached postgres").isEqualTo(24_000);
        }
        assertThat(r.accounting().sinkFailed() + r.accounting().sinkOverflow())
                .as("graph deliveries during the pause were dead-lettered").isPositive();
    }

    private static String config(Path input, Path table) {
        return """
                pipeline: { workers: 3 }
                sources:
                  - { name: file, type: file, path: '%s', exitAtEof: true, keyField: user }
                filter:
                  drop: { type: heartbeat }
                transforms:
                  - { type: timestamp }
                  - { type: redact, fields: [email], salt: e2e }
                  - { type: enrich-ip, table: '%s', reloadInterval: 0ms }
                routes:
                  rules:
                    - { name: auth, when: { type: auth }, to: [graph, postgres] }
                  default: [postgres]
                sinks:
                  - { name: postgres, type: postgres, url: '%s', user: '%s', password: '%s', workers: 2 }
                  - { name: graph, type: neo4j, uri: '%s', password: conduit-test }
                """.formatted(input, table, POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword(),
                NEO4J.getBoltUrl());
    }

    private static Connection pg() throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static Driver neo() {
        return GraphDatabase.driver(NEO4J.getBoltUrl(), AuthTokens.basic("neo4j", "conduit-test"));
    }
}
