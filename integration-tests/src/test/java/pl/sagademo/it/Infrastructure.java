package pl.sagademo.it;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import pl.sagademo.events.Topics;

public final class Infrastructure {

    public static final String POSTGRES_USER = "saga";
    public static final String POSTGRES_PASSWORD = "saga";

    private static final List<String> DATABASES = List.of("order_db", "payment_db", "inventory_db");

    private final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.0"));
    private final PostgreSQLContainer postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:17-alpine"))
            .withUsername(POSTGRES_USER)
            .withPassword(POSTGRES_PASSWORD)
            .withDatabaseName("postgres");

    public void start() {
        kafka.start();
        postgres.start();
        createDatabases();
        createTopics();
    }

    public String bootstrapServers() {
        return kafka.getBootstrapServers();
    }

    public Database database() {
        return new Database(postgres.getHost(), postgres.getMappedPort(5432), POSTGRES_USER, POSTGRES_PASSWORD);
    }

    private void createDatabases() {
        try (Connection connection =
                        DriverManager.getConnection(postgres.getJdbcUrl(), POSTGRES_USER, POSTGRES_PASSWORD);
                Statement statement = connection.createStatement()) {
            for (String database : DATABASES) {
                statement.execute("CREATE DATABASE " + database);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot create databases", e);
        }
    }

    private void createTopics() {
        List<NewTopic> topics = Topics.ALL.stream()
                .map(name -> new NewTopic(name, Topics.PARTITIONS, (short) 1))
                .toList();
        try (Admin admin = Admin.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()))) {
            admin.createTopics(topics).all().get();
        } catch (ExecutionException e) {
            throw new IllegalStateException("Cannot create topics", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while creating topics", e);
        }
    }
}
