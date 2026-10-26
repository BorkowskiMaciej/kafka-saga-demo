package pl.sagademo.it;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class SagaEnvironment {

    private static final Duration READY_TIMEOUT = Duration.ofSeconds(120);

    private static SagaEnvironment instance;

    private final Infrastructure infrastructure = new Infrastructure();
    private final Path modulesDirectory = Path.of(System.getProperty("saga.modules.dir", "..")).toAbsolutePath().normalize();
    private final String version = System.getProperty("saga.version", "0.1.0-SNAPSHOT");
    private final String heap = System.getProperty("saga.service.heap", "384m");
    private final Path workDirectory = Path.of("target", "it-work").toAbsolutePath();
    private final Path logDirectory = Path.of("target", "it-logs").toAbsolutePath();
    private final List<ServiceProcess> processes = new ArrayList<>();

    private final Database database;
    private final KafkaProbe kafka;
    private final ServiceProcess orderService;
    private final ServiceProcess paymentService;
    private final ServiceProcess inventoryService;
    private final OrderApi orders;

    private int extraInstances;

    public static synchronized SagaEnvironment get() {
        if (instance == null) {
            instance = new SagaEnvironment();
        }
        return instance;
    }

    private SagaEnvironment() {
        infrastructure.start();
        database = infrastructure.database();
        kafka = new KafkaProbe(infrastructure.bootstrapServers());
        orderService = register(newProcess("order-service", "ORDER_DB_URL", "order_db"));
        paymentService = register(newProcess("payment-service", "PAYMENT_DB_URL", "payment_db"));
        inventoryService = register(newProcess("inventory-service", "INVENTORY_DB_URL", "inventory_db"));
        orders = new OrderApi(orderService.baseUrl());
        Runtime.getRuntime().addShutdownHook(new Thread(this::stopAll));
        orderService.start();
        paymentService.start();
        inventoryService.start();
        orderService.awaitReady(READY_TIMEOUT);
        paymentService.awaitReady(READY_TIMEOUT);
        inventoryService.awaitReady(READY_TIMEOUT);
    }

    public Database database() {
        return database;
    }

    public KafkaProbe kafka() {
        return kafka;
    }

    public OrderApi orders() {
        return orders;
    }

    public ServiceProcess orderService() {
        return orderService;
    }

    public ServiceProcess paymentService() {
        return paymentService;
    }

    public ServiceProcess inventoryService() {
        return inventoryService;
    }

    public synchronized ServiceProcess startPaymentInstance() {
        extraInstances++;
        ServiceProcess extra = register(newProcess(
                "payment-service-" + (extraInstances + 1), "PAYMENT_DB_URL", "payment_db", "payment-service"));
        extra.start();
        extra.awaitReady(READY_TIMEOUT);
        return extra;
    }

    private ServiceProcess newProcess(String name, String databaseVariable, String databaseName) {
        return newProcess(name, databaseVariable, databaseName, name);
    }

    private ServiceProcess newProcess(String name, String databaseVariable, String databaseName, String module) {
        Map<String, String> environment = new LinkedHashMap<>();
        environment.put("POSTGRES_USER", Infrastructure.POSTGRES_USER);
        environment.put("POSTGRES_PASSWORD", Infrastructure.POSTGRES_PASSWORD);
        environment.put(databaseVariable, database.jdbcUrl(databaseName));
        environment.put("KAFKA_BOOTSTRAP_SERVERS", kafka.bootstrapServers());
        Path jar = modulesDirectory.resolve(module).resolve("target").resolve(module + "-" + version + ".jar");
        if (!Files.isRegularFile(jar)) {
            throw new IllegalStateException(
                    "Missing " + jar + ". Build the services first: mvn -DskipTests package");
        }
        Path logFile = logDirectory.resolve(name + ".log");
        return new ServiceProcess(name, jar, freePort(), environment, workDirectory, logFile, heap);
    }

    private ServiceProcess register(ServiceProcess process) {
        synchronized (processes) {
            processes.add(process);
        }
        return process;
    }

    private void stopAll() {
        synchronized (processes) {
            for (ServiceProcess process : processes) {
                process.stop();
            }
        }
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("No free port", e);
        }
    }
}
