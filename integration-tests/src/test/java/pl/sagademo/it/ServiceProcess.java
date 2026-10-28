package pl.sagademo.it;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class ServiceProcess {

    private final String name;
    private final Path jar;
    private final int port;
    private final Map<String, String> environment;
    private final Path workDirectory;
    private final Path logFile;
    private final String heap;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    private Process process;

    public ServiceProcess(
            String name,
            Path jar,
            int port,
            Map<String, String> environment,
            Path workDirectory,
            Path logFile,
            String heap) {
        this.name = name;
        this.jar = jar;
        this.port = port;
        this.environment = environment;
        this.workDirectory = workDirectory;
        this.logFile = logFile;
        this.heap = heap;
    }

    public String name() {
        return name;
    }

    public int port() {
        return port;
    }

    public String baseUrl() {
        return "http://localhost:" + port;
    }

    public synchronized void start() {
        if (process != null && process.isAlive()) {
            throw new IllegalStateException(name + " is already running");
        }
        try {
            Files.createDirectories(workDirectory);
            Files.createDirectories(logFile.getParent());
            Path java = Path.of(System.getProperty("java.home"), "bin", "java");
            List<String> command = List.of(
                    java.toString(), "-Xmx" + heap, "-jar", jar.toString(), "--server.port=" + port);
            ProcessBuilder builder = new ProcessBuilder(command)
                    .directory(workDirectory.toFile())
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile.toFile()));
            builder.environment().putAll(environment);
            process = builder.start();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot start " + name, e);
        }
    }

    public void awaitReady(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (!isAlive()) {
                throw new IllegalStateException(name + " exited during startup. Log tail:\n" + logTail());
            }
            if (isHealthy()) {
                return;
            }
            sleep(500);
        }
        throw new IllegalStateException(name + " not ready after " + timeout + ". Log tail:\n" + logTail());
    }

    public synchronized boolean isAlive() {
        return process != null && process.isAlive();
    }

    public synchronized void stop() {
        if (process == null) {
            return;
        }
        process.destroy();
        try {
            if (!process.waitFor(30, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }

    public boolean logContains(String... fragments) {
        try {
            String log = Files.readString(logFile);
            for (String fragment : fragments) {
                if (!log.contains(fragment)) {
                    return false;
                }
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private boolean isHealthy() {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl() + "/actuator/health"))
                .timeout(Duration.ofSeconds(2))
                .GET()
                .build();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200 && response.body().contains("\"status\":\"UP\"");
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private String logTail() {
        try {
            List<String> lines = new ArrayList<>(Files.readAllLines(logFile));
            int from = Math.max(0, lines.size() - 40);
            return String.join("\n", lines.subList(from, lines.size()));
        } catch (IOException e) {
            return "(log unavailable: " + e.getMessage() + ")";
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted", e);
        }
    }
}
