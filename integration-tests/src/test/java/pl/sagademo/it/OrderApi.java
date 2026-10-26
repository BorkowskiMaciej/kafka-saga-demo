package pl.sagademo.it;

import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collection;
import java.util.UUID;
import org.awaitility.Awaitility;

public final class OrderApi {

    public record OrderView(UUID id, String status, String cancellationReason) {
    }

    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final String baseUrl;

    public OrderApi(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public UUID create(String customerId, String productId, int quantity, String unitPrice) {
        String body = "{\"customerId\":\"%s\",\"items\":[{\"productId\":\"%s\",\"quantity\":%d,\"unitPrice\":%s}]}"
                .formatted(customerId, productId, quantity, unitPrice);
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/orders"))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = send(request);
        if (response.statusCode() != 201) {
            throw new AssertionError("POST /orders returned " + response.statusCode() + ": " + response.body());
        }
        String id = JsonPath.read(response.body(), "$.id");
        return UUID.fromString(id);
    }

    public OrderView view(UUID id) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/orders/" + id))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build();
        HttpResponse<String> response = send(request);
        if (response.statusCode() != 200) {
            throw new AssertionError("GET /orders/" + id + " returned " + response.statusCode());
        }
        String status = JsonPath.read(response.body(), "$.status");
        String reason = JsonPath.read(response.body(), "$.cancellationReason");
        return new OrderView(id, status, reason);
    }

    public OrderView awaitStatus(UUID id, String status, Duration timeout) {
        Awaitility.await("order " + id + " -> " + status)
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(100))
                .until(() -> view(id).status().equals(status));
        return view(id);
    }

    public void awaitAll(Collection<UUID> ids, String status, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        for (UUID id : ids) {
            Duration remaining = Duration.ofNanos(Math.max(deadline - System.nanoTime(), 1_000_000_000L));
            awaitStatus(id, status, remaining);
        }
    }

    private HttpResponse<String> send(HttpRequest request) {
        try {
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IllegalStateException("HTTP call failed: " + request.uri(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted", e);
        }
    }
}
