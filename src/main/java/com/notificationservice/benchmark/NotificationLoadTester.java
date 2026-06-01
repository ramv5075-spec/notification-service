package com.notificationservice.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class NotificationLoadTester {

    private static final Logger log = LoggerFactory.getLogger(NotificationLoadTester.class);
    private static final String API_URL = "http://localhost:8080/api/notify";

    private final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .executor(Executors.newFixedThreadPool(100))
        .build();

    private final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        NotificationLoadTester tester = new NotificationLoadTester();

        System.out.println("\n╔══════════════════════════════════════════╗");
        System.out.println("║   Notification Service Load Test Results  ║");
        System.out.println("╚══════════════════════════════════════════╝\n");

        tester.warmUp();

        BenchmarkResult r1 = tester.runBenchmark("EMAIL burst",     500,  10);
        BenchmarkResult r2 = tester.runBenchmark("Mixed types",     1000, 20);
        BenchmarkResult r3 = tester.runBenchmark("High concurrency",500,  50);

        tester.printResult(r1);
        tester.printResult(r2);
        tester.printResult(r3);
        tester.printSummary(r1, r2, r3);
    }

    private void warmUp() throws Exception {
        log.info("Warming up...");
        for (int i = 0; i < 10; i++)
            sendMessage("EMAIL", "warmup@test.com", "warmup", "warmup");
        Thread.sleep(1000);
        log.info("Warm up complete");
    }

    public BenchmarkResult runBenchmark(String name, int totalOps, int threads) throws Exception {
        log.info("Running [{}]: {} ops, {} threads", name, totalOps, threads);

        ExecutorService pool  = Executors.newFixedThreadPool(threads);
        List<Long> latencies  = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failed  = new AtomicInteger(0);
        CountDownLatch latch  = new CountDownLatch(totalOps);

        String[] types      = {"EMAIL","SMS","PUSH","WEBHOOK"};
        String[] recipients = {"user@example.com","+19085551234",
                               "device-token-123","https://hook.example.com"};

        long start = System.currentTimeMillis();

        for (int i = 0; i < totalOps; i++) {
            final int idx = i;
            pool.submit(() -> {
                long t0 = System.nanoTime();
                try {
                    int status = sendMessage(
                        types[idx % 4], recipients[idx % 4],
                        "Subject " + idx, "Payload " + idx);
                    if (status == 200) success.incrementAndGet();
                    else               failed.incrementAndGet();
                } catch (Exception e) {
                    failed.incrementAndGet();
                } finally {
                    latencies.add((System.nanoTime() - t0) / 1_000_000);
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - start;
        pool.shutdown();

        Collections.sort(latencies);
        double throughput = (success.get() * 1000.0) / elapsed;
        long avg = (long) latencies.stream().mapToLong(l -> l).average().orElse(0);
        long p50 = percentile(latencies, 50);
        long p95 = percentile(latencies, 95);
        long p99 = percentile(latencies, 99);

        return new BenchmarkResult(name, totalOps, threads, elapsed,
            throughput, avg, p50, p95, p99, success.get(), failed.get());
    }

    private int sendMessage(String type, String recipient,
                             String subject, String payload) throws Exception {
        Map<String, String> body = Map.of(
            "type", type, "recipient", recipient,
            "subject", subject, "payload", payload);
        String json = mapper.writeValueAsString(body);

        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(API_URL))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .timeout(Duration.ofSeconds(30))
            .build();

        return client.send(req, HttpResponse.BodyHandlers.ofString()).statusCode();
    }

    private long percentile(List<Long> sorted, int p) {
        if (sorted.isEmpty()) return 0;
        int idx = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, idx));
    }

    private void printResult(BenchmarkResult r) {
        System.out.printf("┌──────────────────────────────────────────┐%n");
        System.out.printf("│ %-40s │%n", r.name);
        System.out.printf("├──────────────────────────────────────────┤%n");
        System.out.printf("│ Throughput:   %8.0f msg/sec            │%n", r.throughput);
        System.out.printf("│ Avg latency:  %8d ms                │%n", r.avgMs);
        System.out.printf("│ p50 latency:  %8d ms                │%n", r.p50Ms);
        System.out.printf("│ p95 latency:  %8d ms                │%n", r.p95Ms);
        System.out.printf("│ p99 latency:  %8d ms                │%n", r.p99Ms);
        System.out.printf("│ Success:      %8d / %-8d        │%n", r.success, r.totalOps);
        System.out.printf("│ Duration:     %8d ms                │%n", r.elapsedMs);
        System.out.printf("└──────────────────────────────────────────┘%n%n");
    }

    private void printSummary(BenchmarkResult... results) {
        double maxThroughput = Arrays.stream(results)
            .mapToDouble(r -> r.throughput).max().orElse(0);
        long minP99 = Arrays.stream(results)
            .mapToLong(r -> r.p99Ms).min().orElse(0);

        System.out.println("╔══════════════════════════════════════════╗");
        System.out.println("║           RESUME NUMBERS                 ║");
        System.out.println("╠══════════════════════════════════════════╣");
        System.out.printf( "║  Peak throughput:  %6.0f msg/sec         ║%n", maxThroughput);
        System.out.printf( "║  Best p99 latency: %6d ms              ║%n", minP99);
        System.out.println("╚══════════════════════════════════════════╝");
    }

    public record BenchmarkResult(
        String name, int totalOps, int threads, long elapsedMs,
        double throughput, long avgMs, long p50Ms, long p95Ms, long p99Ms,
        int success, int failed) {}
}
