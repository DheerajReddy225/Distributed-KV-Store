package com.kvstore.controller;

import com.kvstore.config.NodeRegistry;
import com.kvstore.hashing.ConsistentHashRing;
import com.kvstore.service.RouterService;
import com.kvstore.service.StorageService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AdminController — cluster observability + benchmarking
 *
 * GET  /admin/status         → node health map
 * GET  /admin/ring/{key}     → which nodes own a given key
 * GET  /admin/keys           → all local keys
 * POST /admin/benchmark      → latency/throughput benchmark
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    @Value("${node.id:node-1}")
    private String selfId;

    private final NodeRegistry nodeRegistry;
    private final ConsistentHashRing hashRing;
    private final StorageService storageService;
    private final RouterService routerService;

    public AdminController(NodeRegistry nodeRegistry, ConsistentHashRing hashRing,
                           StorageService storageService, RouterService routerService) {
        this.nodeRegistry = nodeRegistry;
        this.hashRing = hashRing;
        this.storageService = storageService;
        this.routerService = routerService;
    }

    /** Cluster-wide health snapshot visible from this node. */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("selfId", selfId);
        result.put("localKeys", storageService.size());
        result.put("nodes", nodeRegistry.getHealthMap());
        return ResponseEntity.ok(result);
    }

    /** Which nodes (primary + replicas) are responsible for a given key? */
    @GetMapping("/ring/{key}")
    public ResponseEntity<Map<String, Object>> ringInfo(@PathVariable String key,
            @RequestParam(defaultValue = "2") int replicationFactor) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", key);
        result.put("primary", hashRing.getPrimaryNode(key));
        result.put("replicas", hashRing.getReplicaNodes(key, replicationFactor));
        return ResponseEntity.ok(result);
    }

    /** All keys stored on THIS node (useful for debugging ring distribution). */
    @GetMapping("/keys")
    public ResponseEntity<Map<String, String>> localKeys() {
        return ResponseEntity.ok(storageService.getAll());
    }

    /**
     * Benchmark: runs N concurrent PUT/GET operations and reports:
     *   - total duration
     *   - operations per second (throughput)
     *   - average latency per op
     *
     * Example: POST /admin/benchmark?ops=1000&threads=10
     */
    @PostMapping("/benchmark")
    public ResponseEntity<Map<String, Object>> benchmark(
            @RequestParam(defaultValue = "500")  int ops,
            @RequestParam(defaultValue = "10")   int threads) throws InterruptedException {

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(ops);
        AtomicLong totalLatencyNs = new AtomicLong(0);
        AtomicLong errors = new AtomicLong(0);

        long startMs = System.currentTimeMillis();

        for (int i = 0; i < ops; i++) {
            final int idx = i;
            pool.submit(() -> {
                long t0 = System.nanoTime();
                try {
                    String key = "bench-key-" + idx;
                    routerService.put(key, "value-" + idx);
                    routerService.get(key);
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    totalLatencyNs.addAndGet(System.nanoTime() - t0);
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        pool.shutdown();

        long durationMs = System.currentTimeMillis() - startMs;
        double throughput = (ops * 1000.0) / durationMs; // ops/sec
        double avgLatencyMs = (totalLatencyNs.get() / 1_000_000.0) / ops;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("operations", ops);
        result.put("threads", threads);
        result.put("durationMs", durationMs);
        result.put("throughputOpsPerSec", String.format("%.1f", throughput));
        result.put("avgLatencyMs", String.format("%.3f", avgLatencyMs));
        result.put("errors", errors.get());

        return ResponseEntity.ok(result);
    }
}
