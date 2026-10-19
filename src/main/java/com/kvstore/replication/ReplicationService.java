package com.kvstore.replication;

import com.kvstore.config.NodeRegistry;
import com.kvstore.hashing.ConsistentHashRing;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * ReplicationService — updated with quorum replication.
 *
 * replicatePutWithQuorum  — fans out to all nodes in parallel,
 *                           waits for majority ACK before returning.
 * replicateDeleteWithQuorum — same pattern for deletes.
 *
 * The old async fire-and-forget methods are kept for the benchmark
 * endpoint which still uses the original replication path.
 */
@Service
public class ReplicationService {

    private static final Logger log =
            Logger.getLogger(ReplicationService.class.getName());

    @Value("${node.id:node-1}")
    private String selfId;

    private final ConsistentHashRing hashRing;
    private final NodeRegistry       nodeRegistry;
    private final RestTemplate       restTemplate = new RestTemplate();

    private final ExecutorService replicationPool =
            Executors.newFixedThreadPool(
                    Runtime.getRuntime().availableProcessors() * 2);

    public ReplicationService(ConsistentHashRing hashRing,
                               NodeRegistry nodeRegistry) {
        this.hashRing     = hashRing;
        this.nodeRegistry = nodeRegistry;
    }

    // ── Quorum replication (used by leader via RouterService) ───────────────

    /**
     * Fan out PUT to ALL live nodes in parallel.
     * Block until majority have acknowledged.
     * Throws if quorum cannot be achieved.
     */
    public void replicatePutWithQuorum(String key, String value) {
        List<String> allNodes  = new ArrayList<>(hashRing.getAllNodes());
        int majority           = allNodes.size() / 2 + 1;
        // Leader already wrote locally — count as 1 acknowledgement
        AtomicInteger acknowledged = new AtomicInteger(1);

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String nodeId : allNodes) {
            if (nodeId.equals(selfId)) continue;
            if (!nodeRegistry.isAlive(nodeId)) continue;

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    sendPut(nodeId, key, value);
                    int acks = acknowledged.incrementAndGet();
                    log.info("Quorum PUT ack from " + nodeId
                            + " — " + acks + "/" + allNodes.size());
                } catch (Exception e) {
                    log.warning("Quorum PUT failed to " + nodeId
                            + ": " + e.getMessage());
                }
            }, replicationPool);

            futures.add(future);
        }

        // Wait up to 2 seconds for all futures, then check quorum
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(2, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.warning("Quorum replication timed out for key: " + key);
        } catch (Exception e) {
            log.warning("Quorum replication error: " + e.getMessage());
        }

        if (acknowledged.get() < majority) {
            throw new RuntimeException(
                    "Failed to achieve write quorum for key: " + key
                    + " — only " + acknowledged.get()
                    + "/" + allNodes.size() + " nodes acknowledged");
        }

        log.info("Write quorum achieved for key: " + key
                + " (" + acknowledged.get() + "/" + allNodes.size() + ")");
    }

    /**
     * Fan out DELETE to ALL live nodes in parallel.
     * Block until majority have acknowledged.
     */
    public void replicateDeleteWithQuorum(String key) {
        List<String> allNodes  = new ArrayList<>(hashRing.getAllNodes());
        int majority           = allNodes.size() / 2 + 1;
        AtomicInteger acknowledged = new AtomicInteger(1);

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String nodeId : allNodes) {
            if (nodeId.equals(selfId)) continue;
            if (!nodeRegistry.isAlive(nodeId)) continue;

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    sendDelete(nodeId, key);
                    acknowledged.incrementAndGet();
                } catch (Exception e) {
                    log.warning("Quorum DELETE failed to " + nodeId);
                }
            }, replicationPool);

            futures.add(future);
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warning("Quorum delete replication error: " + e.getMessage());
        }

        if (acknowledged.get() < majority) {
            throw new RuntimeException(
                    "Failed to achieve delete quorum for key: " + key);
        }
    }

    // ── Original async replication (kept for benchmark compatibility) ────────

    public void replicatePut(String key, String value) {
        List<String> replicas = hashRing.getReplicaNodes(key, 2);
        for (String replicaId : replicas) {
            if (replicaId.equals(selfId)) continue;
            if (!nodeRegistry.isAlive(replicaId)) continue;
            replicationPool.submit(() -> sendPut(replicaId, key, value));
        }
    }

    public void replicateDelete(String key) {
        List<String> replicas = hashRing.getReplicaNodes(key, 2);
        for (String replicaId : replicas) {
            if (replicaId.equals(selfId)) continue;
            if (!nodeRegistry.isAlive(replicaId)) continue;
            replicationPool.submit(() -> sendDelete(replicaId, key));
        }
    }

    // ── HTTP helpers ─────────────────────────────────────────────────────────

    private void sendPut(String nodeId, String key, String value) {
        String url = nodeRegistry.getBaseUrl(nodeId)
                + "/internal/replicate/" + key;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        restTemplate.exchange(url, HttpMethod.PUT,
                new HttpEntity<>(value, headers), Void.class);
    }

    private void sendDelete(String nodeId, String key) {
        String url = nodeRegistry.getBaseUrl(nodeId)
                + "/internal/replicate/" + key;
        restTemplate.delete(url);
    }

    // ── Field needed by RouterService ────────────────────────────────────────
    private final AtomicInteger dummyRef = new AtomicInteger(0);
}