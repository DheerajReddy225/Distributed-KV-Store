package com.kvstore.replication;

import com.kvstore.config.NodeRegistry;
import com.kvstore.hashing.ConsistentHashRing;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * ReplicationService
 *
 * After the primary node writes a key locally it calls this service to
 * forward the write to all replica nodes in parallel.
 *
 * Key design decisions:
 *  - Parallel fan-out via a thread pool (not sequential) keeps latency low.
 *  - We use "fire and forget" async replication here (AP system — available
 *    + partition tolerant). For strong consistency you would wait for a quorum
 *    of acknowledgements before returning to the client (CP system).
 *  - Replicas receive writes via a dedicated /internal/replicate endpoint
 *    so they apply the value directly without re-routing it.
 */
@Service
public class ReplicationService {

    private static final Logger log = Logger.getLogger(ReplicationService.class.getName());

    @Value("${replication.factor:2}")
    private int replicationFactor;

    @Value("${node.id:node-1}")
    private String selfId;

    private final ConsistentHashRing hashRing;
    private final NodeRegistry nodeRegistry;
    private final RestTemplate restTemplate = new RestTemplate();

    // Thread pool sized to the max number of concurrent replication fan-outs
    private final ExecutorService replicationPool =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    public ReplicationService(ConsistentHashRing hashRing, NodeRegistry nodeRegistry) {
        this.hashRing = hashRing;
        this.nodeRegistry = nodeRegistry;
    }

    /**
     * Replicate a PUT to all replica nodes (excluding self — already written).
     */
    public void replicatePut(String key, String value) {
        List<String> replicas = hashRing.getReplicaNodes(key, replicationFactor);
        for (String replicaId : replicas) {
            if (replicaId.equals(selfId)) continue;
            if (!nodeRegistry.isAlive(replicaId)) {
                log.warning("Skipping replication to dead node: " + replicaId);
                continue;
            }
            // Fire-and-forget — submit to thread pool, don't block the caller
            replicationPool.submit(() -> sendPut(replicaId, key, value));
        }
    }

    /**
     * Replicate a DELETE to all replica nodes.
     */
    public void replicateDelete(String key) {
        List<String> replicas = hashRing.getReplicaNodes(key, replicationFactor);
        for (String replicaId : replicas) {
            if (replicaId.equals(selfId)) continue;
            if (!nodeRegistry.isAlive(replicaId)) continue;
            replicationPool.submit(() -> sendDelete(replicaId, key));
        }
    }

    private void sendPut(String nodeId, String key, String value) {
        try {
            String url = nodeRegistry.getBaseUrl(nodeId) + "/internal/replicate/" + key;
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.TEXT_PLAIN);
            HttpEntity<String> entity = new HttpEntity<>(value, headers);
            restTemplate.exchange(url, HttpMethod.PUT, entity, Void.class);
            log.fine("Replicated PUT key=" + key + " to " + nodeId);
        } catch (Exception e) {
            log.warning("Replication PUT failed to " + nodeId + ": " + e.getMessage());
        }
    }

    private void sendDelete(String nodeId, String key) {
        try {
            String url = nodeRegistry.getBaseUrl(nodeId) + "/internal/replicate/" + key;
            restTemplate.delete(url);
            log.fine("Replicated DELETE key=" + key + " to " + nodeId);
        } catch (Exception e) {
            log.warning("Replication DELETE failed to " + nodeId + ": " + e.getMessage());
        }
    }
}
