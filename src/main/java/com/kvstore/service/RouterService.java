package com.kvstore.service;

import com.kvstore.config.NodeRegistry;
import com.kvstore.hashing.ConsistentHashRing;
import com.kvstore.raft.RaftConsensus;
import com.kvstore.replication.ReplicationService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;
import java.util.logging.Logger;

/**
 * RouterService — updated to route all writes through the Raft leader.
 *
 * Reads  → still go to any node that has the key (low latency)
 * Writes → always forwarded to the LEADER who applies write quorum
 */
@Service
public class RouterService {

    private static final Logger log = Logger.getLogger(RouterService.class.getName());

    @Value("${node.id:node-1}")
    private String selfId;

    private final ConsistentHashRing  hashRing;
    private final NodeRegistry        nodeRegistry;
    private final StorageService      storageService;
    private final ReplicationService  replicationService;
    private final RaftConsensus       raftConsensus;
    private final RestTemplate        restTemplate = new RestTemplate();

    public RouterService(ConsistentHashRing hashRing,
                         NodeRegistry nodeRegistry,
                         StorageService storageService,
                         ReplicationService replicationService,
                         RaftConsensus raftConsensus) {
        this.hashRing          = hashRing;
        this.nodeRegistry      = nodeRegistry;
        this.storageService    = storageService;
        this.replicationService = replicationService;
        this.raftConsensus     = raftConsensus;
    }

    /**
     * GET — reads still go to any node holding the key.
     * Eventual consistency on reads keeps latency low.
     */
    public Optional<String> get(String key) {
        Optional<String> local = storageService.get(key);
        if (local.isPresent()) return local;

        String primary = hashRing.getPrimaryNode(key);
        if (primary.equals(selfId)) return Optional.empty();

        try {
            String url = nodeRegistry.getBaseUrl(primary) + "/kv/" + key;
            ResponseEntity<String> resp =
                    restTemplate.getForEntity(url, String.class);
            return resp.getStatusCode().is2xxSuccessful()
                    ? Optional.ofNullable(resp.getBody())
                    : Optional.empty();
        } catch (Exception e) {
            log.warning("Forward GET failed to " + primary + ": " + e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * PUT — all writes go through the Raft leader.
     * If this node is the leader → write locally + quorum replication.
     * If this node is a follower → forward to the leader.
     */
    public void put(String key, String value) {
        if (raftConsensus.isLeader()) {
            // I am the leader — write locally and replicate with quorum
            storageService.put(key, value);
            replicationService.replicatePutWithQuorum(key, value);
        } else {
            // Forward to the current leader
            String leaderId = raftConsensus.getCurrentLeaderId();
            if (leaderId == null) {
                throw new RuntimeException(
                        "No leader elected yet — cluster is stabilising, please retry");
            }
            forwardPut(leaderId, key, value);
        }
    }

    /**
     * DELETE — same pattern as PUT, routes through the leader.
     */
    public boolean delete(String key) {
        if (raftConsensus.isLeader()) {
            boolean deleted = storageService.delete(key);
            if (deleted) replicationService.replicateDeleteWithQuorum(key);
            return deleted;
        } else {
            String leaderId = raftConsensus.getCurrentLeaderId();
            if (leaderId == null) return false;
            return forwardDelete(leaderId, key);
        }
    }

    // ── Forwarding helpers ──────────────────────────────────────────────────

    private void forwardPut(String nodeId, String key, String value) {
        try {
            String url = nodeRegistry.getBaseUrl(nodeId) + "/kv/" + key;
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.TEXT_PLAIN);
            restTemplate.exchange(url, HttpMethod.PUT,
                    new HttpEntity<>(value, headers), Void.class);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Forward PUT to leader " + nodeId + " failed: " + e.getMessage(), e);
        }
    }

    private boolean forwardDelete(String nodeId, String key) {
        try {
            String url = nodeRegistry.getBaseUrl(nodeId) + "/kv/" + key;
            restTemplate.delete(url);
            return true;
        } catch (Exception e) {
            log.warning("Forward DELETE to leader " + nodeId + " failed: "
                    + e.getMessage());
            return false;
        }
    }
}