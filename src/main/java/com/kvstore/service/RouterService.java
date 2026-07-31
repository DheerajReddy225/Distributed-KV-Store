package com.kvstore.service;

import com.kvstore.config.NodeRegistry;
import com.kvstore.hashing.ConsistentHashRing;
import com.kvstore.replication.ReplicationService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;
import java.util.logging.Logger;

/**
 * RouterService
 *
 * Sits in front of the local StorageService and handles two cases:
 *
 *  Case A — This node IS the primary for the key:
 *    Write locally, then fan-out replication to replicas.
 *
 *  Case B — This node is NOT the primary:
 *    Forward the request to the correct primary node and return its response.
 *    This gives clients a single-node API — they can talk to ANY node and
 *    always get the right answer (transparent routing).
 */
@Service
public class RouterService {

    private static final Logger log = Logger.getLogger(RouterService.class.getName());

    @Value("${node.id:node-1}")
    private String selfId;

    private final ConsistentHashRing hashRing;
    private final NodeRegistry nodeRegistry;
    private final StorageService storageService;
    private final ReplicationService replicationService;
    private final RestTemplate restTemplate = new RestTemplate();

    public RouterService(ConsistentHashRing hashRing, NodeRegistry nodeRegistry,
                         StorageService storageService, ReplicationService replicationService) {
        this.hashRing = hashRing;
        this.nodeRegistry = nodeRegistry;
        this.storageService = storageService;
        this.replicationService = replicationService;
    }

    public Optional<String> get(String key) {
        // For reads, check locally first (we may be a replica holding the value)
        Optional<String> local = storageService.get(key);
        if (local.isPresent()) return local;

        // If not found locally, forward to primary
        String primary = hashRing.getPrimaryNode(key);
        if (primary.equals(selfId)) return Optional.empty();

        try {
            String url = nodeRegistry.getBaseUrl(primary) + "/kv/" + key;
            ResponseEntity<String> resp = restTemplate.getForEntity(url, String.class);
            return resp.getStatusCode().is2xxSuccessful()
                    ? Optional.ofNullable(resp.getBody())
                    : Optional.empty();
        } catch (Exception e) {
            log.warning("Forward GET failed to " + primary + ": " + e.getMessage());
            return Optional.empty();
        }
    }

    public void put(String key, String value) {
        String primary = hashRing.getPrimaryNode(key);

        if (primary.equals(selfId)) {
            // We own this key — write locally and replicate
            storageService.put(key, value);
            replicationService.replicatePut(key, value);
        } else {
            // Forward to the correct primary
            forwardPut(primary, key, value);
        }
    }

    public boolean delete(String key) {
        String primary = hashRing.getPrimaryNode(key);

        if (primary.equals(selfId)) {
            boolean deleted = storageService.delete(key);
            if (deleted) replicationService.replicateDelete(key);
            return deleted;
        } else {
            return forwardDelete(primary, key);
        }
    }

    private void forwardPut(String nodeId, String key, String value) {
        try {
            String url = nodeRegistry.getBaseUrl(nodeId) + "/kv/" + key;
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.TEXT_PLAIN);
            restTemplate.exchange(url, HttpMethod.PUT, new HttpEntity<>(value, headers), Void.class);
        } catch (Exception e) {
            throw new RuntimeException("Forward PUT to " + nodeId + " failed: " + e.getMessage(), e);
        }
    }

    private boolean forwardDelete(String nodeId, String key) {
        try {
            String url = nodeRegistry.getBaseUrl(nodeId) + "/kv/" + key;
            restTemplate.delete(url);
            return true;
        } catch (Exception e) {
            log.warning("Forward DELETE to " + nodeId + " failed: " + e.getMessage());
            return false;
        }
    }
}
