package com.kvstore.config;

import com.kvstore.hashing.ConsistentHashRing;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * NodeRegistry
 *
 * Keeps track of every node in the cluster:
 *   - parses the NODE_PEERS environment variable on startup
 *   - maintains a live/dead map updated by the health-check scheduler
 *   - adds/removes nodes from the consistent hash ring accordingly
 */
@Component
public class NodeRegistry {

    private static final Logger log = Logger.getLogger(NodeRegistry.class.getName());

    @Value("${node.id:node-1}")
    private String selfId;

    @Value("${node.peers:node-1:8080,node-2:8081,node-3:8082}")
    private String peersConfig;

    private final ConsistentHashRing hashRing;
    private final RestTemplate restTemplate = new RestTemplate();

    // nodeId -> "host:port"
    private final Map<String, String> nodeAddresses = new ConcurrentHashMap<>();

    // nodeId -> alive?
    private final Map<String, Boolean> nodeHealth = new ConcurrentHashMap<>();

    public NodeRegistry(ConsistentHashRing hashRing) {
        this.hashRing = hashRing;
    }

    @PostConstruct
    public void init() {
        // Parse "node-1:8080,node-2:8081,node-3:8082"
        // node ID = first segment, port = second; host = node ID (Docker service name)
        List<String> peers = Arrays.asList(peersConfig.split(","));
        for (String peer : peers) {
            String[] parts = peer.trim().split(":");
            if (parts.length == 2) {
                String nodeId = parts[0];
                String address = parts[0] + ":" + parts[1]; // host:port
                nodeAddresses.put(nodeId, address);
                nodeHealth.put(nodeId, true); // assume alive at start
                hashRing.addNode(nodeId);
                log.info("Registered node: " + nodeId + " @ " + address);
            }
        }
    }

    /** Mark a node healthy/unhealthy and update the hash ring. */
    public void updateHealth(String nodeId, boolean alive) {
        Boolean previous = nodeHealth.put(nodeId, alive);
        if (previous == null || previous != alive) {
            if (alive) {
                hashRing.addNode(nodeId);
                log.info("Node RECOVERED: " + nodeId);
            } else {
                hashRing.removeNode(nodeId);
                log.warning("Node FAILED: " + nodeId + " — removed from ring");
            }
        }
    }

    public boolean isAlive(String nodeId) {
        return nodeHealth.getOrDefault(nodeId, false);
    }

    /** Base URL for HTTP calls to a peer node, e.g. "http://node-2:8081" */
    public String getBaseUrl(String nodeId) {
        String addr = nodeAddresses.get(nodeId);
        return addr != null ? "http://" + addr : null;
    }

    public String getSelfId() { return selfId; }

    public Map<String, Boolean> getHealthMap() {
        return Map.copyOf(nodeHealth);
    }

    public Map<String, String> getAddressMap() {
        return Map.copyOf(nodeAddresses);
    }
}
