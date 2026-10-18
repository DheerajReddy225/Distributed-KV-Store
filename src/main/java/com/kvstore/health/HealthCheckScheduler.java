package com.kvstore.health;

import com.kvstore.config.NodeRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
import java.util.logging.Logger;

/**
 * HealthCheckScheduler
 *
 * Every N milliseconds this pings every peer's /actuator/health endpoint.
 * If a node stops responding it is marked dead and removed from the hash ring —
 * this is the "leader failover" mechanism: the cluster automatically routes
 * around failures without any manual intervention.
 *
 * In a production system you'd use a consensus protocol (Raft / Paxos) for
 * proper leader election; this simpler approach is intentional for clarity.
 */
@Component
public class HealthCheckScheduler {

    private static final Logger log = Logger.getLogger(HealthCheckScheduler.class.getName());

    private final NodeRegistry nodeRegistry;
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${node.id:node-1}")
    private String selfId;

    public HealthCheckScheduler(NodeRegistry nodeRegistry) {
        this.nodeRegistry = nodeRegistry;
    }

    @Scheduled(fixedDelayString = "${health.check.interval.ms:3000}")
    public void checkPeers() {
        for (Map.Entry<String, String> entry : nodeRegistry.getAddressMap().entrySet()) {
            String nodeId = entry.getKey();
            if (nodeId.equals(selfId)) continue; // don't ping ourselves

            String url = nodeRegistry.getBaseUrl(nodeId) + "/actuator/health";
            try {
                restTemplate.getForObject(url, String.class);
                nodeRegistry.updateHealth(nodeId, true);
            } catch (Exception e) {
                nodeRegistry.updateHealth(nodeId, false);
                log.warning("Health check failed for " + nodeId + ": " + e.getMessage());
            }
        }
    }
}
