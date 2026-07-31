package com.kvstore.hashing;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/**
 * Consistent Hash Ring
 *
 * WHY consistent hashing?
 *   In a naive approach (key % N), adding or removing a node reshuffles almost
 *   every key. Consistent hashing limits reshuffling to only ~(1/N) of keys.
 *
 * HOW it works:
 *   1. Each node is placed at multiple points (virtual nodes) around a logical ring
 *      of 2^32 positions.
 *   2. A key is hashed to a position on that ring.
 *   3. Walking clockwise, the first node we hit is the "primary" owner of that key.
 *   4. The next (replicationFactor - 1) distinct nodes in clockwise order are replicas.
 */
@Component
public class ConsistentHashRing {

    // More virtual nodes = more even distribution but more memory
    private static final int VIRTUAL_NODES_PER_NODE = 150;

    // Sorted map: ring position -> node ID
    private final TreeMap<Long, String> ring = new TreeMap<>();

    // All real node IDs we know about
    private final Set<String> nodes = new LinkedHashSet<>();

    /**
     * Add a node to the ring. Creates VIRTUAL_NODES_PER_NODE phantom entries
     * so the node's keys are spread evenly rather than landing in one arc.
     */
    public synchronized void addNode(String nodeId) {
        nodes.add(nodeId);
        for (int i = 0; i < VIRTUAL_NODES_PER_NODE; i++) {
            long hash = hash(nodeId + "-vnode-" + i);
            ring.put(hash, nodeId);
        }
    }

    public synchronized void removeNode(String nodeId) {
        nodes.remove(nodeId);
        for (int i = 0; i < VIRTUAL_NODES_PER_NODE; i++) {
            long hash = hash(nodeId + "-vnode-" + i);
            ring.remove(hash);
        }
    }

    /**
     * Return the primary node responsible for this key.
     */
    public synchronized String getPrimaryNode(String key) {
        if (ring.isEmpty()) throw new IllegalStateException("Hash ring is empty");
        long keyHash = hash(key);
        // ceilingKey = first entry >= keyHash; if none, wrap around to ring.firstKey()
        Map.Entry<Long, String> entry = ring.ceilingEntry(keyHash);
        return (entry != null ? entry : ring.firstEntry()).getValue();
    }

    /**
     * Return `replicationFactor` distinct nodes for this key (primary + replicas).
     */
    public synchronized List<String> getReplicaNodes(String key, int replicationFactor) {
        if (ring.isEmpty()) return Collections.emptyList();

        List<String> replicas = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        long keyHash = hash(key);
        // Start from the ceiling entry and walk clockwise
        NavigableMap<Long, String> tailMap = ring.tailMap(keyHash, true);
        Iterator<Map.Entry<Long, String>> iter = tailMap.entrySet().iterator();

        // If we exhaust the tail, wrap around from the beginning
        if (!iter.hasNext()) {
            iter = ring.entrySet().iterator();
        }

        while (replicas.size() < replicationFactor && replicas.size() < nodes.size()) {
            if (!iter.hasNext()) {
                iter = ring.entrySet().iterator(); // wrap
            }
            Map.Entry<Long, String> entry = iter.next();
            if (seen.add(entry.getValue())) {      // add() returns false if already present
                replicas.add(entry.getValue());
            }
        }
        return replicas;
    }

    public synchronized Set<String> getAllNodes() {
        return Collections.unmodifiableSet(nodes);
    }

    public synchronized boolean isEmpty() {
        return ring.isEmpty();
    }

    // -------------------------------------------------------------------
    // MD5-based hash collapsed into an unsigned 32-bit long.
    // MD5 is NOT used for security here — just for even ring distribution.
    // -------------------------------------------------------------------
    private long hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            // Take the first 4 bytes and treat them as an unsigned 32-bit int
            return ((long) (digest[3] & 0xFF) << 24)
                 | ((long) (digest[2] & 0xFF) << 16)
                 | ((long) (digest[1] & 0xFF) << 8)
                 | ((long) (digest[0] & 0xFF));
        } catch (Exception e) {
            throw new RuntimeException("Hash failure", e);
        }
    }
}
