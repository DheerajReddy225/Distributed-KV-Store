package com.kvstore;

import com.kvstore.hashing.ConsistentHashRing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashRingTest {

    private ConsistentHashRing ring;

    @BeforeEach
    void setup() {
        ring = new ConsistentHashRing();
        ring.addNode("node-1");
        ring.addNode("node-2");
        ring.addNode("node-3");
    }

    @Test
    void getPrimaryNode_returnsSameNodeForSameKey() {
        String n1 = ring.getPrimaryNode("my-key");
        String n2 = ring.getPrimaryNode("my-key");
        assertEquals(n1, n2, "Same key must always map to same node");
    }

    @Test
    void getReplicaNodes_returnsCorrectCount() {
        List<String> replicas = ring.getReplicaNodes("my-key", 2);
        assertEquals(2, replicas.size());
        // All replicas must be distinct nodes
        assertEquals(replicas.size(), Set.copyOf(replicas).size());
    }

    @Test
    void removeNode_redistributesKeysNotCrash() {
        String before = ring.getPrimaryNode("key-that-might-move");
        ring.removeNode("node-2");
        // Must still return A node (not crash)
        String after = ring.getPrimaryNode("key-that-might-move");
        assertNotNull(after);
        // If the removed node was the primary, a different one should take over
        assertTrue(ring.getAllNodes().contains(after));
    }

    @Test
    void addNode_doesNotBreakExistingLookups() {
        ring.addNode("node-4");
        // Should not throw and should return a valid node
        String owner = ring.getPrimaryNode("stable-key");
        assertTrue(Set.of("node-1", "node-2", "node-3", "node-4").contains(owner));
    }
}
