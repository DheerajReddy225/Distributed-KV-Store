package com.kvstore.raft;

import com.kvstore.config.NodeRegistry;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.Logger;

/**
 * RaftConsensus — replaces HealthCheckScheduler
 *
 * Implements the core Raft leader election mechanism:
 *  - Nodes start as FOLLOWER
 *  - If no heartbeat received within election timeout, become CANDIDATE and start election
 *  - CANDIDATE requests votes from all peers
 *  - If majority votes received, become LEADER
 *  - LEADER sends heartbeats every 50ms to prevent new elections
 *  - On leader change, updates NodeRegistry so the hash ring stays accurate
 */
@Component
public class RaftConsensus {

    private static final Logger log = Logger.getLogger(RaftConsensus.class.getName());

    // ── Node states ──────────────────────────────────────────────────────────
    enum State { FOLLOWER, CANDIDATE, LEADER }

    // ── Raft persistent state ────────────────────────────────────────────────
    private final AtomicInteger currentTerm = new AtomicInteger(0);
    private volatile String votedFor = null;
    private volatile State currentState = State.FOLLOWER;

    // ── Heartbeat tracking ───────────────────────────────────────────────────
    private volatile long lastHeartbeatMs = System.currentTimeMillis();

    // ── Config ───────────────────────────────────────────────────────────────
    @Value("${node.id:node-1}")
    private String selfId;

    @Value("${raft.election.timeout.min:150}")
    private int electionTimeoutMin;

    @Value("${raft.election.timeout.max:300}")
    private int electionTimeoutMax;

    private final NodeRegistry nodeRegistry;
    private final RestTemplate restTemplate = new RestTemplate();
    private final Random random = new Random();

    public RaftConsensus(NodeRegistry nodeRegistry) {
        this.nodeRegistry = nodeRegistry;
    }

    @PostConstruct
    public void init() {
        log.info("Raft starting on node: " + selfId + " as FOLLOWER");
    }

    /**
     * Election timeout check — runs every 50ms
     * If we haven't heard from a leader within the timeout window,
     * start an election
     */
    @Scheduled(fixedDelay = 50)
    public void checkElectionTimeout() {
        if (currentState == State.LEADER) return; // leaders don't start elections

        long elapsed = System.currentTimeMillis() - lastHeartbeatMs;
        int timeout = electionTimeoutMin
                + random.nextInt(electionTimeoutMax - electionTimeoutMin);

        if (elapsed > timeout) {
            log.info(selfId + " election timeout — starting election for term "
                    + (currentTerm.get() + 1));
            startElection();
        }
    }

    /**
     * Heartbeat sender — runs every 50ms
     * Only the LEADER sends heartbeats to prevent followers from timing out
     */
    @Scheduled(fixedDelay = 50)
    public void sendHeartbeats() {
        if (currentState != State.LEADER) return;

        for (Map.Entry<String, String> entry : nodeRegistry.getAddressMap().entrySet()) {
            String nodeId = entry.getKey();
            if (nodeId.equals(selfId)) continue;

            String url = nodeRegistry.getBaseUrl(nodeId) + "/raft/heartbeat";
            try {
                Map<String, Object> body = new HashMap<>();
                body.put("term", currentTerm.get());
                body.put("leaderId", selfId);
                restTemplate.postForEntity(url, body, Void.class);
                nodeRegistry.updateHealth(nodeId, true);
            } catch (Exception e) {
                log.warning("Heartbeat failed to " + nodeId + " — marking dead");
                nodeRegistry.updateHealth(nodeId, false);
            }
        }
    }

    /**
     * Start an election:
     * 1. Increment term
     * 2. Vote for self
     * 3. Request votes from peers
     * 4. If majority — become leader
     */
    private synchronized void startElection() {
        currentState = State.CANDIDATE;
        int newTerm = currentTerm.incrementAndGet();
        votedFor = selfId;

        int votes = 1; // vote for self
        int totalNodes = nodeRegistry.getAddressMap().size();

        for (Map.Entry<String, String> entry : nodeRegistry.getAddressMap().entrySet()) {
            String nodeId = entry.getKey();
            if (nodeId.equals(selfId)) continue;

            try {
                String url = nodeRegistry.getBaseUrl(nodeId) + "/raft/vote";
                Map<String, Object> request = new HashMap<>();
                request.put("term", newTerm);
                request.put("candidateId", selfId);

                ResponseEntity<Map> response =
                        restTemplate.postForEntity(url, request, Map.class);

                if (response.getBody() != null
                        && Boolean.TRUE.equals(response.getBody().get("voteGranted"))) {
                    votes++;
                    log.info("Vote received from " + nodeId
                            + " — total votes: " + votes);
                }
            } catch (Exception e) {
                log.warning("Vote request failed to " + nodeId);
            }
        }

        // Check if we won the majority
        if (votes > totalNodes / 2) {
            becomeLeader();
        } else {
            log.info(selfId + " lost election for term " + newTerm
                    + " — reverting to FOLLOWER");
            currentState = State.FOLLOWER;
        }
    }

    /**
     * Become the leader — update the cluster registry
     * This is the ONLY point where Raft connects to the rest of the system
     */
    private void becomeLeader() {
        currentState = State.LEADER;
        log.info("*** " + selfId + " became LEADER for term "
                + currentTerm.get() + " ***");
        // Mark self as alive in the registry — same method health checker used
        nodeRegistry.updateHealth(selfId, true);
    }

    /**
     * Called by RaftController when a heartbeat arrives from the leader
     */
    public synchronized void receiveHeartbeat(int term, String leaderId) {
        if (term >= currentTerm.get()) {
            currentTerm.set(term);
            currentState = State.FOLLOWER;
            votedFor = null;
            lastHeartbeatMs = System.currentTimeMillis();
            nodeRegistry.updateHealth(leaderId, true);
        }
    }

    /**
     * Called by RaftController when a vote request arrives
     * Grant vote if:
     *  - The candidate's term is >= our term
     *  - We haven't voted for anyone else this term
     */
    public synchronized boolean handleVoteRequest(int term, String candidateId) {
        if (term > currentTerm.get()) {
            currentTerm.set(term);
            currentState = State.FOLLOWER;
            votedFor = null;
        }

        boolean voteGranted = (term >= currentTerm.get())
                && (votedFor == null || votedFor.equals(candidateId));

        if (voteGranted) {
            votedFor = candidateId;
            lastHeartbeatMs = System.currentTimeMillis(); // reset timeout
            log.info(selfId + " voted for " + candidateId
                    + " in term " + term);
        }

        return voteGranted;
    }

    public State getCurrentState() { return currentState; }
    public int getCurrentTerm()    { return currentTerm.get(); }
}