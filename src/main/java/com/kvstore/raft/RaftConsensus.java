package com.kvstore.raft;

import com.kvstore.config.NodeRegistry;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.Logger;

@Component
public class RaftConsensus {

    private static final Logger log = Logger.getLogger(RaftConsensus.class.getName());

    public enum State { FOLLOWER, CANDIDATE, LEADER }

    private final AtomicInteger currentTerm  = new AtomicInteger(0);
    private volatile String     votedFor     = null;
    private volatile State      currentState = State.FOLLOWER;
    private volatile long       lastHeartbeatMs;
    private volatile String     currentLeaderId = null;

    @Value("${node.id:node-1}")
    private String selfId;

    @Value("${raft.election.timeout.min:150}")
    private int electionTimeoutMin;

    @Value("${raft.election.timeout.max:600}")
    private int electionTimeoutMax;

    private final NodeRegistry nodeRegistry;
    private final Random       random = new Random();
    private final RestTemplate restTemplate;

    public RaftConsensus(NodeRegistry nodeRegistry) {
        this.nodeRegistry = nodeRegistry;
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(200);
        factory.setReadTimeout(200);
        this.restTemplate = new RestTemplate(factory);
    }

    @PostConstruct
    public void init() throws InterruptedException {
        int startupDelay = random.nextInt(electionTimeoutMax);
        log.info(selfId + " waiting " + startupDelay + "ms before starting Raft");
        Thread.sleep(startupDelay);
        lastHeartbeatMs = System.currentTimeMillis();
        log.info(selfId + " Raft started as FOLLOWER");
    }

    @Scheduled(fixedDelay = 75)
    public void checkElectionTimeout() {
        if (currentState == State.LEADER) return;
        long elapsed = System.currentTimeMillis() - lastHeartbeatMs;
        int timeout  = electionTimeoutMin
                + random.nextInt(electionTimeoutMax - electionTimeoutMin);
        if (elapsed > timeout) {
            log.info(selfId + " timed out — starting election for term "
                    + (currentTerm.get() + 1));
            startElection();
        }
    }

    @Scheduled(fixedDelay = 50)
    public void sendHeartbeats() {
        if (currentState != State.LEADER) return;
        for (Map.Entry<String, String> entry
                : nodeRegistry.getAddressMap().entrySet()) {
            String nodeId = entry.getKey();
            if (nodeId.equals(selfId)) continue;
            String url = nodeRegistry.getBaseUrl(nodeId) + "/raft/heartbeat";
            try {
                Map<String, Object> body = new HashMap<>();
                body.put("term",     currentTerm.get());
                body.put("leaderId", selfId);
                restTemplate.postForEntity(url, body, Void.class);
                nodeRegistry.updateHealth(nodeId, true);
            } catch (Exception e) {
                log.warning("Heartbeat failed to " + nodeId);
                nodeRegistry.updateHealth(nodeId, false);
            }
        }
    }

    private synchronized void startElection() {
        if (currentState == State.LEADER) return;
        currentState = State.CANDIDATE;
        int newTerm  = currentTerm.incrementAndGet();
        votedFor     = selfId;

        int votes      = 1;
        int totalNodes = nodeRegistry.getAddressMap().size();

        log.info(selfId + " requesting votes for term " + newTerm);

        for (Map.Entry<String, String> entry
                : nodeRegistry.getAddressMap().entrySet()) {
            String nodeId = entry.getKey();
            if (nodeId.equals(selfId)) continue;
            try {
                String url = nodeRegistry.getBaseUrl(nodeId) + "/raft/vote";
                Map<String, Object> request = new HashMap<>();
                request.put("term",        newTerm);
                request.put("candidateId", selfId);
                ResponseEntity<Map> response =
                        restTemplate.postForEntity(url, request, Map.class);
                if (response.getBody() != null
                        && Boolean.TRUE.equals(
                                response.getBody().get("voteGranted"))) {
                    votes++;
                    log.info(selfId + " got vote from " + nodeId
                            + " — total: " + votes + "/" + totalNodes);
                }
            } catch (Exception e) {
                log.warning(selfId + " vote request to " + nodeId + " failed");
            }
        }

        if (votes > totalNodes / 2) {
            becomeLeader();
        } else {
            log.info(selfId + " lost election — reverting to FOLLOWER");
            currentState    = State.FOLLOWER;
            lastHeartbeatMs = System.currentTimeMillis();
        }
    }

    private void becomeLeader() {
        currentState    = State.LEADER;
        currentLeaderId = selfId;
        log.info("*** " + selfId + " is now LEADER for term "
                + currentTerm.get() + " ***");
        nodeRegistry.updateHealth(selfId, true);
    }

    public synchronized void receiveHeartbeat(int term, String leaderId) {
        if (term >= currentTerm.get()) {
            if (currentState != State.FOLLOWER) {
                log.info(selfId + " stepping down — heartbeat from "
                        + leaderId + " term " + term);
            }
            currentTerm.set(term);
            currentState    = State.FOLLOWER;
            votedFor        = null;
            lastHeartbeatMs = System.currentTimeMillis();
            currentLeaderId = leaderId;
            nodeRegistry.updateHealth(leaderId, true);
        }
    }

    public synchronized boolean handleVoteRequest(int term, String candidateId) {
        if (term > currentTerm.get()) {
            currentTerm.set(term);
            currentState = State.FOLLOWER;
            votedFor     = null;
        }
        boolean voteGranted = (term >= currentTerm.get())
                && (votedFor == null || votedFor.equals(candidateId));
        if (voteGranted) {
            votedFor        = candidateId;
            lastHeartbeatMs = System.currentTimeMillis();
            log.info(selfId + " voted for " + candidateId + " in term " + term);
        } else {
            log.info(selfId + " denied vote for " + candidateId
                    + " (already voted for: " + votedFor + ")");
        }
        return voteGranted;
    }

    public boolean  isLeader()           { return currentState == State.LEADER; }
    public String   getCurrentLeaderId() { return currentLeaderId; }
    public State    getCurrentState()    { return currentState; }
    public int      getCurrentTerm()     { return currentTerm.get(); }
    public String   getSelfId()          { return selfId; }
}