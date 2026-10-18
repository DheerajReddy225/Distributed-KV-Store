package com.kvstore.raft;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * RaftController — HTTP endpoints for Raft messages
 *
 * /raft/heartbeat  — receives heartbeats from the leader
 * /raft/vote       — receives vote requests from candidates
 * /raft/status     — shows current Raft state (useful for debugging)
 */
@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftConsensus raftConsensus;

    public RaftController(RaftConsensus raftConsensus) {
        this.raftConsensus = raftConsensus;
    }

    /**
     * Leader calls this on all followers every 50ms
     * Prevents followers from starting a new election
     */
    @PostMapping("/heartbeat")
    public ResponseEntity<Void> receiveHeartbeat(@RequestBody Map<String, Object> body) {
        int term = (Integer) body.get("term");
        String leaderId = (String) body.get("leaderId");
        raftConsensus.receiveHeartbeat(term, leaderId);
        return ResponseEntity.ok().build();
    }

    /**
     * Candidates call this during an election to request a vote
     * Returns whether the vote was granted
     */
    @PostMapping("/vote")
    public ResponseEntity<Map<String, Object>> requestVote(
            @RequestBody Map<String, Object> body) {
        int term = (Integer) body.get("term");
        String candidateId = (String) body.get("candidateId");

        boolean voteGranted = raftConsensus.handleVoteRequest(term, candidateId);

        Map<String, Object> response = new HashMap<>();
        response.put("voteGranted", voteGranted);
        response.put("term", raftConsensus.getCurrentTerm());
        return ResponseEntity.ok(response);
    }

    /**
     * Useful for checking which node is currently the leader
     * curl http://localhost:8081/raft/status
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> status = new HashMap<>();
        status.put("state", raftConsensus.getCurrentState());
        status.put("term", raftConsensus.getCurrentTerm());
        return ResponseEntity.ok(status);
    }
}