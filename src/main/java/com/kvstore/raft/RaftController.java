package com.kvstore.raft;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftConsensus raftConsensus;

    public RaftController(RaftConsensus raftConsensus) {
        this.raftConsensus = raftConsensus;
    }

    @PostMapping("/heartbeat")
    public ResponseEntity<Void> receiveHeartbeat(
            @RequestBody Map<String, Object> body) {
        int    term     = (Integer) body.get("term");
        String leaderId = (String)  body.get("leaderId");
        raftConsensus.receiveHeartbeat(term, leaderId);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/vote")
    public ResponseEntity<Map<String, Object>> requestVote(
            @RequestBody Map<String, Object> body) {
        int    term        = (Integer) body.get("term");
        String candidateId = (String)  body.get("candidateId");
        boolean voteGranted = raftConsensus.handleVoteRequest(term, candidateId);
        Map<String, Object> response = new HashMap<>();
        response.put("voteGranted", voteGranted);
        response.put("term",        raftConsensus.getCurrentTerm());
        return ResponseEntity.ok(response);
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> status = new HashMap<>();
        status.put("state",    raftConsensus.getCurrentState());
        status.put("term",     raftConsensus.getCurrentTerm());
        status.put("leaderId", raftConsensus.getCurrentLeaderId());
        status.put("isLeader", raftConsensus.isLeader());
        return ResponseEntity.ok(status);
    }
}