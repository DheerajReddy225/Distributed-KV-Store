@Component
public class RaftConsensus {

    // Three possible states for every node
    enum State { FOLLOWER, CANDIDATE, LEADER }

    private State currentState = State.FOLLOWER;
    private int currentTerm = 0;
    private String votedFor = null;
    private final NodeRegistry nodeRegistry;

    // Election timeout — if no heartbeat received
    // within this time, start an election
    @Scheduled(fixedDelay = 300)
    public void electionTimeout() {
        if (currentState == State.FOLLOWER) {
            startElection();
        }
    }

    private void startElection() {
        currentState = State.CANDIDATE;
        currentTerm++;
        // Request votes from all peers
        // If majority vote yes → become LEADER
        // If majority vote no → revert to FOLLOWER
        int votes = requestVotesFromPeers();
        if (votes > totalNodes / 2) {
            becomeLeader();
        } else {
            currentState = State.FOLLOWER;
        }
    }

    private void becomeLeader() {
        currentState = State.LEADER;
        // This is the ONLY line that connects
        // Raft to the rest of the system
        nodeRegistry.updateHealth(selfId, true);
        startSendingHeartbeats();
    }

    // Leader sends heartbeat to all followers
    // every 50ms to prevent new elections
    @Scheduled(fixedDelay = 50)
    public void sendHeartbeats() {
        if (currentState == State.LEADER) {
            // ping all peers — "I'm alive, don't elect"
        }
    }
}