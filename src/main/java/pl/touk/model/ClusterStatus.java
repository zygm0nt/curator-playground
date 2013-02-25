package pl.touk.model;

/**
 * @author mcl
 */
public class ClusterStatus {
    public String current;
    public boolean isLeader;
    public String leader;
    public Iterable<String> participants;

    public ClusterStatus(String serverId, boolean isLeader, String leader, Iterable<String> participants) {
        this.isLeader = isLeader;
        this.leader = leader;
        this.current = serverId;
        this.participants = participants;
    }

    @Override
    public String toString() {
        return "ClusterStatus{" +
                "current='" + current + '\'' +
                ", isLeader=" + isLeader +
                ", leader='" + leader + '\'' +
                ", participants=" + participants +
                '}';
    }
}
