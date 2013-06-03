package pl.touk.curator;

import com.google.common.io.Closeables;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import pl.touk.model.ClusterStatus;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mcl
 */
public class LeaderLatchSelection implements Closeable {

    private static Logger log = Logger.getLogger(LeaderLatchSelection.class);

    LeaderLatch leaderLatch;
    String serverId;
    CuratorFramework client;
    private static String LEADER_PATH = "/apps/leaderLatch/abc";

    public LeaderLatchSelection(String connectionString, String serverId) throws InterruptedException {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        client.start();
        client.getZookeeperClient().blockUntilConnectedOrTimedOut();
        leaderLatch = new LeaderLatch(client, LEADER_PATH, serverId);

        this.serverId = serverId;
    }

    public void start() throws Exception {
        leaderLatch.start();
        watchLeaderChildren(client);


    }

    public void work() throws Exception {
        for (int i = 0; i < 5;) {
            if (clusterStatus().isLeader) {
                log.info(String.format("(%s) ping", serverId));
                i++;
            }
            Thread.sleep(2000);
        }
        close();
    }

    public void close() throws IOException {
        leaderLatch.close();
    }


    public void watchLeaderChildren(final CuratorFramework client) throws Exception {
        client.getChildren().usingWatcher(new CuratorWatcher() {
            public void process(WatchedEvent event) throws Exception {
                ClusterStatus cs = clusterStatus();

                // Do something with cluster status (log leadership change, etc)
                log.info("Got event: " + cs);

                // Re-set watch
                client.getChildren().usingWatcher(this).inBackground().forPath(LEADER_PATH);
            }
        }).inBackground().forPath(LEADER_PATH);
    }

    private ClusterStatus clusterStatus() throws Exception {
        boolean isLeader = leaderLatch.hasLeadership();
        Iterable<Participant> participants = leaderLatch.getParticipants();
        String leader = findLeader(participants);

        return new ClusterStatus(serverId, isLeader, leader, participantIds(participants));
    }

    private String findLeader(Iterable<Participant> participants) {
        for (Participant participant : participants)
            if (participant.isLeader())
                return participant.getId();
        return "<none>";
    }

    private Iterable<String> participantIds(Iterable<Participant> participants) {
        List<String> retVal = new ArrayList<String>();

        for (Participant participant : participants)
            retVal.add(participant.getId());
        return retVal;
    }

    public static void main(String[] args) throws Exception {
        List<LeaderLatchSelection> threads = new ArrayList();
        TestingServer server = new TestingServer();

        try {
            for (int i = 0; i < 3; i++) {
                final String serverId = "" + i;
                LeaderLatchSelection ls = new LeaderLatchSelection(server.getConnectString(), serverId);
                threads.add(ls);

                ls.start();
            }
            for (LeaderLatchSelection l : threads) l.work();
            Thread.sleep(50000);
        } finally {
            for (Closeable closeable : threads) Closeables.closeQuietly(closeable);
        }
    }
}
