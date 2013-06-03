package pl.touk.curator;

import com.google.common.io.Closeables;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mcl
 */
public class LeaderSelection  implements Closeable, LeaderSelectorListener {

    private static Logger log = Logger.getLogger(LeaderSelection.class);

    Thread ourThread;
    LeaderSelector leaderSelector;

    String serverId;

    public LeaderSelection(String connectionString, String serverId) throws InterruptedException {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        client.start();
        client.getZookeeperClient().blockUntilConnectedOrTimedOut();
        leaderSelector = new LeaderSelector(client, "/apps/leaderElection/ping", this);
        leaderSelector.autoRequeue();
        this.serverId = serverId;
    }

    public void start() throws IOException {
        // the selection for this instance doesn't start until the leader selector is started
        // leader selection is done in the background so this call to leaderSelector.start() returns immediately
        leaderSelector.start();
    }

    public void close() throws IOException {
        leaderSelector.close();
    }

    private void working() {
        try {
            for (int i = 0; i < 5; i++) {
                log.info("ping");
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            log.warn("Relinquished leadership");
        }
    }

    public static void main(String[] args) throws Exception {
        List<LeaderSelection> threads = new ArrayList();
        TestingServer server = new TestingServer();

        try {
            for (int i = 0; i < 3; i++) {
                final String serverId = "" + i;
                LeaderSelection ls = new LeaderSelection(server.getConnectString(), serverId);
                threads.add(ls);

                ls.start();
            }

            Thread.sleep(50000);
        } finally {
            for (LeaderSelection l : threads) Closeables.closeQuietly(l);
        }
    }

    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
        ourThread = Thread.currentThread();
        log.info(String.format("(%s) Got leadership", serverId));

        working();
    }

    public void stateChanged(CuratorFramework curatorFramework, ConnectionState newState) {
        log.debug("Got connection state change: " + newState);

        if ( (newState == ConnectionState.LOST) || (newState == ConnectionState.SUSPENDED) ) {
            if ( ourThread != null ) {
                ourThread.interrupt();
            }
        }
    }
}
