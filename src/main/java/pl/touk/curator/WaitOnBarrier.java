package pl.touk.curator;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.barriers.DistributedBarrier;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * @author mcl
 */
public class WaitOnBarrier implements Closeable {

    private static Logger log = Logger.getLogger(WaitOnBarrier.class);

    DistributedBarrier barrier;

    public WaitOnBarrier(String connectionString, String serverId, String lockPath) throws InterruptedException {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        client.start();
        client.getZookeeperClient().blockUntilConnectedOrTimedOut();

        barrier = new DistributedBarrier(client, lockPath);
    }

    public void process() throws Exception {
        long threadId = Thread.currentThread().getId();
        barrier.setBarrier();

        if (threadId % 2 != 0) {
            log.info("I'm the one to remove all barriers!");
            Thread.sleep(2000);
            barrier.removeBarrier();
        } else
            log.info(String.format("[%s] Waiting on barrier", threadId));
            barrier.waitOnBarrier();

        log.info(String.format("[%s] Doing some work", threadId));
        Thread.sleep(1 + Math.abs(new Random().nextInt()) % 5000);
    }

    public void close() throws IOException {
        try {
            barrier.removeBarrier();
        } catch (Exception e) {
            log.error("Error removing barrier", e);
        }
    }

    public static void main(String[] args) throws Exception {
        List<Thread> threads = new ArrayList();

        final String path = "/barrier/" + UUID.randomUUID().toString().split("-")[0];
        for (int i = 0; i < 3; i++) {
            final String serverId = "" + i;
            threads.add(new Thread() {
                @Override
                public void run() {
                    setName("Zk thread " + serverId);
                    try {
                        WaitOnBarrier ls = new WaitOnBarrier("localhost:2187", serverId, path);
                        ls.process();
                    } catch (Exception e) {
                        log.error("Thread error: ", e);
                    }
                }
            });
            threads.get(i).start();
        }
        Thread.sleep(50000);
    }
}
