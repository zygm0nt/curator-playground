package pl.touk.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Random;

/**
 * @author mcl
 */
public class WaitOnBarrier extends BaseExperimentRunner {

    private static Logger log = Logger.getLogger(WaitOnBarrier.class);

    DistributedBarrier barrier;

    WaitOnBarrier() {}

    public WaitOnBarrier(String connectionString, String serverId, String lockPath) throws InterruptedException {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        client.start();
        client.getZookeeperClient().blockUntilConnectedOrTimedOut();

        barrier = new DistributedBarrier(client, lockPath);
    }

    @Override
    protected String getPath() {
        return "/barrier/";
    }

    @Override
    protected BaseExperimentRunner instantiate(String zkAddr, String serverId, String path) throws Exception {
        return new WaitOnBarrier("localhost:2187", serverId, path);
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
        new WaitOnBarrier().run();
    }
}
