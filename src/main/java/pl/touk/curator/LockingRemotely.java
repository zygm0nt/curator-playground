package pl.touk.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * @author mcl
 */
public class LockingRemotely extends BaseExperimentRunner {

    private static Logger log = Logger.getLogger(LockingRemotely.class);

    private InterProcessSemaphoreMutex lock;

    LockingRemotely() {}

    public LockingRemotely(String connectionString, String serverId, String lockPath) throws InterruptedException {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        client.start();
        client.getZookeeperClient().blockUntilConnectedOrTimedOut();
        lock = new InterProcessSemaphoreMutex(client, lockPath);
    }

    @Override
    protected String getPath() {
        return "/locks/";
    }

    @Override
    protected BaseExperimentRunner instantiate(String zkAddr, String serverId, String path) throws Exception {
        return new LockingRemotely("localhost:2187", serverId, path);
    }

    public void process() throws Exception {
        long threadId = Thread.currentThread().getId();
        log.info(String.format("[%s] Acquiring an exclusive lock", threadId));
        lock.acquire(5, TimeUnit.MINUTES);
        log.info(String.format("[%s] Doing some work", threadId));
        Thread.sleep(5000);
        log.info(String.format("[%s] Releasing a lock", threadId));
        lock.release();
    }


    public static void main(String[] args) throws Exception {
        new LockingRemotely().run();
    }
}
