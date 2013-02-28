package pl.touk.curator;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author mcl
 */
public class LockingRemotely implements Closeable {

    private static Logger log = Logger.getLogger(LockingRemotely.class);

    private InterProcessSemaphoreMutex lock;

    public LockingRemotely(String connectionString, String serverId, String lockPath) throws InterruptedException {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        client.start();
        client.getZookeeperClient().blockUntilConnectedOrTimedOut();
        lock = new InterProcessSemaphoreMutex(client, lockPath);
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


    public void close() throws IOException {

    }

    public static void main(String[] args) throws Exception {
        List<Thread> threads = new ArrayList();

        final String path = "/locks/" + UUID.randomUUID().toString().split("-")[0];
        for (int i = 0; i < 3; i++) {
            final String serverId = "" + i;
            threads.add(new Thread() {
                @Override
                public void run() {
                    setName("Zk thread " + serverId);
                    try {
                        LockingRemotely ls = new LockingRemotely("localhost:2187", serverId, path);
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
