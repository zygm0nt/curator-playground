package pl.touk.curator;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.atomic.AtomicValue;
import com.netflix.curator.framework.recipes.atomic.DistributedAtomicLong;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * @author mcl
 */
public class IncrementingRacer {

    private static Logger log = Logger.getLogger(IncrementingRacer.class);

    private DistributedAtomicLong distributedAtomicLong;

    public IncrementingRacer(String connectionString, String serverId, String counterPath) throws InterruptedException {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        client.start();
        client.getZookeeperClient().blockUntilConnectedOrTimedOut();

        distributedAtomicLong = new DistributedAtomicLong(client, counterPath, new ExponentialBackoffRetry(100, 3));
    }

    public void process() throws Exception {
        Thread.sleep(Math.abs(new Random().nextInt()) % 1000);
        long threadId = Thread.currentThread().getId();
        long lastValue = 0;
        for (int i = 0; i < 100; i++) {
            distributedAtomicLong.compareAndSet(lastValue, (long)i);
            AtomicValue<Long> retVal = distributedAtomicLong.increment();
            lastValue = retVal.postValue();
            log.info(String.format("[%s] thread after update - status %s ", threadId, retVal.getStats().getOptimisticTimeMs()));
        }

        log.info("Incremented to: " + distributedAtomicLong.get().postValue() + " status: " + distributedAtomicLong.get().getStats());
    }

    public void close() throws IOException {

    }

    public static void main(String[] args) throws Exception {
        List<Thread> threads = new ArrayList();

        final String path = "/increment/" + UUID.randomUUID().toString().split("-")[0];
        for (int i = 0; i < 3; i++) {
            final String serverId = "" + i;
            threads.add(new Thread() {
                @Override
                public void run() {
                    setName("Zk thread " + serverId);
                    try {
                        IncrementingRacer ls = new IncrementingRacer("localhost:2187", serverId, path);
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
