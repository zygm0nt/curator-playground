package pl.touk.curator;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.atomic.AtomicValue;
import com.netflix.curator.framework.recipes.atomic.DistributedAtomicLong;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

import java.util.Random;

/**
 * @author mcl
 */
public class IncrementingRacer extends BaseExperimentRunner {

    private static Logger log = Logger.getLogger(IncrementingRacer.class);

    private DistributedAtomicLong distributedAtomicLong;

    @Override
    protected String getPath() {
        return "/increment/";
    }

    @Override
    protected BaseExperimentRunner instantiate(String zkAddr, String serverId, String path) throws Exception {
        return new IncrementingRacer(zkAddr, serverId, path);
    }

    IncrementingRacer() { }

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

    public static void main(String[] args) throws Exception {
        new IncrementingRacer().run();
    }
}
