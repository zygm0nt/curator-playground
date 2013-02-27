package pl.touk.curator;

import com.google.common.base.Throwables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.x.discovery.ServiceInstance;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import pl.touk.model.WorkerMetadata;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author mcl
 */
public class ServiceDiscoverer implements Closeable {
    public static final String zkConnectionString = "127.0.0.1:2187";
    public static final String basePath = "/myApp/serviceDiscovery";

    private static Logger log = Logger.getLogger(ServiceDiscoverer.class);

    CuratorFramework curatorFramework;
    WorkerAdvertiser workerAdvertiser;

    public ServiceDiscoverer() {
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectionTimeoutMs(1000)
                .retryPolicy(new RetryNTimes(10, 500))
                .connectString(zkConnectionString)
                .build();
        curatorFramework.start();
    }

    public void discover() {
        try {
            new EnsurePath(basePath).ensure(curatorFramework.getZookeeperClient());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        WorkerFinder workerFinder = new WorkerFinder(curatorFramework, getInstanceSerializerFactory());

        for (ServiceInstance<WorkerMetadata> instance : workerFinder.getWorkers("app1")) {
            WorkerMetadata workerMetadata = instance.getPayload();
            log.info(workerMetadata);
        }
    }

    private InstanceSerializerFactory getInstanceSerializerFactory() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return new InstanceSerializerFactory(objectMapper.reader(), objectMapper.writer());
    }

    public void advertise() {
        workerAdvertiser = new WorkerAdvertiser(curatorFramework, getInstanceSerializerFactory(), "app1", "localhost", 2187);
        workerAdvertiser.advertiseAvailability();
        log.info("advertised...");
    }

    public void close() throws IOException {
        workerAdvertiser.deAdvertiseAvailability();
        workerAdvertiser.close();
        curatorFramework.close();
    }

    public static void main(String[] args) throws Exception {
        ServiceDiscoverer sd = new ServiceDiscoverer();

        sd.advertise();

        Thread.sleep(1000);

        sd.discover();

        sd.close();
    }
}
