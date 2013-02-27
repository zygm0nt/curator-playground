package pl.touk.curator;

import com.google.common.base.Throwables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.ZKPaths;
import com.netflix.curator.x.discovery.JsonServiceInstance;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import org.apache.log4j.Logger;
import org.codehaus.jackson.type.TypeReference;
import pl.touk.model.WorkerMetadata;

import java.util.Collection;
import java.util.List;

/**
 * @author mcl
 */
public final class WorkerFinder {

    private final ServiceDiscovery<WorkerMetadata> discovery;
    private CuratorFramework curatorFramework;

    private static Logger log = Logger.getLogger(WorkerFinder.class);

    WorkerFinder(CuratorFramework curatorFramework, InstanceSerializerFactory instanceSerializerFactory) {

        discovery = ServiceDiscoveryBuilder.builder(WorkerMetadata.class)
                .basePath(ServiceDiscoverer.basePath)
                .client(curatorFramework)
                .serializer(instanceSerializerFactory
                        .getInstanceSerializer(new TypeReference<JsonServiceInstance<WorkerMetadata>>() {
                        }))
                .build();

        this.curatorFramework = curatorFramework;

        try {
            discovery.start();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public Collection<ServiceInstance<WorkerMetadata>> getWorkers(String serviceName) {

        Collection<ServiceInstance<WorkerMetadata>> instances;
        try {
            instances = discovery.queryForInstances(serviceName);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return instances;
    }

    private void p(String name) throws Exception {
        String path = ZKPaths.makePath(ZKPaths.makePath(ServiceDiscoverer.basePath, name), null);

        List<String> files = curatorFramework.getChildren().forPath("/myApp/serviceDiscovery");
        log.debug(files);
    }
}
