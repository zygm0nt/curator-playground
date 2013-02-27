package pl.touk.curator;


import com.netflix.curator.x.discovery.JsonServiceInstance;
import com.netflix.curator.x.discovery.details.InstanceSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Test;
import pl.touk.model.WorkerMetadata;

/**
 * @author mcl
 */
public class JsonSerializerTest {

    String input = "{\"address\":\"localhost\",\"name\":\"app1\",\"id\":\"bd3d5f22-ae34-42f0-b2db-78b726cce71a\",\"port\":2187," +
            "\"payload\":{\"@class\":\"pl.touk.model.WorkerMetadata\",\"workerId\":\"bd3d5f22-ae34-42f0-b2db-78b726cce71a\",\"listenAddress\":\"localhost\",\"listenPort\":2187}," +
            "\"sslPort\":null,\"registrationTimeUTC\":1361874297096,\"serviceType\":\"DYNAMIC\",\"uriSpec\":null}";

    @Test
    public void shouldDeserializeFromString() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        InstanceSerializer serializer = new InstanceSerializerFactory(objectMapper.reader(), objectMapper.writer()).getInstanceSerializer(
                new TypeReference<JsonServiceInstance<WorkerMetadata>>() {
                });
        Object o = serializer.deserialize(input.getBytes());
        o.getClass();
    }
}
