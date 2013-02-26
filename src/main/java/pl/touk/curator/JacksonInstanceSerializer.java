package pl.touk.curator;

import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.details.InstanceSerializer;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.type.TypeReference;

import java.io.ByteArrayOutputStream;

/**
 * @author mcl
 */
final class JacksonInstanceSerializer<T> implements InstanceSerializer<T> {
    private final TypeReference<ServiceInstance<T>> typeRef;
    private final ObjectWriter objectWriter;
    private final ObjectReader objectReader;

    private static Logger log = Logger.getLogger(JacksonInstanceSerializer.class);

    JacksonInstanceSerializer(ObjectReader objectReader, ObjectWriter objectWriter,
                              TypeReference<ServiceInstance<T>> typeRef) {
        this.objectReader = objectReader;
        this.objectWriter = objectWriter;
        this.typeRef = typeRef;
    }

    public ServiceInstance<T> deserialize(byte[] bytes) throws Exception {
        String s = new String(bytes);
        log.info(s);

        return objectReader.withType(typeRef).readValue(bytes);
    }

    public byte[] serialize(ServiceInstance<T> serviceInstance) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        objectWriter.writeValue(out, serviceInstance);
        return out.toByteArray();
    }
}
