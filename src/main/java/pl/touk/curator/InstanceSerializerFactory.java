package pl.touk.curator;

import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.type.TypeReference;

import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.details.InstanceSerializer;

/**
 * @author mcl
 */
public class InstanceSerializerFactory {
    private final ObjectReader objectReader;
    private final ObjectWriter objectWriter;

    InstanceSerializerFactory(ObjectReader objectReader, ObjectWriter objectWriter) {
        this.objectReader = objectReader;
        this.objectWriter = objectWriter;
    }

    public <T> InstanceSerializer<T> getInstanceSerializer(
            TypeReference<ServiceInstance<T>> typeReference) {
        return new JacksonInstanceSerializer<T>(objectReader, objectWriter, typeReference);
    }

}
