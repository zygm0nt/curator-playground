package org.apache.curator.x.discovery;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author mcl
 */
public class JsonServiceInstance<T> extends ServiceInstance<T> {

    @JsonCreator
    public JsonServiceInstance(@JsonProperty("name") String name,
                               @JsonProperty("id") String id,
                               @JsonProperty("address") String address,
                               @JsonProperty("port") Integer port,
                               @JsonProperty("sslPort") Integer sslPort,
                               @JsonProperty("payload") T payload,
                               @JsonProperty("registrationTimeUTC") long registrationTimeUTC,
                               @JsonProperty("serviceType") ServiceType serviceType,
                               @JsonProperty("uriSpec") UriSpec uriSpec) {
        super(name, id, address, port, sslPort, payload, registrationTimeUTC, serviceType, uriSpec);
    }
}
