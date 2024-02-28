package org.acme;

// if using Quarkus 2.16.12.Final
//import javax.ws.rs.GET;
//import javax.ws.rs.PUT;
//import javax.ws.rs.Path;
//import javax.ws.rs.core.Response;

// if using Quarkus 3.6.8
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

import java.util.Map;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@Path("/connectors")
@RegisterRestClient
public interface KafkaConnectorRestClient {
    @GET
    @Path("/kafka-data-source-cdc-connector/config")
    Response getKafkaDataCdcConnector();

    @PUT
    @Path("/kafka-data-source-cdc-connector/config")
    Response configureKafkaDataCdcConnector(Map<String, String> config);
}
