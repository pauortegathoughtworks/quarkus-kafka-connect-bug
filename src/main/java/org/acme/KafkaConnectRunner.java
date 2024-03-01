package org.acme;

import static java.util.concurrent.TimeUnit.SECONDS;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

// if using Quarkus 2.16.12.Final
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

// if using Quarkus 3.6.8
//import jakarta.enterprise.context.ApplicationScoped;
//import jakarta.enterprise.event.Observes;

import java.util.*;

import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.runtime.Connect;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class KafkaConnectRunner {

    private static final String[] KAFKA_DISTRIBUTED_CONFIG_PROPERTIES = {
            "bootstrap.servers",
            "cleanup.policy",
            "config.providers",
            "config.providers.file.class",
            "config.storage.replication.factor",
            "config.storage.topic",
            "consumer.isolation.level",
            "group.id",
            "key.converter",
            "key.converter",
            "key.converter.schema.registry.url",
            "key.converter.schemas.enable",
            "latest.compatibility.strict",
            "offset.flush.interval.ms",
            "offset.storage.replication.factor",
            "offset.storage.topic",
            "plugin.path",
            "producer.security.protocol",
            "producer.ssl.key.password",
            "producer.ssl.keystore.location",
            "producer.ssl.keystore.password",
            "producer.ssl.truststore.location",
            "rest.advertised.host.name",
            "rest.advertised.port",
            "security.protocol",
            "ssl.key.password",
            "ssl.keystore.location",
            "ssl.keystore.password",
            "ssl.truststore.location",
            "status.storage.replication.factor",
            "status.storage.topic",
            "value.converter",
            "value.converter",
            "value.converter.schema.registry.url",
            "value.converter.schemas.enable"
    };

    private static int RETRY_COUNT = 0;

    private static final Logger LOGGER = Logger.getLogger(KafkaConnectRunner.class);

    public KafkaConnectRunner() {
    }

    void onStart(@Observes StartupEvent ev) {
        LOGGER.info("The application is starting...");
        start();
    }

    void onStop(@Observes ShutdownEvent ev) {
        LOGGER.info("The application is stopping...");
    }

    private void start() {
        LOGGER.info("On start...");
        Map<String, String> propertiesMap = new HashMap<>();

        Arrays.stream(KAFKA_DISTRIBUTED_CONFIG_PROPERTIES)
                .forEach(key -> checkAndAddProperties(propertiesMap, key));

        LOGGER.info("About to startConnect()...");
        Connect connect = new ConnectDistributed().startConnect(propertiesMap);

        while (!connect.isRunning()) {
            if (RETRY_COUNT >= 12) {
                throw new RuntimeException("Kafka Connect failed to start...");
            }
            LOGGER.info("Kafka not yet started. Waiting...");
            try {
                RETRY_COUNT++;
                SECONDS.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        LOGGER.info("Kafka Connect started...");
    }

    private void checkAndAddProperties(Map<String, String> propertiesMap, String key) {
        Optional<String> propertyValue =
                ConfigProvider.getConfig().getOptionalValue("kafka-distributed." + key, String.class);
        propertyValue.ifPresent(s -> propertiesMap.put(key, s));
    }
}
