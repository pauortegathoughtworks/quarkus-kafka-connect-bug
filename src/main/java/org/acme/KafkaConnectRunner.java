package org.acme;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

// if using Quarkus 2.16.12.Final
//import javax.enterprise.context.ApplicationScoped;
//import javax.enterprise.event.Observes;
//import javax.inject.Inject;

// if using Quarkus 3.6.8
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import java.util.*;

import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.runtime.Connect;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;
//import uk.co.mo.services.ola.utils.PrivateInformationConfigurationReader;

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

    @Inject
    @RestClient
    KafkaConnectorRestClient kafkaConnectorRestClient;

    private static final Logger LOGGER = Logger.getLogger(KafkaConnectRunner.class);

//    @Inject
//    PrivateInformationConfigurationReader privateInformationReader;

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

        Map<String, String> propertiesMap = new HashMap<>();

        Arrays.stream(KAFKA_DISTRIBUTED_CONFIG_PROPERTIES)
                .forEach(key -> checkAndAddProperties(propertiesMap, key));

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

        Map<String, String> connectConfig = new HashMap<>();
        connectConfig.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        connectConfig.put("tasks.max", "1");
        connectConfig.put("database.hostname", getPropertyValueAsString("database.hostname"));
        connectConfig.put("database.port", getPropertyValueAsString("database.port"));
        connectConfig.put("database.user", getPropertyValueAsString("database.user"));
        connectConfig.put("database.password", getPropertyValueAsString("database.password"));
        connectConfig.put("database.dbname", getPropertyValueAsString("database.dbname"));
        connectConfig.put("publication.autocreate.mode", "filtered");
        connectConfig.put("topic.creation.enable", "true");
        connectConfig.put("plugin.name", "pgoutput");
        connectConfig.put(
                "topic.creation.default.cleanup.policy",
                getPropertyValueAsString("kafka-distributed.cleanup.policy"));
        connectConfig.put(
                "topic.creation.default.retention.ms",
                getPropertyValueAsString("kafka-distributed.retention.ms"));
        connectConfig.put("topic.creation.default.compression.type", "lz4");
//        putConvertersConfig(connectConfig);

        registerConnector(connectConfig);
    }

    private void registerConnector(Map<String, String> connectConfig) {
        // For CDC
        Map<String, String> cdcConfig = getCdcDebeziumConfig(connectConfig);

        try {
            await()
                    .atMost(600, SECONDS)
                    .pollInterval(1000, MILLISECONDS)
                    .until(
                            () -> {
                                try {
                                    LOGGER.info("Trying to configure CDC");
                                    kafkaConnectorRestClient.configureKafkaDataCdcConnector(cdcConfig);
                                    System.out.println(cdcConfig.size());
                                    LOGGER.info("Kafka Data Connector configured..");
                                    return true;
                                } catch (Exception e) {
                                    LOGGER.error("Error trying to configure CDC", e);
                                    return false;
                                }
                            });
        } catch (Exception e) {
            LOGGER.error("Failed to configure debezium, try manual configuration..", e);
        }
    }

    private void checkAndAddProperties(Map<String, String> propertiesMap, String key) {
        Optional<String> propertyValue =
                ConfigProvider.getConfig().getOptionalValue("kafka-distributed." + key, String.class);
        propertyValue.ifPresent(s -> propertiesMap.put(key, s));
    }

    private String getPropertyValueAsString(String key) {
        try {
            Optional<String> propertyValue =
                    ConfigProvider.getConfig().getOptionalValue(key, String.class);
            if (propertyValue.isPresent() && !propertyValue.get().trim().isEmpty()) {
                return propertyValue.get().trim();
            } else {
                return null;
            }
        } catch (Exception ex) {
            return null;
        }
    }

    private Boolean getPropertyValueAsBoolean(String key) {
        return Optional.ofNullable(getPropertyValueAsString(key))
                .map(value -> value.equalsIgnoreCase("true"))
                .orElse(Boolean.FALSE);
    }

    private void putConvertersConfig(Map<String, String> connectConfig) {
        // converters order is very important, only one converter per column is applied
        connectConfig.put("converters", "yearextractor,stringdate,postcodeconverter");
        connectConfig.put("stringdate.type", "uk.co.mo.debezium.converter.DateConverter");
        connectConfig.put("stringdate.format", "yyyy-MM-dd");

        connectConfig.put("yearextractor.type", "uk.co.mo.debezium.converter.YearExtractor");
//        connectConfig.put("yearextractor.columns", privateInformationReader.readColumnsToExtractYear());

        connectConfig.put("postcodeconverter.type", "uk.co.mo.debezium.converter.PostcodeConverter");
//        connectConfig.put(
//                "postcodeconverter.columns", privateInformationReader.readColumnsToConvertPostCode());
    }

    private Map<String, String> getCdcDebeziumConfig(Map<String, String> connectConfig) {
        Map<String, String> cdcConfig = new HashMap<>();
        cdcConfig.putAll(connectConfig);
        cdcConfig.put("topic.prefix", "kafka_data_connector_db_cdc_dc");
        cdcConfig.put("slot.name", "cdc_slot");
        cdcConfig.put("heartbeat.interval.ms", "3000");
        cdcConfig.put(
                "topic.creation.default.replication.factor",
                getPropertyValueAsString("cdc.topic.creation.default.replication.factor"));
        cdcConfig.put(
                "topic.creation.default.partitions",
                getPropertyValueAsString("cdc.topic.creation.default.partitions"));
        cdcConfig.put("table.include.list", getPropertyValueAsString("cdc.tables.list"));
        cdcConfig.put("decimal.handling.mode", "double");
        Boolean isAdhocSnapshotEnabled = getPropertyValueAsBoolean("cdc.adhoc-snapshot.enable");
        if (isAdhocSnapshotEnabled) {
            String signalTable = getPropertyValueAsString("cdc.adhoc-snapshot.table");
            String cdcTableList = getPropertyValueAsString("cdc.tables.list");
            cdcConfig.put("table.include.list", format("%s,%s", signalTable, cdcTableList));
            cdcConfig.put("signal.data.collection", signalTable);
        }
//        String columnsToExclude = privateInformationReader.readColumnsToExclude();
        String columnsToExclude = "";
        if (Objects.nonNull(columnsToExclude)) {
            cdcConfig.put("column.exclude.list", columnsToExclude);
            LOGGER.info("Debezium column exclude configured..");
        }
        String columnsToTransform = getPropertyValueAsString("cdc.column.transform.list");
        if (Objects.nonNull(columnsToTransform)) {
            cdcConfig.put("transforms", "RenameField");
            cdcConfig.put(
                    "transforms.RenameField.type", "uk.co.mo.debezium.transform.RenameColumn$Value");
            cdcConfig.put("transforms.RenameField.renames", columnsToTransform);
            String topicsToTransform = getPropertyValueAsString("cdc.column.transform.topics.filter");
            if (Objects.nonNull(topicsToTransform)) {
                cdcConfig.put("transforms.RenameField.topics", topicsToTransform);
            }
            LOGGER.info("CDC Debezium column transformation configured..");
        }

        return cdcConfig;
    }
}
