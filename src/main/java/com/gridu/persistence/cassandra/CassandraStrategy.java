package com.gridu.persistence.cassandra;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.gridu.model.BotRegistry;
import com.gridu.persistence.PersistenceStrategy;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class CassandraStrategy implements PersistenceStrategy<BotRegistry> {
    private Logger logger = LoggerFactory.getLogger(getClass());

    public static final String KEY_SPACE = "stopbot";
    private SparkContext sc;
    public static final String CASSANDRA_HOST_PROPERTY = "localhost";
    public static final String CASSANDRA_BOT_REGISTRY_TABLE = "botregistry";
    private Session session;


    public CassandraStrategy(SparkContext sc) {
        this.sc = sc;
        setup();
    }

    @Override
    public void setup() {
        logger.info(">>>BOTS TTL {} DAYS<<<",TTL);
        sc.getConf().set("spark.cassandra.connection.host", CASSANDRA_HOST_PROPERTY);
        sc.getConf().set("spark.driver.allowMultipleContexts", "true");
        final CassandraConnector cassandraConnector = CassandraConnector.apply(sc.conf());
        session = cassandraConnector.openSession();
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEY_SPACE
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute("CREATE TABLE IF NOT EXISTS " + KEY_SPACE + "." + CASSANDRA_BOT_REGISTRY_TABLE
                + " (ip TEXT PRIMARY KEY, events BIGINT, categories BIGINT, views_Clicks_Diff FLOAT, " +
                "views BIGINT, clicks BIGINT)");
    }

    @Override
    public void persist(JavaRDD<BotRegistry> botRegistryDataset) {
        logger.info(">>> PERSISTING {} BOTS TO CASSANDRA <<<<<<<",botRegistryDataset.count());
        javaFunctions(botRegistryDataset)
                .writerBuilder(KEY_SPACE, CASSANDRA_BOT_REGISTRY_TABLE, mapToRow(BotRegistry.class))
                .withConstantTTL(Duration.standardDays(TTL)).saveToCassandra();
    }


    @Override
    public List<BotRegistry> getAllRecords() {
        return javaFunctions(sc)
                .cassandraTable(KEY_SPACE,
                        CASSANDRA_BOT_REGISTRY_TABLE,
                        mapRowTo(BotRegistry.class)).collect();
    }

    @Override
    public void cleanUp() {
        session.close();
    }
}
