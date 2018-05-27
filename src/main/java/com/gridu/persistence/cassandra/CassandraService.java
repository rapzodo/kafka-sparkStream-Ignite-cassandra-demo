package com.gridu.persistence.cassandra;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.gridu.model.BotRegistry;
import com.gridu.persistence.Repository;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class CassandraService implements Repository<BotRegistry> {
    private Logger logger = LoggerFactory.getLogger(getClass());

    public static final String KEY_SPACE = "stopbot";
    private SparkContext sc;
    public static final String CASSANDRA_HOST_PROPERTY = "localhost";
    public static final String CASSANDRA_BOT_REGISTRY_TABLE = "botregistry";
    private Session session;


    public CassandraService(SparkContext sc) {
        this.sc = sc;
        setup();
    }

    @Override
    public void setup() {
        sc.getConf().set("spark.cassandra.connection.host", CASSANDRA_HOST_PROPERTY);
        sc.getConf().set("spark.driver.allowMultipleContexts", "true");
        final CassandraConnector cassandraConnector = CassandraConnector.apply(sc.conf());
        session = cassandraConnector.openSession();
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEY_SPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute("CREATE TABLE IF NOT EXISTS " + KEY_SPACE + "." + CASSANDRA_BOT_REGISTRY_TABLE + " (ip TEXT PRIMARY KEY, url TEXT, count INT)");
    }

    @Override
    public void persist(JavaRDD<BotRegistry> botRegistryDataset) {
        logger.info(">>> PERSISTING BOTS TO CASSANDRA <<<<<<<");
        javaFunctions(botRegistryDataset)
                .writerBuilder(KEY_SPACE, CASSANDRA_BOT_REGISTRY_TABLE, mapToRow(BotRegistry.class))
                .withConstantTTL(Duration.standardSeconds(TTL)).saveToCassandra();
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
