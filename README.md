# Stream-processing-final-project
final project for stream processing engineer learning path

A spybot blocker
the system analyses event logs from partners, identifying and blacklisting suspicious Ips

Data should be ingested in kafka, consumed and processed by sparkstreaming and persist blacklisted ips

<h3>Description</h3>

- Kafka FileConnector consumes partners events files from specif directory and populates kafka topic 
- Messages are consumed by Kafka-Spark-streaming API
- a Custom JsonConverter validates, clean and parses the messagens into a Events JavaIgniteRdd
- IgniteRdd are queried and aggregated counting by ip and category and sinks an aggregated dataset ready to be scanned in order to find bots (using a filter)
- Identified bots are persisted in Cassandra/Ignite dependind on BaseDao implementation provided.
- A constante TTL is defined in order to remove bots from Blacklist 
