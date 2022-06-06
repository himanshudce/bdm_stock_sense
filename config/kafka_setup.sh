
# 1. start zookeeper server on one terminal
# zookeper default port 2181
# kafka_2.13-3.1.0/bin/zookeeper-server-start.sh kafka_2.13-3.1.0/config/zookeeper.properties
BDM_Software/kafka_2.13-3.1.0/bin/zookeeper-server-start.sh BDM_Software/kafka_2.13-3.1.0/config/zookeeper.properties

# 2. start kafka server on another terminal
# kafka default port 2181
# kafka_2.13-3.1.0/bin/kafka-server-start.sh kafka_2.13-3.1.0/config/server.properties
BDM_Software/kafka_2.13-3.1.0/bin/kafka-server-start.sh BDM_Software/kafka_2.13-3.1.0/config/server.properties


# 3. create a topic twitter if not created
BDM_Software/kafka_2.13-3.1.0/bin/kafka-topics.sh --create --topic twitter --bootstrap-server localhost:9092


# =============================================================================
# checking functions for kafka

# 4. To check if kafka is running
kafka_2.13-3.1.0/bin/kafka-topics.sh --describe --topic twitter --bootstrap-server localhost:9092


# 5. kafka check messages from begining
kafka_2.13-3.1.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter --from-beginning



# =================== errors resolutions if any ======================
# to kill any service list all - sudo lsof -iTCP -sTCP:LISTEN -n -P
# check its process ID and kill - sudo kill -9 <PID>
# remove the lock over kafka pipeline if get an error - rm /tmp/kafka-logs/.lock

