#create topic maxwell-sink-configs
bin/kafka-topics.sh --create --zookeeper node1:2181,node2:2181,node3:2181  --replication-factor 5 --partitions 1 --config compression.type=gzip --topic maxwell-sink-configs
#create topic maxwell-sink-offsets
bin/kafka-topics.sh --create --zookeeper node1:2181,node2:2181,node3:2181  --replication-factor 3 --partitions 5 --config compression.type=gzip --topic maxwell-sink-offsets
#create topic maxwell-sink-status
bin/kafka-topics.sh --create --zookeeper node1:2181,node2:2181,node3:2181  --replication-factor 3 --partitions 15 --config compression.type=gzip --topic maxwell-sink-status
