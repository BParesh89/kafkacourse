kafka-topics.sh --create --zookeeper localhost:2181 --topic hello-producer-2 --partitions 5 --replication-factor 3 --config min.insync.replicas=2