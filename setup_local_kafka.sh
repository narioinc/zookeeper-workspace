#start kafka single node
cd ./kafka-docker
docker run -d -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=localhost --env ADVERTISED_PORT=9092 spotify/kafka

sleep 15s

#setup topics
cd ../kafka_bin/bin/
./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic source-topic
./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sink-topic

#check topics
./kafka-topics.sh --list --bootstrap-server localhost:9092


