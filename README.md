# Content: Example Avro Schema cooperate with Schema Registry Server.

 - Avro examples
 - Kafka Avro Producer & Consumer
 - Schema Evolutions

### Setup Kafka, Zookeeper, Schema Registry and Control-Center
- Kafka: Broker service
- Zookeeper is a centralized service for maintaining configuration information, naming, providing distributed synchronization, and providing group services.
- Schema Registry manage the schema.
- Control-Center: Kafka management.
```
docker-compose up -d
```
- Execute a command 
```
mvn clean install -DskipTests
```
- Run KafkaAvroJavaConsumerV1Demo.java
- Run KafkaAvroJavaConsumerV1Demo.java
- Get all subjects from Registry server
```
brew install jq
curl --silent -X GET http://localhost:8081/subjects | jq
```
- Get a schema by id
```
curl -X GET http://localhost:8081/schemas/ids/1 | jq
```

**DONE**
