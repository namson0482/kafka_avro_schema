# Content

 - Avro examples
 - Kafka Avro Producer & Consumer
 - Schema Evolutions

### Setup
- Install kafka, registry server
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

