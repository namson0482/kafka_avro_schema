package son.vu.kafka.apps.v1;

import lombok.extern.slf4j.Slf4j;
import son.vu.avro.domain.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class KafkaAvroJavaProducerV1Demo {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        Producer<String, Customer> producer = new KafkaProducer<String, Customer>(properties);

        String topic = "customer-avro";

        int i = 1;
        // copied from avro examples
        do {
            Customer customer = Customer.newBuilder()
                    .setAge(34 + i)
                    .setAutomatedEmail(false)
                    .setFirstName("John " + i)
                    .setLastName("Doe " + i)
                    .setHeight(178f + i)
                    .setWeight(75f + i)
                    .build();

            ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(topic, customer);

//            System.out.println(customer);
            log.info(customer.toString());
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
//                        System.out.println(metadata);
                    log.info(metadata.toString());
                } else {
                    exception.printStackTrace();
                }
            });
            i++;
            Thread.sleep(5);
        } while (i<1500);

        producer.flush();
        producer.close();

    }
}
