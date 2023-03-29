package son.vu.kafka.apps.v1;

import lombok.extern.slf4j.Slf4j;
import son.vu.avro.domain.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;
import java.util.SplittableRandom;

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

        int countDown = 0;
        int loop = 2000;
        Scanner sc = new Scanner(System.in);
        // copied from avro examples
        do {
            Customer customer = Customer.newBuilder()
                    .setAge(new SplittableRandom().nextInt(0, 100))
                    .setAutomatedEmail(false)
                    .setFirstName("John " + countDown)
                    .setLastName("Doe " + countDown)
                    .setHeight(178f + countDown)
                    .setWeight(75f + countDown)
                    .build();

            ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(topic, customer);
            log.info(customer.toString());
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    log.info(metadata.toString());
                } else {
                    exception.printStackTrace();
                }
            });
            countDown++;
            Thread.sleep(1);
            if(countDown == loop) {
                System.out.print("Enter value for countDown: ");
                String value = sc.next();
                if(value.trim().equalsIgnoreCase("exit")) {
                    countDown = loop;
                } else {
                    try {
                        loop = Integer.parseInt(value);
                    } catch (Exception e) {

                    }
                    countDown = 0;
                }
            }
        } while (countDown<loop);

        producer.flush();
        producer.close();

    }
}
