package son.vu.kafka.apps.v2;

import lombok.extern.slf4j.Slf4j;
import son.vu.avro.domain.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

@Slf4j
public class KafkaAvroJavaProducerV2Demo {

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

        Producer<String, Customer> producer = new KafkaProducer<>(properties);

        String topic = "customer-avro";
        int countDown = 0;
        int loop = 2000;
        Scanner sc = new Scanner(System.in);

        do {
            String uuid = UUID.randomUUID().toString();
            String generatedStringOne = "uuid = " + uuid;
            String generatedStringTwo = "uuid = " + uuid;

            int min = 10;
            int max = 100;
            int b = (int)(Math.random()*(max-min+1)+min);
            int c = (int)(Math.random()*(max-min+1)+min);
            // copied from avro examples
            Customer customer = Customer.newBuilder()
                    .setAge(new SplittableRandom().nextInt(0, 100))
                    .setFirstName(generatedStringTwo+"_"+countDown)
                    .setLastName(generatedStringOne+"_"+countDown)
                    .setHeight(b)
                    .setWeight(c)
                    .setEmail( generatedStringOne + "@gmail.com")
                    .setPhoneNumber("(123)-456-7890"+countDown)
                    .build();

            ProducerRecord<String, Customer> producerRecord = new ProducerRecord<String, Customer>(
                    topic, customer
            );

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
