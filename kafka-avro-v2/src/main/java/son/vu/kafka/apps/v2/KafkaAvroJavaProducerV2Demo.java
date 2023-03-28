package son.vu.kafka.apps.v2;

import son.vu.avro.domain.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.Charset;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

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

        Producer<String, Customer> producer = new KafkaProducer<String, Customer>(properties);

        String topic = "customer-avro";
        int j = 0;

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
                    .setAge(34)
                    .setFirstName(generatedStringTwo)
                    .setLastName(generatedStringOne)
                    .setHeight(b)
                    .setWeight(c)
                    .setEmail( generatedStringOne + "@gmail.com")
                    .setPhoneNumber("(123)-456-7890")
                    .build();

            ProducerRecord<String, Customer> producerRecord = new ProducerRecord<String, Customer>(
                    topic, customer
            );

            System.out.println(customer);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata);
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
            j++;
            Thread.sleep(2);
        } while (j<2000);

        producer.flush();
        producer.close();

    }
}
