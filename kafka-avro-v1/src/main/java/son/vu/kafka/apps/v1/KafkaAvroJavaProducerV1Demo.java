package son.vu.kafka.apps.v1;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import son.vu.avro.domain.Customer;

import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.SplittableRandom;

@Slf4j
public class KafkaAvroJavaProducerV1Demo {

    static final String [] FIRSTNAMES = {"Tom","Mary",
            "Chris", "Thomas", "Wa", "Queue", "Wash", "Putin",
            "Luna", "Lost", "Tiki", "Kiki", "Wawa", "Pepe",
            "Frank", "Sat"};

    static final String [] SURNAMES = {"Vu","Nguyen",
            "Del", "Tee", "Tea", "Green", "Blue", "Red",
            "Luna", "Lost", "Tiki", "Kiki", "Wawa", "Pepe",
            "Frank", "Sat"};
    static final Random RAND_OBJECT = new Random();

    static final String TOPIC = "customer-avro";

    String getFirstName() {
        int randItem = RAND_OBJECT.nextInt(FIRSTNAMES.length);
        return FIRSTNAMES[randItem];
    }

    String getSurName() {
        int randItem = RAND_OBJECT.nextInt(SURNAMES.length);
        return SURNAMES[randItem];
    }

    void sendMessage() throws InterruptedException {
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
        int countDown = 0;
        int loop = 2000;
        Scanner sc = new Scanner(System.in);
        // copied from avro examples
        do {
            Customer customer = Customer.newBuilder()
                    .setAge(new SplittableRandom().nextInt(0, 100))
                    .setAutomatedEmail(false)
                    .setFirstName(getFirstName())
                    .setLastName(getSurName())
                    .setHeight(new SplittableRandom().nextInt(10, 200))
                    .setWeight(new SplittableRandom().nextInt(10, 200))
                    .build();

            ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(TOPIC, customer);
            log.info(customer.toString());
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    log.info(metadata.toString());
                } else {
                    exception.printStackTrace();
                }
            });
            countDown++;
            Thread.sleep(10);
            if(countDown == loop) {
                System.out.print("Enter value for countDown: ");
                String value = sc.next();
                if(value.trim().equalsIgnoreCase("exit")) {
                    countDown = loop;
                } else {
                    try {
                        loop = Integer.parseInt(value);
                    } catch (Exception e) {
                        loop = 2000;
                    }
                    countDown = 0;
                }
            }
        } while (countDown<loop);
        producer.flush();
        producer.close();
    }

    public static void main(String[] args)  {
        try {
            new KafkaAvroJavaProducerV1Demo().sendMessage();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
