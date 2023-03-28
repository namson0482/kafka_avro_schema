package son.vu.kafka.apps.v1;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import son.vu.avro.domain.SaleDetail;

import java.util.Properties;

@Slf4j
public class KafkaSaleProducer {

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

        Producer<String, SaleDetail> producer = new KafkaProducer<>(properties);

        String topic = "saledetail-avro";

        int i = 1;
        // copied from avro examples
        do {
            SaleDetail saleDetail = SaleDetail.newBuilder()
                    .setSaleDate("22/10/2022")
                    .setProductName("Apple Macbook")
                    .setSalesUnits(760)
                    .setSalesRevenue(17000812f)
                    .setStoreName("Thế giới di động")
                    .build();

            ProducerRecord<String, SaleDetail> producerRecord = new ProducerRecord<>(topic, saleDetail);
            log.info(saleDetail.toString());
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
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
