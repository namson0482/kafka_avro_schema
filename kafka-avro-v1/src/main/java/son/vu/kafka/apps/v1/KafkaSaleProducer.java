package son.vu.kafka.apps.v1;

import com.opencsv.CSVReader;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import son.vu.avro.domain.SaleDetail;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class KafkaSaleProducer {


    public static final String TOPIC = "saledetail-avro";

    public static final Properties properties = new Properties();

    static {
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
    }

    KafkaSaleProducer() {

    }

    private void sendPSVMessage(Properties properties) throws ExecutionException, InterruptedException, IOException {
        Producer<String, SaleDetail> producer = new KafkaProducer<>(properties);

        CSVReader csvReader = new CSVReader(new FileReader("src/main/resources/10.psv"), '|', '\0', 1);
        List<String[]> allData = csvReader.readAll();
        int primaryKey = 0;
        for (String[] values : allData) {
            SaleDetail saleDetail = SaleDetail.newBuilder()
                    .setSaleDate(values[0])
                    .setStoreName(values[1])
                    .setProductName(values[2])
                    .setSalesUnits(Integer.parseInt(values[3]))
                    .setSalesRevenue(Float.parseFloat(values[4]))
                    .build();
            String id = values[1]+ "_" + values[2] + "_" + primaryKey++;
            ProducerRecord<String, SaleDetail> pr = new ProducerRecord<>(TOPIC, id, saleDetail);
            RecordMetadata metadata = producer.send(pr).get();
            log.info(metadata.toString());
            Thread.sleep(10);
        }

        producer.flush();
        producer.close();
    }

    private void sendParticularMessage() throws InterruptedException {
        Producer<String, SaleDetail> producer = new KafkaProducer<>(properties);

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

            ProducerRecord<String, SaleDetail> producerRecord = new ProducerRecord<>(TOPIC, saleDetail);
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

    public void sendMessage() {
        try {
            KafkaSaleProducer kafkaSaleProducer = new  KafkaSaleProducer();
            kafkaSaleProducer.sendParticularMessage();
        } catch (InterruptedException e) {

        }
    }


    public static void main(String[] args) throws InterruptedException {

        new KafkaSaleProducer().sendMessage();
    }
}
