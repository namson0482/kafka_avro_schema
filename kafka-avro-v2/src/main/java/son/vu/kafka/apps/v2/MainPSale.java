package son.vu.kafka.apps.v2;

import son.vu.avro.domain.SaleDetail;
import com.opencsv.CSVReader;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


@Slf4j
public class MainPSale {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        String topic = "saledetail";

        KafkaProducer<String, SaleDetail> producer = new KafkaProducer<>(properties);
        int primaryKey = 1;

        CSVReader csvReader = new CSVReader(new FileReader("src/main/resources/10.psv"), '|', '\0', 1);

        List<String[]> allData = csvReader.readAll();
        for (String[] values : allData) {
            SaleDetail saleDetail = SaleDetail.newBuilder()
                    .setSaleDate(values[0])
                    .setStoreName(values[1])
                    .setProductName(values[2])
                    .setSalesUnits(Integer.parseInt(values[3]))
                    .setSalesRevenue(Float.parseFloat(values[4]))
                    .build();
            String id = values[1]+ "_" + values[2] + "_" + primaryKey;
            ProducerRecord<String, SaleDetail> pr = new ProducerRecord<>(topic, id, saleDetail);
            RecordMetadata metadata = producer.send(pr).get();
            log.info(metadata.toString());
            Thread.sleep(10);
        }

        producer.flush();
        producer.close();
    }
}
