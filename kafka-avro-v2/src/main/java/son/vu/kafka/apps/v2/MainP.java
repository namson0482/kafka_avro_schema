package son.vu.kafka.apps.v2;

import son.vu.avro.domain.User;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.io.FileReader;
import java.util.List;
import java.util.Properties;


@Slf4j
public class MainP {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", IntegerSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        String topic = "excel";

        KafkaProducer<Integer, User> producer = new KafkaProducer<>(properties);
        try {
            FileReader filereader = new FileReader("src/main/resources/C2ImportCalEventSample.csv");
            CSVReader csvReader = new CSVReaderBuilder(filereader)
                    .withSkipLines(1)
                    .build();
            List<String[]> allData = csvReader.readAll();
            int k = 0;
            do {
                for (String[] row : allData) {
                    User user = User.newBuilder()
                            .setStartTime(row[1])
                            .setEndDate(row[2])
                            .setEndTime(row[3])
                            .setEventTitle(row[4])
                            .setAllDayEvent(row[5])
                            .setNoEndTime(row[6])
                            .setEventDescription(row[7])
                            .build();
                    int id = Integer.parseInt(row[0]);
                    ProducerRecord<Integer, User> pr = new ProducerRecord<Integer, User>(topic, id, user);
                    RecordMetadata metadata = producer.send(pr).get();
                    log.info(metadata.toString());
                    Thread.sleep(10);
                }
            } while(k++<100);

        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.flush();
        producer.close();
    }
}
