package www.topcheer.com.kafka.lesson4;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

/**
 * 功能描述: 同步提交消息
 * 修订历史:
 * 版本: 1.0.0
 * 作者: huyn
 * 邮箱: huyining18@163.com
 * 日期:2018/5/5 20:52
 */
public class SyncSender {

    private final static Logger LOGGER = LoggerFactory.getLogger(SyncSender.class);

    public static void main(String[] args) {
        Properties properties = initPropes();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        IntStream.range(10,20).forEach(i->{ //file_and_forget_sender
            ProducerRecord<String, String> record = new ProducerRecord<>("test14", String.valueOf(i), "hello" + i);
            Future<RecordMetadata> future = producer.send(record);

            try {
                RecordMetadata metadata = future.get(); //获取元数据
                LOGGER.info("The  message is send done and the key is{}, offset {},patitions {}", i, metadata.offset(), metadata.partition());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
    }

    public static Properties initPropes() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","192.168.2.80:9092,192.168.2.81:9092,192.168.2.82:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("ack","all");
        return  properties;
    }
}
