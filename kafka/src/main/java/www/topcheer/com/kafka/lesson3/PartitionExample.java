package www.topcheer.com.kafka.lesson3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 功能描述:
 * 修订历史:
 * 版本: 1.0.0
 * 作者: huyn
 * 邮箱: huyining18@163.com
 * 日期:2018/5/6 21:46
 */
public class PartitionExample {

    private final static Logger LOGGER = LoggerFactory.getLogger(PartitionExample.class);


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = initProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
       /* ProducerRecord<String, String> record = new ProducerRecord<>("test_p","hello", "hello");
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata recordMetadata = future.get();
        LOGGER.info("{}", recordMetadata.partition());


        new ProducerRecord<>("test_p", "hello","hello");
        future = producer.send(record);
        recordMetadata = future.get();*/

        ProducerRecord<String, String> record = new ProducerRecord<>("test_p", "xxx", "hello");
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata recordMetadata = future.get();
        LOGGER.info("{}", recordMetadata.partition());

        producer.flush();
        producer.close();
    }


    private static Properties initProps() {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.2.80:9092,192.168.2.81:9092,192.168.2.82:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("partitioner.class","www.topcheer.com.kafka.lesson3.MyPartitioner");
        return properties;
    }

}
