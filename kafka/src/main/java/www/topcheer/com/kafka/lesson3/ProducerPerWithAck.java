package www.topcheer.com.kafka.lesson3;

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
 * 功能描述:
 * 修订历史:
 * 版本: 1.0.0
 * 作者: huyn
 * 邮箱: huyining18@163.com
 * 日期:2018/5/6 11:51
 */
public class ProducerPerWithAck {
    private final static Logger LOGGER = LoggerFactory.getLogger(ProducerPerWithAck.class);

    public static void main(String[] args) {
        Properties properties = initProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        long start = System.currentTimeMillis();
        IntStream.range(0, 20000).forEach(i ->
        {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("ack", String.valueOf(i), "hello " + i);
            Future<RecordMetadata> future = producer.send(record);
            try {
                RecordMetadata metaData = future.get();
                LOGGER.info("The message is send done and the key is {},offset {}", i, metaData.offset());

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
        producer.flush();
        producer.close();
        long current = System.currentTimeMillis();
        LOGGER.info("total spend {} ms", (current - start));
    }

    /**
     * acks=   24620 ms
     * acks=1  52430  ms
     * acks=all 130077ms
     * acks=all+gzip 137292ms
     * acks=all+snappy122387 ms
     * acks=all+gzip 140043ms
     *
     * @return
     */
    private static Properties initProps() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.2.80:9092,192.168.2.81:9092,192.168.2.82:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
       //props.put("compression.type", "gzip");
        props.put("compression.type", "snappy");
        props.put("acks", "all");
        return props;
    }


}
