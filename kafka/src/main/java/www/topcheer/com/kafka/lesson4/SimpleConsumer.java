package www.topcheer.com.kafka.lesson4;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 功能描述:
 * 修订历史:
 * 版本: 1.0.0
 * 作者: huyn
 * 邮箱: huyining18@163.com
 * 日期:2018/5/8 21:43
 */
public class SimpleConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumer.class);


    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("test_c"));

        final AtomicInteger counter = new AtomicInteger();

        for (; ; ){
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record->{
                //biz handler
                LOGGER.info("offset:{}",record.offset());
                LOGGER.info("value:{}",record.value());
                LOGGER.info("key:{}",record.key());

                int cnt = counter.incrementAndGet();
                if (cnt >=3){
                    Runtime.getRuntime().halt(-1);
                }
            });
        }
    }

    private  static Properties loadProp() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","192.168.2.80:9092,192.168.2.81:9092,192.168.2.82:9092");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test_group");
        properties.put("client.id","demo-consumer-client");
        properties.put("auto.offset.reset","earliest");
        properties.put("auto.commit.interval.ms","10000");
        return properties;
    }
}
