package www.topcheer.com.kafka.lesson4;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;


/**
 * 功能描述:
 * 修订历史:
 * 版本: 1.0.0
 * 作者: huyn
 * 邮箱: huyining18@163.com
 * 日期:2018/5/10 21:48
 */
public class CommitOffsetBothAsynSync {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommitOffsetBothAsynSync.class);

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(initPropes());
        consumer.subscribe(Collections.singletonList("test12"));
        try {
            for(; ;) {
                ConsumerRecords<String, String> recoders = consumer.poll(100);
                recoders.forEach(recoder->{
                    LOGGER.info("offset:{}",recoder.offset());
                    LOGGER.info("value:{}",recoder.value());
                    LOGGER.info("key:{}",recoder.key());
                });
                consumer.commitAsync(); //异步
            }
        } finally {
            consumer.commitSync(); //同步
        }
    }



    public static Properties initPropes() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","192.168.2.80:9092,192.168.2.81:9092,192.168.2.82:9092");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test_group9");
        properties.put("client.id","demo-commit-consumer-client");
        /**
         * earliest: 当各分区下有已经提交的offset时, 从提交的offset开始消费;
         *           无提交的offset时, 从头开始消费.
         * latest:  当各分区下有已经提交的offset时,从提交的offset开始消费;
         *          无提交offset的时候,消费新产生该分区下的数据.
         * none:  topic各分区都存在已提交的offset时,从offset后开始消费;
         *         只要有一个分区不存在已经提交的offset,则抛出异常.
         */
        properties.put("auto.offset.reset","earliest");
        properties.put("enable.auto.commit","false");
        return properties;
    }
}
