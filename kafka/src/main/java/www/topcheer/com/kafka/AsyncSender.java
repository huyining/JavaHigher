package www.topcheer.com.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

/**
 * 功能描述:
 * 修订历史:
 * 版本: 1.0.0
 * 作者: huyn
 * 邮箱: huyining18@163.com
 * 日期:2018/5/5 21:43
 */
public class AsyncSender {
    private final static Logger LOGGER = LoggerFactory.getLogger(AsyncSender.class);

    public static void main(String[] args) {
        Properties properties = initProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        IntStream.range(0,10).forEach(i->{
            ProducerRecord<String, String> record = new ProducerRecord<>("file_and_forget_sender", String.valueOf(i), "hello" + i);
            producer.send(record, (r,e)->{
               if (e==null){
                    LOGGER.info("The message is send done and the key is{},offset{}",i,r.offset());
               }
            });


        });

    }

    private static Properties initProps() {
        final Properties props = new Properties();
        props.put("bootstrap.servers","192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
