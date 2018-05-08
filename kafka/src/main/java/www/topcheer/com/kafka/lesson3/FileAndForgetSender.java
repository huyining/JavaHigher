package www.topcheer.com.kafka.lesson3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
 * 日期:2018/5/4 22:04
 */
public class FileAndForgetSender {

    private final static Logger LOOGGER = LoggerFactory.getLogger(FileAndForgetSender.class);

    public static void main(String[] args) {
        Properties properties = initProps();
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);
        IntStream.range(0,10).forEach(i->{
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("file_and_forget_sender",String.valueOf(i),"hello"+i);
            kafkaProducer.send(producerRecord);
            LOOGGER.info("The message is send done and the key is {}"+i);
        });
        kafkaProducer.flush();;
        kafkaProducer.close();


    }


    private static Properties initProps() {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.2.80:9092,192.168.2.81:9092,192.168.2.82:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }


}


