package www.topcheer.com.kafka.lesson4;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import www.topcheer.com.kafka.lesson4.internal.User;

import java.util.Collections;
import java.util.Properties;


public class CustomSerializerAndDeserializer
{
    private static final Logger LOG = LoggerFactory.getLogger(CustomSerializerAndDeserializer.class);


    public static void main(String[] args)
    {
        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("test14"));


        for (; ; )
        {
            ConsumerRecords<String, User> records = consumer.poll(100);
            records.forEach(record ->
            {
                //biz handler.
                LOG.info("key:{},value:{}", record.key(), record.value());
            });
        }
    }

    private static Properties loadProp()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.2.80:9092,192.168.2.81:9092,192.168.2.81:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "www.topcheer.com.kafka.lesson4.internal.UserDeserializer");
        props.put("group.id", "g1");
        props.put("client.id", "customer-ser");
        props.put("auto.offset.reset", "earliest");
        return props;
    }
}