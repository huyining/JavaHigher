package www.topcheer.com.kafka.lesson4.internal;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import javax.sql.rowset.serial.SerialException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 功能描述:
 * 修订历史:
 * 版本: 1.0.0
 * 作者: huyn
 * 邮箱: huyining18@163.com
 * 日期:2018/5/11 12:58
 */
public class UserDeserializer implements Deserializer<User>{
    @Override
    public void configure(Map<String,?> map, boolean b) {
        //do noting
    }

    @Override
    public User deserialize(String s, byte[] date) {
        if (date == null)
        return null;
        if (date.length < 12)
            throw  new SerializationException("The User date bytes length should not be less than 12");
        ByteBuffer buffer = ByteBuffer.wrap(date);
        int id = buffer.getInt();
        int nameLength = buffer.getInt();
        byte[] nameBytes = new byte[nameLength];
        buffer.get(nameBytes);
        String name = new String(nameBytes);

        int addrLength = buffer.getInt();
        byte[] addressBytes = new byte[addrLength];
        buffer.get(addressBytes);
        String address = new String(addressBytes);
        return new User(id,name,address);


    }

    @Override
    public void close() {

    }
}
