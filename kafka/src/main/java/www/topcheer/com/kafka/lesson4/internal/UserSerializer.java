package www.topcheer.com.kafka.lesson4.internal;

import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 功能描述:
 * 修订历史:
 * 版本: 1.0.0
 * 作者: huyn
 * 邮箱: huyining18@163.com
 * 日期:2018/5/11 8:40
 */
public class UserSerializer  implements Serializer<User> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, User user) {
        if (user ==null)
            return  null;
        int id = user.getId();
        String name = user.getName();
        String address = user.getAddress();

        byte[] nameBytes;
        byte[] addrBytes;
        if(name != null)
            nameBytes = name.getBytes();
        else
            nameBytes = new byte[0];
        if (address != null)
            addrBytes = address.getBytes();
        else
            addrBytes = new byte[0];
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + nameBytes.length + 4 + addrBytes.length);
        buffer.putInt(id);
        buffer.putInt(nameBytes.length);
        buffer.put(nameBytes);
        buffer.putInt(addrBytes.length);
        buffer.put(addrBytes);
        return buffer.array();
    }

    @Override
    public void close() {

    }
}
