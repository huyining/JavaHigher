package www.topcheer.com.kafka;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * 类描述: MessageSerializer
 * 功能描述:
 * 修订历史:
 * 版本: 1.0.0
 * 作者: huyn
 * 日期: 2018/5/6 14:44
 */
public class MessageSerializer implements Serializer<Message>
{
    @Override
    public void configure(Map<String, ?> map, boolean b)
    {

    }

    @Override
    public byte[] serialize(String s, Message message)
    {
        if (null == message)
            return null;

        int id = message.getId();
        String name = message.getName();
        int nameLength = 0;
        if (name != null && !name.isEmpty())
        {
            nameLength = name.length();
        }

        return new byte[0];
    }

    @Override
    public void close()
    {

    }
}
