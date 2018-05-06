package www.topcheer.com.kafka;

/**
 * 类描述: Message
 * 功能描述:
 * 修订历史:
 * 版本: 1.0.0
 * 作者: huyn
 * 日期: 2018/5/6 21:47
 */
public class Message
{
    private final int id;
    private final String name;

    public Message(int id, String name)
    {
        this.id = id;
        this.name = name;
    }

    public int getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }
}