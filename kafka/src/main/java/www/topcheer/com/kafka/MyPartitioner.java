package www.topcheer.com.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 功能描述:
 * 修订历史:
 * 版本: 1.0.0
 * 作者: huyn
 * 邮箱: huyining18@163.com
 * 日期:2018/5/6 22:19
 */
public class MyPartitioner implements Partitioner {

    private final String LOGIN = "LOGIN";
    private final String LOGOFF = "LOGOFF";
    private final String ORDER = "ORDER";


    @Override
    public int partition(String s, Object key, byte[] bytes, Object value, byte[] keyBytes, Cluster cluster) {
      if (keyBytes ==null || keyBytes.length ==0){
          throw  new IllegalArgumentException("this key  is required for BIZ");
      }
      switch (key.toString().toUpperCase()){
          case LOGIN:
              return 0;
          case LOGOFF:
              return 1;
          case ORDER:
              return 2;
              default:
                  throw  new IllegalArgumentException("the key is invalid");
      }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
