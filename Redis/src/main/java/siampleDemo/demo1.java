package siampleDemo;

import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Created by HuShiwei on 2016/8/11 0011.
 * 单机无连接池方式
 */
public class demo1 {
    public static void main(String[] args) {
        String ip = "192.168.4.202";
        int port = 6379;
        final Jedis jedis = new Jedis(ip, port);
        final List<String> list = jedis.lrange("externalService.mysqljob.mysqlnode.outputData", 0, -1);
        for (String s : list) {
            System.out.println(s);
        }
    }
}
