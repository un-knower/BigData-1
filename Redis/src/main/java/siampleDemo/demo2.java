package siampleDemo;

/**
 * Created by HuShiwei on 2016/8/11 0011.
 */

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;

/**
 * 单机连接池方式
 * 1.new一个JedisPoolConfig类,然后set这个连接池的一些属性.
 * 2.new一个JedisPool类,然后获取一个jedis连接池,返回一个Jedis对象
 * 3.现在有Jedis对象了,就可以对redis数据库进行操作了
 */
public class demo2 {
    public static void main(String[] args) {
        final JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

//        指定连接池中最大空闲连接数
        jedisPoolConfig.setMaxIdle(10);

//        连接池中创建的最大连接数
        jedisPoolConfig.setMaxTotal(100);

//        设置创建链接的超时时间
        jedisPoolConfig.setMaxWaitMillis(2000);

//        表示连接池在创建链接的时候会测试一下链接是否可用，这样可以保证连接池中的连接都是可用的
        jedisPoolConfig.setTestOnBorrow(true);

        String ip = "192.168.4.202";
        int port = 6379;

//        创建一个连接池
        final JedisPool jedisPool = new JedisPool(jedisPoolConfig, ip, port);

//        从连接池中获取一个链接
        final Jedis jedis = jedisPool.getResource();
        final List<String> list = jedis.lrange("externalService.mysqljob.mysqlnode.outputData", 0, -1);

        for (String s : list) {
            System.out.println(s);
        }

        jedisPool.returnResource(jedis);

    }
}
