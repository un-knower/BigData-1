package com.bigdata.kafka.redismsg;

import com.bigdata.kafka.util.ConfigFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.*;

/**
 * 引入sentinel 后，加入对redis HA的支持
 * 考虑到偶尔出ClassCastException, 方法加上同步
 * 再加上读写分离，读访问slave，写访问master
 * @author tao
 */
public class RedisClusterUtils {
	private JedisSentinelPool pool;
	private Set<String> sentinels ;  
	
	//为减轻读的压力，只读用
	private JedisPool[] pools4read;
	//debug用
//	private String[] poolNames4read;
	private int count;//自增的一个数
	private final static long DEFAULT_TIMEOUT=5000;//超时
	private long timeout;
	private String masterName="mymaster";//sentinel配置的master名字，缺省为mymaster
	private GenericObjectPoolConfig poolConfig=new GenericObjectPoolConfig();
	
	//读取配置文件中的url
	public RedisClusterUtils( long timeout) {
		this.timeout = timeout;
		String sentinelUrl = ConfigFactory.getString("redis.url");
		initSentine( sentinelUrl  );
		initMaster();
		makePool4Read();
		// logger.info("init OK");
	}
	
	//使用提供的url连接Redis
	public RedisClusterUtils(String sentinelUrl, long timeout) {
		this.timeout = timeout;
		initSentine( sentinelUrl );
		initMaster();
		makePool4Read();
		// logger.info("init OK");
	}

	public void initSentine(String sentinelUrl ){
		String[] servers = sentinelUrl.split(",");
		sentinels = new HashSet<String>();
		for (String server : servers) {
			if (!server.endsWith(":26379")) {// 没有带端口
				server += ":26379";
			}
			sentinels.add(server);
			// logger.info("add sentinel:{}",server);
		}
	}
	
	public RedisClusterUtils(String masterName, String sentinelUrl, int port,
			long timeout) {
		this(masterName, sentinelUrl, port, timeout,
				new GenericObjectPoolConfig());
	}
	public RedisClusterUtils(String masterName,String sentinelUrl,int port, 
			long timeout, GenericObjectPoolConfig poolConfig) {
		this.masterName = masterName;
		this.timeout = timeout;
		this.poolConfig = poolConfig;
		String[] servers = sentinelUrl.split(",");
		sentinels = new HashSet<String>();
		for (String server : servers) {
			server = server + ":" + port;
			sentinels.add(server);
		}
		initMaster();
		makePool4Read();
	}

	//使用默认的Redis地址
	public RedisClusterUtils( ) {
		this(  DEFAULT_TIMEOUT);
	}
	//使用默认的 Redis地址
	public RedisClusterUtils(String sentinelUrl ) {
		this(sentinelUrl,  DEFAULT_TIMEOUT);
	}
	public void initMaster() {
		pool = new JedisSentinelPool(masterName, sentinels, poolConfig,
				(int) timeout);// 链接超时为5s
		HostAndPort master = pool.getCurrentHostMaster(); // 获得master
		// logger.info("master:{}",master);
	}
	private  void makePool4Read() {  
        JedisPoolConfig config = new JedisPoolConfig();  
        config.setMaxWaitMillis(timeout);  
    	List<JedisPool> poolList=new ArrayList<JedisPool>();
		for (String sentinel : sentinels) {
			//轮询每个sentinel
			Jedis jedis = null;
			String[] parts = sentinel.split(":");
			String ip = parts[0];
			int port = Integer.parseInt(parts[1]);
			try {
				jedis = new Jedis(ip, port);
				// 调用其命令 SENTINEL slaves mymaster
				List<Map<String, String>> slavesList = jedis
						.sentinelSlaves(masterName);
				for (Map<String, String> slaveMap : slavesList) {
					ip = slaveMap.get("ip");
					port = Integer.parseInt(slaveMap.get("port"));
					JedisPool rpool = new JedisPool(config, ip, port,
							(int) timeout);
					poolList.add(rpool);
					// poolNames4read[i]=ip;
					// logger.info("make jedis reading pool to :{}",slaveMap.get("name"));
				}
				// 只要调用其中一个sentinel成功即可
				break;
			} catch (JedisConnectionException e) {
				// logger.warn("Cannot connect to sentinel running @ " +
				// sentinel + ". Trying next one.");
			} finally {
				if (jedis != null) {
					jedis.close();
				}
			}
		}
    	//最后获取只读的jedis集合
        pools4read=poolList.toArray(new JedisPool[0]);
	}
	/*
	private  void makePool4Read() {  
        JedisPoolConfig config = new JedisPoolConfig();  
        config.setMaxWaitMillis(timeout);  
//	            config.setTestOnBorrow(true);  
//	            config.setTestOnReturn(true);  
//        pools4read=new JedisPool[pools4read.length];
        int i=0;
    	HostAndPort master = pool.getCurrentHostMaster();  //获得master
    	String masterHost=master.getHost();
    	logger.info("master:{}",masterHost);
    	List<JedisPool> poolList=new ArrayList<JedisPool>();
    	poolNames4read=new String[3];//最多3个
        for(String server:sentinels)
        {
        	String host=server.split(":")[0];
        	String ip=Utils.getIP(host);//转换为IP比较才一致
            try{    
            	if (ip.equals(masterHost))
            	{//master只做写入，不做读取
            		continue;
            	}
            	JedisPool rpool=new JedisPool(config, host, 6379,(int)timeout);
            	poolList.add(rpool);
//            	pools4read[i] = new JedisPool(config, host, 6379,(int)timeout);  
            	poolNames4read[i]=ip;
            	logger.info("make jedis reading pool to :{}",ip);
            	i++;
            } catch(Exception e) {  
                e.printStackTrace();  
                logger.error("",e);
            }  
        }
        pools4read=poolList.toArray(new JedisPool[0]);
	}*/
	public void close() {
		if (pool != null) {
			pool.destroy();
			pool.close();
		}
		for (JedisPool rpool : pools4read) {
			if (rpool != null) {
				rpool.destroy();
				rpool.close();
			}
		}
	}
	//重新初始化
	public void reInit() {
		close();
		initMaster();
		makePool4Read();
	}
	//返回一个只读的jedis
	public Jedis getJedis4Read() {
		JedisPool pool = pools4read[count++ % pools4read.length];
		Jedis jedis = pool.getResource();
		return jedis;
	}
	/**根据键获string类型的取值
	 * @param key 键
	 * @return
	 */
	synchronized public String getValue(String key) {
		String value = null;
		Jedis jedis = null;
		int index = 0;
		try {
			// 随机选取其中一个pool
			index = count++ % pools4read.length;
			JedisPool pool = pools4read[index];
			jedis = pool.getResource();
			value = jedis.get(key);
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return value;
	}
	/**添加string类型的键值
	 * @param key	键
	 * @param value	值
	 * @param expireSec	失效时长(秒)
	 * @return
	 */
	synchronized public boolean setValue(String key, String value, int expireSec) {
		boolean OK = false;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			String ret = jedis.setex(key, expireSec, value);
			OK = "OK".equals(ret);
		} catch (Exception e) {
			e.printStackTrace();
			OK = false;
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return OK;

	}
	/**添加string类型的键值
	 * @param key	键
	 * @param value	值
	 * @return
	 */
	synchronized public boolean setValue(String key, String value) {
		boolean OK = false;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			String ret = jedis.set(key, value);
			OK = "OK".equals(ret);
		} catch (Exception e) {
			e.printStackTrace();
			OK = false;
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return OK;
	}

	public Jedis getJedis4Write() {
		return pool.getResource();
	}

	public void release(Jedis jedis) {
		if (jedis != null) {
			jedis.close();
		}
	}
	/**
	 * 添加set类型的键值
	 * @param key	键
	 * @param values	值
	 * @return
	 */
	synchronized public long add2set(String key,String... values)
	{
		Jedis jedis =null;
		long ret=0l;
		try {
			jedis = pool.getResource();  
			ret= jedis.sadd(key,  values);
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		}
		finally{
			try {
				if (jedis!=null)
				{
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return ret;
	}
	/**
	 * 添加set类型的键值
	 * @param key	键
	 * @param values	值
	 * @return
	 */
	synchronized public long add2set(String key, byte[]... values) {
		Jedis jedis = null;
		long ret = 0l;
		try {
			jedis = pool.getResource();
			ret = jedis.sadd(key.getBytes(), values);
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return ret;
	}

	/**重命名键
	 * @param oldKey	旧键
	 * @param newKey	新键
	 * @return
	 */
	synchronized public String renameKey(String oldKey, String newKey) {
		Jedis jedis = null;
		String ret = "";
		try {
			jedis = pool.getResource();
			ret = jedis.rename(oldKey.getBytes(), newKey.getBytes());
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return ret;
	}
	/**根据键获set类型的取值
	 * @param key	键
	 * @return
	 */
	synchronized public Set<String> getSet(String key) {
		Jedis jedis = null;
		Set<String> ret = null;
		try {
			// 随机选取其中一个pool
			int index = count++ % pools4read.length;
			JedisPool rpool = pools4read[index];
			jedis = rpool.getResource();
			ret = jedis.smembers(key);
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return ret;
	}
	/**根据键获set类型的取值
	 * @param key	键
	 * @return
	 */
	synchronized public Set<byte[]> getBinarySet(String key) {
		Jedis jedis = null;
		Set<byte[]> ret = null;
		try {
			// 随机选取其中一个pool
			int index = count++ % pools4read.length;
			JedisPool rpool = pools4read[index];
			jedis = rpool.getResource();
			ret = jedis.smembers(key.getBytes());
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return ret;
	}
	/**获取所有满足pattern的key
	 * @param pattern
	 * @return
	 */
	synchronized public Set<String> getKeys(String pattern) {
		Set<String> keys = null;
		Jedis jedis = null;
		try {
			// 随机选取其中一个pool
			int index = count++ % pools4read.length;
			JedisPool rpool = pools4read[index];
			jedis = rpool.getResource();
			keys = jedis.keys(pattern);
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return keys;
	}
	/**删除键
	 * @param key	键
	 * @return
	 */
	synchronized public boolean delKey(String key)
	{
		boolean OK=false;
		Jedis jedis =null;
		try {
			jedis = pool.getResource();  
			long ret= jedis.del(key);
			OK=(ret==1);
		} catch (Exception e) {
			e.printStackTrace();
			OK=false;
			reInit();
		}
		finally{
			try {
				if (jedis!=null)
				{
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return OK;
	}

//	synchronized public String setCounter(final String topic, final String key,
//			final long value) {
//		Jedis jedis = null;
//		String ret = null;
//		try {
//			jedis = pool.getResource();
//			ret = jedis.set(topic + "_" + key, String.valueOf(value));
//		} catch (Exception e) {
//			e.printStackTrace();
//			reInit();
//		} finally {
//			try {
//				jedis.close();
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		return ret;
//	}
	//用做计数器,考虑到多线程下使用，发现有异常：java.lang.ClassCastException: [B cannot be cast to java.lang.Long
	//看看是否加synchronized能够避免
//	synchronized public long incrCounter(final String topic, final String key) {
//		Jedis jedis = null;
//		long ret = 0;
//		try {
//			jedis = pool.getResource();
//			ret = jedis.incr(topic + "_" + key);
//		} catch (Exception e) {
//			e.printStackTrace();
//			reInit();
//		} finally {
//			try {
//				jedis.close();
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		return ret;
//	}
	//用做计数器
//	synchronized public long incrCounter(final String topic, final String key,
//			long num) {
//		Jedis jedis = null;
//		long ret = 0;
//		try {
//			jedis = pool.getResource();
//			ret = jedis.incrBy(topic + "_" + key, num);
//		} catch (Exception e) {
//			e.printStackTrace();
//			reInit();
//		} finally {
//			try {
//				jedis.close();
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		return ret;
//	}
//	public void resetCounter(final String topic, String... keys) {
//		Jedis jedis = null;
//		try {
//			jedis = pool.getResource();
//			for (String key : keys) {
//				// 删除
//				jedis.del(topic + "_" + key);
//				// setCounter(topic,key,0);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				if (jedis != null) {
//					jedis.close();
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//	}
//	public void resetCounter(String keyPrefix) {
//		Jedis jedis = null;
//		try {
//			jedis = pool.getResource();
//			Set<String> keys = jedis.keys(keyPrefix);
//			for (String key : keys) {
//				// 删除
//				jedis.del(key);
//				// setCounter(topic,key,0);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				if (jedis != null) {
//					jedis.close();
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//	}
	/**删除多个键
	 * @param keys	多个键
	 */
	synchronized public void delete(String... keys) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			for (String key : keys) {
				// 删除
				jedis.del(key);
			}
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	/**不存在时设值
	 * @param key	键
	 * @param value	值
	 * @return
	 */
	synchronized public long setValueIfNotExist(final String key,
			final String value) {
		Jedis jedis = null;
		long val = 0;// not set
		try {
			jedis = pool.getResource();
			// 不存在key时才赋值，否则不覆盖
			val = jedis.setnx(key, value);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {// 即使jedis=null也可以调用
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return val;
	}
	/**
	 * 添加hash类型的键值
	 * @param key	键
	 * @param map	值
	 */
	synchronized public void setHashMap(String key, Map<String, String> map) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			for (String name : map.keySet()) {
				jedis.hset(key, name, map.get(name));
			}
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 根据键获hash类型的取值
	 * @param key	键
	 * @return
	 */
	synchronized public Map<String, String> getHashMap(String key) {
		Jedis jedis = null;
		Map<String, String> map = null;
		try {
			// 随机选取其中一个pool
			int index = count++ % pools4read.length;
			JedisPool rpool = pools4read[index];
			jedis = rpool.getResource();
			map = jedis.hgetAll(key);
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return map;
	}
	
	/**添加list类型的键值
	 * @param key	键
	 * @param value	值
	 */
	synchronized public void pushList(String key, String value) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.lpush(key, value);
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	/**根据键获list类型的取值
	 * @param key	键
	 * @return
	 */
	synchronized public String popList(String key) {
		Jedis jedis = null;
		String ret = null;
		try {
			jedis = pool.getResource();
			ret = jedis.rpop(key);
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return ret;
	}
	/**设置过期时长
	 * @param key	键
	 * @param seconds	过期时长(秒)
	 */
	synchronized public void expire(String key, int seconds) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.expire(key, seconds);
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 只保留list中前n个
	 * @param key	键
	 * @param size	长度
	 */
	synchronized public void trimList(String key, int size) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.ltrim(key, 0, size - 1);
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 获得List中指定元素列表，
	 * @param key	键
	 * @param start 从0开始
	 * @param end -1为结尾
	 * @return
	 */
	synchronized public List<String> rangeList(String key, int start, int end) {
		Jedis jedis = null;
		List<String> ret = null;
		try {
			// 随机选取其中一个pool
			int index = count++ % pools4read.length;
			JedisPool rpool = pools4read[index];
			jedis = rpool.getResource();
			ret = jedis.lrange(key, start, end);
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return ret;
	}
	/**
	 * 移除list中的元素
	 * @param key	键
	 * @param value	值
	 * @return
	 */
	synchronized public long removeInList(String key, String value) {
		Jedis jedis = null;
		long ret = 0;
		try {
			jedis = pool.getResource();
			ret = jedis.lrem(key, 0, value);
		} catch (Exception e) {
			e.printStackTrace();
			reInit();
		} finally {
			try {
				if (jedis != null) {
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return ret;
	}
}
