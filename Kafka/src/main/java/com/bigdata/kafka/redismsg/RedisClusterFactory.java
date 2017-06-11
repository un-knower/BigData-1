package com.bigdata.kafka.redismsg;

import org.apache.log4j.Logger;

/**
 * Redis集群的工厂类
 * @author admin
 *
 */
public class RedisClusterFactory { 

	//日志
	public static Logger logger = Logger.getLogger(RedisClusterFactory.class ) ;
	//单例对象
	private static RedisClusterUtils redisClusterUtils ; 
	
	//初始化 集群信息
	public  synchronized static void initRedisCluster(String ips){
		redisClusterUtils = new RedisClusterUtils(ips);
		logger.debug("初始化的ip=="+ips );
		
	}
	
	//得到redis池对象 
	public synchronized static RedisClusterUtils getInstants(){
		if(redisClusterUtils==null){
			redisClusterUtils = new RedisClusterUtils();
		} 
		return redisClusterUtils ; 
	}

}
