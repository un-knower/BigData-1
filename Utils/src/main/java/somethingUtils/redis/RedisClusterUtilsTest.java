package somethingUtils.redis;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.*;

public class RedisClusterUtilsTest extends TestCase {
	private final String sentinels="192.168.4.202,192.168.4.203,192.168.4.206";
	private RedisClusterUtils rcu =new RedisClusterUtils(sentinels);
	
	@Test
	public void testSetValue() {
		rcu.setValue("jusfoun", "jusfoun2016");
	}
	
	@Test 
	public void testSetValueByTime(){
		rcu.setValue("testtime", "100",100);
	}
	
	@Test
	public void testGetValue(){
		System.out.println(rcu.getValue("jusfoun"));
	}
	
	@Test
	public void testDelKey(){
		rcu.delKey("jusfoun");
	}
	
	@Test
	public void testDelete(){
		rcu.delete(new String[]{"testtime","jusfoun"});
	}
	
	@Test
	public void testAdd2setString(){
		rcu.add2set("jsf-set-key-string", new String[]{"jsf-set-value1","jsf-set-value2","jsf-set-value3"});
	}
	
	@Test
	public void testAdd2setByte(){
		rcu.add2set("jsf-set-key-byte", new byte[]{1,2,3,4,5});
	}
	
	@Test
	public void testGetSet(){
		Set<String> resultSet = rcu.getSet("jsf-set-key-string");
		for(String s : resultSet){
			System.out.println(s);
		}
	}
	
	@Test
	public void testGetBinarySet(){
		Set<byte[]> resultSet = rcu.getBinarySet("byte-key");
		for(Iterator<byte[]> it = resultSet.iterator();it.hasNext();){
			byte[] bs = it.next();
			for(int t=0;t<bs.length;t++){
				System.out.println(bs[t]);
			}
		}
	}
	
	@Test
	public void testRenameKey(){
		System.out.println(rcu.renameKey("jsf-set-key-byte","byte-key"));
	}
	
	@Test
	public void testGetKeys(){
		Set<String> keys = rcu.getKeys("*");
		for(String s : keys){
			System.out.println(s);
		}
//		rcu.delete(keys.toArray(new String[]{}));
	}
	
	@Test
	public void testSetValueIfNotExist(){
		System.out.println(rcu.setValueIfNotExist("jusfoun","jsf2016"));
	}
	
	@Test
	public void testSetHashMap(){
		Map<String ,String> map = new HashMap<String, String>();
		map.put("m3", "333");
		map.put("m4", "44444");
		map.put("m2", "22");
		rcu.setHashMap("jsf-map-key",map);
	}
	
	@Test
	public void testGetHashMap(){
		Map<String, String> map = rcu.getHashMap("jsf-map-key");
		for(String key : map.keySet()){
			System.out.println("key : " + key + ",\tvalue : " + map.get(key));
		}
	}
	
	@Test
	public void testPushList(){
		rcu.pushList("jsf-list-key","test list value3");
	}
	
	@Test
	public void testPopList(){
		System.out.println(rcu.popList("jsf-list-key"));
	}
	
	@Test
	public void testExpire(){
		rcu.expire("jsf-list-key",5);
	}
	
	@Test
	public void testTrimList(){
		rcu.trimList("jsf-list-key",2);
	}
	
	@Test
	public void testRangeList(){
		List<String> list = rcu.rangeList("jsf-list-key",1,2);
		for(String s : list){
			System.out.println(s);
		}
	}
	@Test
	public void testRemoveInList(){
		rcu.removeInList("jsf-list-key","test list value2");
	}
}
