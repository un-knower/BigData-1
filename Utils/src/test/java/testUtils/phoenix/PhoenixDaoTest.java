package testUtils.phoenix;

import org.junit.Test;
import somethingUtils.phoenix.PhoenixDao;

import java.util.List;
import java.util.Map;

public class PhoenixDaoTest {

	@Test
	public void testCreateTable( ){
		String sql = "  create table test8 (mykey integer not null primary key, mycolumn varchar) ";
		int row = PhoenixDao.execute(sql);
		System.out.println( row );
	}
	
	@Test
	public void testInsertData( ){
		
		String sql = " upsert into test8 values (3,'Hello')  ";
		int row = PhoenixDao.execute(sql);
		System.out.println( row );
		
	}

	//select * from \"student1\"  
	@Test
	public void testQuery( ){
		
		String sql = " select * from  \"student1\"  ";
		List<Map<String,Object>> list = PhoenixDao.query(sql) ;
		System.out.println( list );
	}
	
}
